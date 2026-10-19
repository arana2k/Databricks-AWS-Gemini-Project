from gemini_client import GeminiAgent
from s3_uploader import S3Uploader
from databricks_client import DatabricksClient
from schema_loader import get_schema
from datetime import date
from typing import Optional, Callable
import logging

log = logging.getLogger(__name__)

# Type aliases for the two optional notification hooks.
# progress_callback: fired after each entity is uploaded to S3.
#   Signature: (entity_name, s3_uri, row_count) -> None
# status_callback: fired for free-form pipeline status messages
#   (warehouse warm-up done, SCD fetch complete, job triggered, etc.)
#   Signature: (message: str) -> None
ProgressCallback = Callable[[str, str, int], None]
StatusCallback = Callable[[str], None]


class RetailDataPipeline:
    def __init__(
        self,
        config: dict,
        schemas: dict,
        progress_callback: Optional[ProgressCallback] = None,
        status_callback: Optional[StatusCallback] = None,
    ):
        """
        Args:
            config:            Pipeline configuration dict (credentials, counts, etc.)
            schemas:           Entity schema definitions loaded from schemas.json.
            progress_callback: Called after each entity upload to S3.
                               Receives (entity_name, s3_uri, row_count).
            status_callback:   Called for free-form status updates
                               (warehouse ready, SCD fetch, job trigger, etc.).
                               Receives a plain string message.
        """
        self.config = config
        self.SCHEMAS = schemas
        self.file_date = config.get("file_date", date.today().strftime("%Y%m%d"))
        self._progress_callback = progress_callback
        self._status_callback = status_callback

        self.gemini = GeminiAgent(
            api_key=config["gemini_api_key"],
            model=config.get("gemini_model", "gemini-3.1-flash-lite"),
        )
        self.s3 = S3Uploader(
            bucket=config["s3_bucket"],
            aws_access_key=config.get("aws_access_key_id"),
            aws_secret_key=config.get("aws_secret_access_key"),
            region=config.get("aws_region", "us-east-1"),
        )
        self.db = DatabricksClient(
            host=config["databricks_host"],
            token=config["databricks_token"],
        )
        self.warehouse_id = config["databricks_warehouse_id"]
        self.db_catalog = config.get("databricks_catalog", "retail_data")
        self.db_schema_bronze = config.get("databricks_schema", "bronze")
        self.db_schema_gold = config.get("databricks_gold_schema", "gold")

        self._gold_tables = {
            "products": f"{self.db_catalog}.{self.db_schema_gold}.products",
            "customers": f"{self.db_catalog}.{self.db_schema_gold}.customers",
        }

    # ── Notification helpers ──────────────────────────────────────────────────

    def _notify(self, entity_name: str, uri: str, row_count: int) -> None:
        """Fire the progress callback after an S3 upload. Errors are swallowed
        so a Telegram hiccup can never crash the pipeline."""
        if self._progress_callback is None:
            return
        try:
            self._progress_callback(entity_name, uri, row_count)
        except Exception as e:
            log.warning(f"progress_callback error (ignored): {e}")

    def _status(self, message: str) -> None:
        """Fire the status callback with a plain message. Errors are swallowed."""
        if self._status_callback is None:
            return
        try:
            self._status_callback(message)
        except Exception as e:
            log.warning(f"status_callback error (ignored): {e}")

    # ── Internal pipeline steps ───────────────────────────────────────────────

    def _fetch_existing(
        self, entity_name: str, id_column: str, scd_mode: str
    ) -> tuple[list[str], list[dict]]:
        """
        Fetch existing records from the consolidated gold schema table.
        Sends a status update before and after so the user sees Databricks
        query progress in Telegram.
        """
        entity_type = "products" if "product" in entity_name else "customers"
        table_path = self._gold_tables[entity_type]
        scd_label = "SCD2 active only" if scd_mode == "scd2" else "SCD1"

        self._status(
            f"🔍 Querying Databricks for existing `{entity_name}` IDs...\n"
            f"   Table: `{table_path}`  [{scd_label}]"
        )

        records = self.db.fetch_existing(
            warehouse_id=self.warehouse_id,
            table_path=table_path,
            id_column=id_column,
            limit=self.config.get("fetch_limit", 300),
            scd_type=2 if scd_mode == "scd2" else 1,
        )
        ids = [str(r.get(id_column, "")) for r in records]

        self._status(f"   ✓ {len(ids)} existing `{entity_name}` IDs loaded from gold")

        log.info(
            f"  Gold table: {table_path}"
            + (f"  [WHERE is_current = true]" if scd_mode == "scd2" else "")
            + f"  → {len(ids)} existing IDs loaded"
        )
        return ids, records

    def _run_entity(
        self,
        entity_name: str,
        channel: str,
        num_records: int,
        scd_mode: Optional[str],
        parent_context: Optional[dict] = None,
    ) -> tuple[str, list[str]]:
        """
        Generate data for one entity, upload to S3, notify progress.
        Returns (s3_uri, all_valid_ids).
        """
        log.info(f"\n{'='*60}")
        log.info(
            f"Generating: {entity_name}  "
            f"(channel={channel}, scd={scd_mode}, n={num_records})"
        )

        schema = self.SCHEMAS[entity_name]
        id_column = schema["gold_table_id_column"]
        local_id_col = schema["id_column"]

        existing_ids, existing_sample = [], []
        if scd_mode in ("scd1", "scd2"):
            existing_ids, existing_sample = self._fetch_existing(
                entity_name, id_column, scd_mode
            )

        rows = self.gemini.generate_csv_records(
            entity=entity_name,
            schema_info=schema,
            num_records=num_records,
            existing_ids=existing_ids,
            existing_sample=existing_sample,
            file_date=self.file_date,
            scd_mode=scd_mode,
            parent_context=parent_context,
        )

        new_ids = [str(r.get(local_id_col)) for r in rows if r.get(local_id_col)]
        all_valid_ids = list(set(existing_ids + new_ids))

        s3_key = self.s3.build_key(
            channel=channel,
            file_date=self.file_date,
            filename=entity_name,
        )
        uri = self.s3.upload_csv(rows, s3_key)

        # Fires the per-entity Telegram update (emoji + row count + S3 URI)
        self._notify(entity_name, uri, len(rows))

        return uri, all_valid_ids

    # ── Full pipeline run ─────────────────────────────────────────────────────

    def run(self) -> dict:
        """Run the full pipeline for all 6 entities. Returns dict of S3 URIs."""
        cfg = self.config
        results = {}

        # ── Duplicate-date guard ──────────────────────────────────────────────
        log.info("=" * 60)
        log.info(f"Checking if data for {self.file_date} already exists in S3...")
        if self.s3.check_date_exists(self.file_date):
            log.warning(
                f"Data for file_date '{self.file_date}' already exists. Aborting."
            )
            return results

        # ── Warehouse warm-up ─────────────────────────────────────────────────
        log.info("=" * 60)
        log.info("Warming up Databricks SQL Warehouse...")
        # Note: _status("🔥 Warming up...") is sent by lambda_worker BEFORE calling
        # pipeline.run(), so we only need to send the "ready" confirmation here,
        # AFTER start_warehouse() returns and the warehouse is confirmed RUNNING.
        self.db.start_warehouse(self.warehouse_id)
        self._status(
            "✅ Databricks SQL Warehouse is *RUNNING*. Starting data generation..."
        )
        log.info("=" * 60)

        # ── ONLINE ────────────────────────────────────────────────────────────
        self._status("⬇️ *Online channel* — generating customers, products, orders...")
        results["online_customers"], onl_cust_ids = self._run_entity(
            "online_customers",
            "online",
            num_records=cfg.get("online_customers_count", 50),
            scd_mode="scd1",
        )
        results["online_products"], onl_prod_ids = self._run_entity(
            "online_products",
            "online",
            num_records=cfg.get("online_products_count", 30),
            scd_mode="scd2",
        )
        results["online_orders"], _ = self._run_entity(
            "online_orders",
            "online",
            num_records=cfg.get("online_orders_count", 200),
            scd_mode=None,
            parent_context={"customer_ids": onl_cust_ids, "sku_ids": onl_prod_ids},
        )

        # ── OFFLINE ───────────────────────────────────────────────────────────
        self._status("🏪 *Offline channel* — generating customers, products, orders...")
        results["offline_customers"], off_cust_ids = self._run_entity(
            "offline_customers",
            "offline",
            num_records=cfg.get("offline_customers_count", 50),
            scd_mode="scd1",
        )
        results["offline_products"], off_prod_ids = self._run_entity(
            "offline_products",
            "offline",
            num_records=cfg.get("offline_products_count", 30),
            scd_mode="scd2",
        )
        results["offline_orders"], _ = self._run_entity(
            "offline_orders",
            "offline",
            num_records=cfg.get("offline_orders_count", 200),
            scd_mode=None,
            parent_context={"customer_ids": off_cust_ids, "sku_ids": off_prod_ids},
        )

        log.info("\n" + "=" * 60)
        log.info("PIPELINE COMPLETE — S3 URIs:")
        for entity, uri in results.items():
            log.info(f"  {entity:30s} → {uri}")

        # ── Optional downstream Databricks job trigger ────────────────────────
        target_job_id = cfg.get("ingestion_job_id")
        if target_job_id:
            log.info("\n" + "=" * 60)
            log.info("Triggering downstream Databricks ingestion job...")
            self._status(
                f"⚙️ Triggering Databricks ingestion job `{target_job_id}` "
                f"for date `{self.file_date}`..."
            )
            try:
                run_id = self.db.run_job(
                    job_id=int(target_job_id),
                    job_parameters={"file_run": self.file_date},
                )
                self._status(
                    f"✅ Databricks ingestion job completed\n"
                    f"   run\\_id: `{run_id}`"
                )
            except Exception as e:
                log.error(f"Downstream job execution failed: {e}")
                self._status(f"❌ Databricks job failed: `{e}`")
                raise

        return results
