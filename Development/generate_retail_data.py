"""
Retail Data Generation Agent
==============================
Uses Gemini LLM to generate realistic synthetic retail data for:
  - online_orders, online_customers, online_products
  - offline_orders, offline_customers, offline_products

SCD Handling:
  - Products (SCD2): Fetches existing records from Databricks, generates updates + new records
  - Customers (SCD1): Fetches existing records from Databricks, generates updates + new records
  - Orders: Pure fact table, always generates new records

Output: CSV files uploaded to S3 under:
  s3://<bucket>/<entity>/<channel>/<YYYYMMDD>/<filename>.csv
"""

import os
import json
import logging
import argparse
from schema_loader import load_schemas
from pipeline_client import RetailDataPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SCHEMA DEFINITIONS  (mirrors Databricks tables)
# ─────────────────────────────────────────────

SCHEMAS = load_schemas()


def parse_args():
    p = argparse.ArgumentParser(description="Retail Data Generation Agent")
    p.add_argument("--config", default="config.json", help="Path to JSON config file")
    p.add_argument("--file-date", help="Override file date YYYYMMDD (default: today)")
    p.add_argument("--entity", help="Run only a specific entity (e.g. online_orders)")
    return p.parse_args()


def load_config(path: str) -> dict:
    """Load config from JSON file, with env var overrides."""
    if os.path.exists(path):
        with open(path) as f:
            cfg = json.load(f)
    else:
        cfg = {}

    # Allow environment variable overrides for secrets
    env_keys = [
        "GEMINI_API_KEY",
        "S3_BUCKET",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_WAREHOUSE_ID",
    ]
    for key in env_keys:
        val = os.environ.get(key)
        if val:
            cfg[key.lower()] = val

    return cfg


def main():
    args = parse_args()
    config = load_config(args.config)

    if args.file_date:
        config["file_date"] = args.file_date

    # Validate required config
    required = [
        "gemini_api_key",
        "s3_bucket",
        "databricks_host",
        "databricks_token",
        "databricks_warehouse_id",
    ]
    missing = [k for k in required if not config.get(k)]
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")

    pipeline = RetailDataPipeline(config, schemas=SCHEMAS)

    if args.entity:
        # Run single entity for testing
        schema = SCHEMAS.get(args.entity)
        if not schema:
            raise ValueError(
                f"Unknown entity: {args.entity}. Choose from: {list(SCHEMAS.keys())}"
            )

        # Add the existence check here as well
        if pipeline.s3.check_date_exists(pipeline.file_date):
            log.warning(
                f"Data for file_date '{pipeline.file_date}' already exists in S3. Aborting single-entity generation."
            )
            return

        channel = "online" if args.entity.startswith("online") else "offline"
        scd = (
            "scd2"
            if "product" in args.entity
            else ("scd1" if "customer" in args.entity else None)
        )
        pipeline._run_entity(args.entity, channel, num_records=20, scd_mode=scd)
    else:
        pipeline.run()


if __name__ == "__main__":
    main()
