"""
lambda_worker.py
================
Long-running worker Lambda.

Invoked asynchronously by lambda_receiver with payload:
  { "chat_id": 123456, "file_date": "20260517" }

Responsibilities:
  1. Load pipeline config from environment variables.
  2. Inject a progress_callback (per-entity S3 upload updates) and a
     status_callback (Databricks warm-up, SCD fetches, job trigger) into
     RetailDataPipeline so every meaningful step reaches Telegram.
  3. Run the full pipeline.
  4. Send a final summary (or error message) to the user.

Environment variables required (configure via Lambda console / Terraform):
  BOT_TOKEN
  GEMINI_API_KEY
  S3_BUCKET
  AWS_ACCESS_KEY_ID_PIPELINE
  AWS_SECRET_ACCESS_KEY_PIPELINE
  AWS_REGION_PIPELINE          (optional, default us-east-1)
  DATABRICKS_HOST
  DATABRICKS_TOKEN
  DATABRICKS_WAREHOUSE_ID
  DATABRICKS_CATALOG           (optional, default retail_data)
  DATABRICKS_SCHEMA            (optional, default bronze)
  DATABRICKS_GOLD_SCHEMA       (optional, default gold)
  INGESTION_JOB_ID             (optional — triggers downstream Databricks job)
  ONLINE_CUSTOMERS_COUNT       (optional, default 50)
  ONLINE_PRODUCTS_COUNT        (optional, default 30)
  ONLINE_ORDERS_COUNT          (optional, default 200)
  OFFLINE_CUSTOMERS_COUNT      (optional, default 50)
  OFFLINE_PRODUCTS_COUNT       (optional, default 30)
  OFFLINE_ORDERS_COUNT         (optional, default 200)
"""

import json
import logging
import os
import urllib.request

from schema_loader import load_schemas
from pipeline_client import RetailDataPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")


# ── Telegram helper ───────────────────────────────────────────────────────────


def send_message(chat_id: int, text: str) -> None:
    """Fire-and-forget Telegram message. Never raises."""
    if not BOT_TOKEN or not chat_id:
        return
    payload = json.dumps(
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
        }
    ).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as r:
            print("Telegram ack:", r.read().decode())
    except Exception as e:
        log.warning(f"send_message error: {e}")


# ── Config builder ────────────────────────────────────────────────────────────


def _build_config(file_date: str) -> dict:
    return {
        "gemini_api_key": os.environ["GEMINI_API_KEY"],
        "gemini_model": os.environ["gemini_model"],
        "s3_bucket": os.environ["S3_BUCKET"],
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID_PIPELINE"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY_PIPELINE"),
        "aws_region": os.environ.get("AWS_REGION_PIPELINE", "us-east-1"),
        "databricks_host": os.environ["DATABRICKS_HOST"],
        "databricks_token": os.environ["DATABRICKS_TOKEN"],
        "databricks_warehouse_id": os.environ["DATABRICKS_WAREHOUSE_ID"],
        "databricks_catalog": os.environ.get("DATABRICKS_CATALOG", "retail_data"),
        "databricks_schema": os.environ.get("DATABRICKS_SCHEMA", "bronze"),
        "databricks_gold_schema": os.environ.get("DATABRICKS_GOLD_SCHEMA", "gold"),
        "ingestion_job_id": os.environ.get("INGESTION_JOB_ID"),
        "file_date": file_date,
        "fetch_limit": int(os.environ.get("fetch_limit", 100)),
        "online_customers_count": int(os.environ.get("ONLINE_CUSTOMERS_COUNT", 50)),
        "online_products_count": int(os.environ.get("ONLINE_PRODUCTS_COUNT", 30)),
        "online_orders_count": int(os.environ.get("ONLINE_ORDERS_COUNT", 200)),
        "offline_customers_count": int(os.environ.get("OFFLINE_CUSTOMERS_COUNT", 50)),
        "offline_products_count": int(os.environ.get("OFFLINE_PRODUCTS_COUNT", 30)),
        "offline_orders_count": int(os.environ.get("OFFLINE_ORDERS_COUNT", 200)),
    }


# ── Emoji helper ──────────────────────────────────────────────────────────────


def _entity_emoji(entity: str) -> str:
    if "order" in entity:
        return "🛒"
    if "customer" in entity:
        return "👤"
    if "product" in entity:
        return "📦"
    return "📄"


# ── Lambda entry point ────────────────────────────────────────────────────────


def lambda_handler(event, context):
    """
    Expected event shape (sent by lambda_receiver):
      { "chat_id": <int>, "file_date": "<YYYYMMDD>" }
    """
    log.info("Worker event: %s", json.dumps(event))

    chat_id = event.get("chat_id")
    file_date = event.get("file_date")

    if not chat_id or not file_date:
        log.error("Missing chat_id or file_date in event payload.")
        return

    try:
        config = _build_config(file_date)
        schemas = load_schemas()

        # ── Callback 1: per-entity S3 upload completion ───────────────────────
        def on_entity_complete(entity_name: str, uri: str, row_count: int) -> None:
            emoji = _entity_emoji(entity_name)
            send_message(
                chat_id,
                f"{emoji} *{entity_name}* — {row_count:,} rows uploaded\n`{uri}`",
            )

        # ── Callback 2: free-form Databricks / pipeline status ────────────────
        # Covers: warehouse ready, SCD fetches (before + after), channel
        # section headers, ingestion job triggered + completed/failed.
        def on_status(message: str) -> None:
            send_message(chat_id, message)

        pipeline = RetailDataPipeline(
            config,
            schemas=schemas,
            progress_callback=on_entity_complete,
            status_callback=on_status,
        )

        # ── Duplicate-date guard ──────────────────────────────────────────────
        if pipeline.s3.check_date_exists(file_date):
            send_message(
                chat_id, f"⚠️ Data for *{file_date}* already exists in S3. Aborting."
            )
            return

        # ── Warehouse warm-up notice ──────────────────────────────────────────
        # Send "warming up" immediately so the user knows what's happening
        # during the potentially long start_warehouse() call inside pipeline.run().
        send_message(chat_id, "🔥 Warming up Databricks SQL Warehouse...")

        results = pipeline.run()

        # ── Final summary ─────────────────────────────────────────────────────
        lines = ["✅ *Pipeline complete!* All files uploaded:\n"]
        for entity, uri in results.items():
            lines.append(f"• `{entity}` → `{uri}`")
        send_message(chat_id, "\n".join(lines))

    except Exception as e:
        log.exception("Pipeline failed")
        send_message(chat_id, f"❌ *Pipeline failed* for `{file_date}`\n\nError: `{e}`")
