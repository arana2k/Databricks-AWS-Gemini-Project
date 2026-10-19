"""
lambda_receiver.py
==================
Telegram webhook receiver.

Responsibilities:
  1. Parse incoming Telegram update.
  2. Authorise /trigger calls.
  3. Immediately return HTTP 200 to Telegram (required — Telegram retries if
     it doesn't get a fast 200).
  4. Invoke lambda_worker asynchronously (InvocationType=Event) so the
     actual pipeline runs outside this request's timeout window.

Environment variables required:
  BOT_TOKEN        – Telegram bot token
  WORKER_LAMBDA    – Name / ARN of the worker Lambda function
  ALLOWED_USERS    – Comma-separated Telegram user IDs allowed to /trigger
"""

import json
import os
import re
import urllib.request
import boto3

# ── Config ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WORKER_LAMBDA = os.environ.get("WORKER_LAMBDA", "retail-data-worker")
ALLOWED_USERS_ENV = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USER_IDS = {
    int(u.strip()) for u in ALLOWED_USERS_ENV.split(",") if u.strip().isdigit()
}

_lambda_client = boto3.client("lambda")


# ── Telegram helpers ──────────────────────────────────────────────────────────
def send_message(chat_id: int, text: str) -> None:
    if not BOT_TOKEN:
        print("ERROR: BOT_TOKEN missing")
        return
    payload = json.dumps(
        {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
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
        print("send_message error:", e)


def success_response():
    return {"statusCode": 200, "body": json.dumps({"status": "ok"})}


# ── Main handler ──────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    print("Receiver event:", json.dumps(event))

    try:
        body = event.get("body", {})
        if isinstance(body, str):
            body = json.loads(body)

        message = body.get("message", {})
        chat = message.get("chat", {})
        sender = message.get("from", {})
        chat_id = chat.get("id")
        user_id = sender.get("id")
        text = message.get("text", "").strip()

        print(f"chat_id={chat_id}  user_id={user_id}  text={text!r}")

        if not chat_id:
            return success_response()

        # ── Static commands ───────────────────────────────────────────────────
        if text == "/start":
            send_message(
                chat_id,
                (
                    "👋 *Retail Data Bot is live!*\n\n"
                    "Commands:\n"
                    "  `/trigger YYYYMMDD` – generate & upload data for a date\n"
                    "  `/ping`            – health check\n"
                    "  `/contact`         – owner info\n"
                    "  `/datawarehouse`   – DWH info"
                ),
            )

        elif text == "/ping":
            send_message(chat_id, "pong ✅")

        elif text == "/contact":
            send_message(chat_id, "LinkedIn: kaustubh-gupta")

        elif text == "/datawarehouse":
            send_message(chat_id, "Data Warehouse: *Databricks*")

        # ── /trigger ──────────────────────────────────────────────────────────
        elif text.startswith("/trigger"):
            # 1. Auth check
            if ALLOWED_USER_IDS and user_id not in ALLOWED_USER_IDS:
                print(f"Unauthorised user: {user_id}")
                send_message(chat_id, "⛔️ Not authorised.")
                return success_response()

            # 2. Parse date argument
            parts = text.split(maxsplit=1)
            file_date = parts[1].strip() if len(parts) > 1 else ""

            if not file_date:
                send_message(chat_id, "⚠️ Usage: `/trigger YYYYMMDD`")
                return success_response()

            if not re.match(r"^\d{8}$", file_date):
                send_message(
                    chat_id,
                    f"⚠️ Invalid date `{file_date}`. Expected format: `YYYYMMDD` (e.g. `20260517`)",
                )
                return success_response()

            # 3. Acknowledge immediately — pipeline may take several minutes
            send_message(
                chat_id,
                f"⚙️ Pipeline triggered for *{file_date}*.\n"
                "I'll send you updates as each entity is processed. Hang tight! 🚀",
            )

            # 4. Fire-and-forget: invoke the worker Lambda asynchronously
            worker_payload = {"chat_id": chat_id, "file_date": file_date}
            _lambda_client.invoke(
                FunctionName=WORKER_LAMBDA,
                InvocationType="Event",  # async — doesn't wait for response
                Payload=json.dumps(worker_payload).encode(),
            )
            print(f"Worker Lambda invoked for chat_id={chat_id} file_date={file_date}")

        else:
            send_message(
                chat_id, f"❓ Unknown command: `{text}`\nSend `/start` for help."
            )

    except Exception as e:
        print("Receiver error:", e)

    # Always return 200 to Telegram
    return success_response()
