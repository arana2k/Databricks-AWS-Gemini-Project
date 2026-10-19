import logging
import time
import json
import re
import csv
import io
from typing import Optional
from google import genai

log = logging.getLogger(__name__)


class GeminiAgent:
    def __init__(self, api_key: str, model: str = "gemini-3.1-flash-lite"):
        self.client = genai.Client(api_key=api_key)
        self.model = model
        log.info(f"Gemini model: {model}")

    def list_models(self) -> list[dict]:
        """Lists all available models accessible by the current API key."""
        try:
            available_models = []
            for m in self.client.models.list():
                model_info = {
                    "name": m.name,
                    "display_name": m.display_name,
                    "supported_methods": getattr(m, "supported_generation_methods", []),
                    "description": getattr(m, "description", ""),
                }
                available_models.append(model_info)
                log.info(f"Model: {m.name}")
            return available_models
        except Exception as e:
            log.error(f"Failed to list models: {e}")
            raise

    def generate(self, prompt: str, retries: int = 3) -> str:
        for attempt in range(retries):
            try:
                response = self.client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                )
                return response.text
            except Exception as e:
                log.warning(f"Gemini attempt {attempt+1} failed: {e}")
                time.sleep(2**attempt)
        raise RuntimeError("Gemini generation failed after retries")

    def generate_csv_records(
        self,
        entity: str,
        schema_info: dict,
        num_records: int,
        existing_ids: list[str],
        existing_sample: list[dict],
        file_date: str,
        scd_mode: Optional[str] = None,
        parent_context: Optional[dict] = None,
    ) -> list[dict]:
        """Ask Gemini to generate CSV rows and parse them back."""

        columns = schema_info["columns"]
        description = schema_info["description"]
        id_column = schema_info["id_column"]

        existing_context = ""
        if existing_sample:
            sample_str = json.dumps(existing_sample[:5], indent=2)
            existing_context = f"""
EXISTING RECORDS IN DATABASE (sample of {len(existing_sample)} total):
{sample_str}

EXISTING {id_column.upper()} IDs (DO NOT reuse for new records):
{json.dumps(existing_ids[:30])}
"""

        scd_instructions = ""
        if scd_mode == "scd2":
            update_count = max(1, num_records // 4)
            new_count = num_records - update_count
            scd_instructions = f"""
SCD TYPE 2 INSTRUCTIONS:
- Generate {new_count} completely NEW records with brand new IDs not in the existing list above.
- Generate {update_count} UPDATE records: pick existing IDs from the list above and change fields like price, is_active, mrp_price, tax_rate, brand, or category. Keep the same ID but change other values to simulate product updates.
"""
        elif scd_mode == "scd1":
            update_count = max(1, num_records // 5)
            new_count = num_records - update_count
            scd_instructions = f"""
SCD TYPE 1 INSTRUCTIONS:
- Generate {new_count} completely NEW customer records with brand new IDs.
- Generate {update_count} UPDATE records: pick existing customer IDs from the list above and change fields like email, mobile_number/phone, city, state. Keep the same ID.
"""

        order_instructions = ""
        if parent_context and "orders" in entity:
            cust_ids = parent_context.get("customer_ids", [])[:300]
            sku_ids = parent_context.get("sku_ids", [])[:300]
            order_instructions = f"""
FOREIGN KEY INSTRUCTIONS (ORDERS):
- For approximately 80% of the generated records, you MUST select customer IDs from this valid list: {json.dumps(cust_ids)}
- For approximately 80% of the generated records, you MUST select SKU/product IDs from this valid list: {json.dumps(sku_ids)}
- For the remaining 20% of records, generate new, realistic IDs that match the ID format described in the schema.
"""

        prompt = f"""You are a data generation agent for a retail company. Generate exactly {num_records} realistic records for the '{entity}' dataset.

SCHEMA COLUMNS: {', '.join(columns)}
DESCRIPTION: {description}
FILE DATE: {file_date}

{existing_context}
{scd_instructions}
{order_instructions}

RULES:
1. Output ONLY valid CSV. First line must be the header row.
2. No markdown, no backticks, no explanations — raw CSV only.
3. Use comma as delimiter. Wrap values containing commas in double quotes.
4. Null values should be written as empty string (nothing between commas).
5. Generate realistic, varied data. Don't repeat the same values.
6. All IDs must follow the exact format described above.
7. Dates in file_date column should be integer {file_date} (no dashes).

Generate the CSV now:"""

        raw = self.generate(prompt)

        # Strip markdown fences if Gemini adds them anyway
        raw = re.sub(r"```[a-z]*\n?", "", raw).strip()

        # Parse CSV
        reader = csv.DictReader(io.StringIO(raw))
        rows = [row for row in reader]

        if not rows:
            raise ValueError(f"Gemini returned no parseable CSV rows for {entity}")

        log.info(f"  Gemini generated {len(rows)} rows for {entity}")
        return rows
