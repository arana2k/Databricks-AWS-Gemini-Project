import requests
import logging
import time

log = logging.getLogger(__name__)


class DatabricksClient:
    def __init__(self, host: str, token: str):
        self.host = host.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def sql_execute(
        self, warehouse_id: str, sql: str, max_rows: int = 500
    ) -> list[dict]:
        """Execute SQL on a Databricks SQL warehouse and return rows as list of dicts."""
        log.info(f"Databricks SQL: {sql[:120]}...")
        endpoint = f"{self.host}/api/2.0/sql/statements"
        payload = {
            "warehouse_id": warehouse_id,
            "statement": sql,
            "wait_timeout": "50s",
            "on_wait_timeout": "CANCEL",
            "row_limit": max_rows,
        }
        resp = requests.post(endpoint, headers=self.headers, json=payload, timeout=90)
        resp.raise_for_status()
        result = resp.json()

        # Poll if still running
        stmt_id = result.get("statement_id")
        for _ in range(20):
            state = result.get("status", {}).get("state", "")
            if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                break
            time.sleep(3)
            poll = requests.get(
                f"{endpoint}/{stmt_id}", headers=self.headers, timeout=30
            )
            poll.raise_for_status()
            result = poll.json()

        if result.get("status", {}).get("state") != "SUCCEEDED":
            raise RuntimeError(f"Databricks query failed: {result.get('status')}")

        schema = result["manifest"]["schema"]["columns"]
        col_names = [c["name"] for c in schema]
        rows = result.get("result", {}).get("data_array", [])
        return [dict(zip(col_names, row)) for row in rows]

    def fetch_existing(
        self,
        warehouse_id: str,
        table_path: str,
        id_column: str,
        limit: int = 300,
        scd_type: int = 0,
    ) -> list[dict]:
        """
        Fetch existing records for SCD comparison.

        Args:
            scd_type: Pass 2 for SCD2 tables — adds WHERE is_current = true
                      so only the active version of each record is returned,
                      not stale historical rows. Pass 0 or 1 for all others.
        """
        where = " WHERE is_current = true" if scd_type == 2 else ""
        sql = f"SELECT DISTINCT {id_column} FROM {table_path}{where} ORDER BY RAND() LIMIT {limit}"
        try:
            return self.sql_execute(warehouse_id, sql, max_rows=limit)
        except Exception as e:
            log.warning(f"Could not fetch existing records from {table_path}: {e}")
            return []

    def start_warehouse(self, warehouse_id: str, timeout_seconds: int = 300) -> None:
        """
        Starts the Databricks SQL warehouse and waits for it to become RUNNING.
        """
        log.info(f"Ensuring Databricks warehouse '{warehouse_id}' is started...")

        # 1. Send the Start request
        start_endpoint = f"{self.host}/api/2.0/sql/warehouses/{warehouse_id}/start"
        try:
            resp = requests.post(start_endpoint, headers=self.headers, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Databricks sometimes throws an error if it's already starting/running.
            # We log it and let the polling step verify the actual state.
            log.warning(
                f"Start request returned status {resp.status_code}: {resp.text}"
            )

        # 2. Poll for the 'RUNNING' state acknowledgement
        get_endpoint = f"{self.host}/api/2.0/sql/warehouses/{warehouse_id}"
        start_time = time.time()

        while True:
            poll_resp = requests.get(get_endpoint, headers=self.headers, timeout=30)
            poll_resp.raise_for_status()

            data = poll_resp.json()
            state = data.get("state")

            if state == "RUNNING":
                log.info(f"Success: Warehouse '{warehouse_id}' is RUNNING.")
                break
            elif state in ("STOPPED", "DELETED", "ERROR"):
                raise RuntimeError(
                    f"Warehouse '{warehouse_id}' failed to start. Final state: {state}"
                )

            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(
                    f"Warehouse '{warehouse_id}' did not start within {timeout_seconds} seconds. Current state: {state}"
                )

            log.info(f"Warehouse is currently '{state}'... waiting 10 seconds.")
            time.sleep(10)

    def run_job(
        self,
        job_id: int,
        job_parameters: dict = None,
        wait_for_completion: bool = True,
        timeout_seconds: int = 1800,
    ) -> int:
        """
        Triggers a Databricks job and optionally waits for it to succeed.
        Returns the run_id of the triggered execution.
        """
        log.info(f"Triggering Databricks job '{job_id}'...")

        # 1. Trigger the job
        trigger_endpoint = f"{self.host}/api/2.2/jobs/run-now"
        payload = {"job_id": job_id}
        if job_parameters:
            payload["job_parameters"] = job_parameters

        resp = requests.post(
            trigger_endpoint, headers=self.headers, json=payload, timeout=30
        )
        resp.raise_for_status()

        run_id = resp.json().get("run_id")
        log.info(f"Job triggered successfully. run_id: {run_id}")

        if not wait_for_completion:
            return run_id

        # 2. Poll for completion
        log.info(f"Waiting for run_id '{run_id}' to complete...")
        poll_endpoint = f"{self.host}/api/2.1/jobs/runs/get"
        start_time = time.time()

        while True:
            poll_resp = requests.get(
                poll_endpoint,
                headers=self.headers,
                params={"run_id": run_id},
                timeout=30,
            )
            poll_resp.raise_for_status()

            run_data = poll_resp.json()
            state = run_data.get("state", {})
            life_cycle_state = state.get("life_cycle_state")
            result_state = state.get("result_state")

            # TERMINATED, SKIPPED, or INTERNAL_ERROR mean the job is no longer running
            if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                if result_state == "SUCCESS":
                    log.info(
                        f"Success: Databricks job run '{run_id}' completed successfully."
                    )
                    break
                else:
                    error_msg = state.get("state_message", "Unknown error")
                    raise RuntimeError(
                        f"Job run '{run_id}' failed. Result State: {result_state}. Message: {error_msg}"
                    )

            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(
                    f"Job run '{run_id}' exceeded timeout of {timeout_seconds}s. Current state: {life_cycle_state}"
                )

            log.info(f"Job is currently '{life_cycle_state}'... waiting 15 seconds.")
            time.sleep(15)

        return run_id
