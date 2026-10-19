import boto3
import csv
import io
import logging

log = logging.getLogger(__name__)


class S3Uploader:
    def __init__(
        self,
        bucket: str,
        aws_access_key: str = None,
        aws_secret_key: str = None,
        region: str = "us-east-1",
    ):
        kwargs = {"region_name": region}
        if aws_access_key and aws_secret_key:
            kwargs["aws_access_key_id"] = aws_access_key
            kwargs["aws_secret_access_key"] = aws_secret_key
        self.s3 = boto3.client("s3", **kwargs)
        self.bucket = bucket

    def upload_csv(self, rows: list[dict], s3_key: str) -> str:
        """Serialize rows to CSV and upload to S3. Returns s3 URI."""
        if not rows:
            log.warning(f"No rows to upload for key: {s3_key}")
            return ""

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        uri = f"s3://{self.bucket}/{s3_key}"
        log.info(f"  Uploaded → {uri}  ({len(rows)} rows)")
        return uri

    def build_key(self, channel: str, file_date: str, filename: str) -> str:
        """
        Build S3 key matching the actual bucket layout:
          orders/<channel>/<YYYYMMDD>/<filename>.csv

        All entities (orders, customers, products) for a given channel
        share the same date folder, e.g.:
          orders/offline/20260209/offline_orders.csv
          orders/offline/20260209/offline_customers.csv
          orders/offline/20260209/offline_products.csv
          orders/online/20260209/online_orders.csv
          orders/online/20260209/online_customers.csv
          orders/online/20260209/online_products.csv
        """
        return f"orders/{channel}/{file_date}/{filename}.csv"

    def check_date_exists(self, file_date: str) -> bool:
        """
        Check if any files already exist in S3 for the given file_date.
        Searches under the 'orders/online/{file_date}/' and 'orders/offline/{file_date}/' prefixes.
        """
        for channel in ["online", "offline"]:
            prefix = f"orders/{channel}/{file_date}/"
            response = self.s3.list_objects_v2(
                Bucket=self.bucket, Prefix=prefix, MaxKeys=1
            )

            # If 'Contents' is returned, at least one file exists under this prefix
            if "Contents" in response and len(response["Contents"]) > 0:
                log.info(f"Found existing data in S3 under prefix: {prefix}")
                return True

        return False
