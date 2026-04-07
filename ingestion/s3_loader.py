"""S3Loader — uploads files or DataFrames to the bronze lakehouse bucket.

Partition scheme:
    s3://{bucket}/{source_system}/{entity}/{year}/{month}/{day}/{filename}
"""

from __future__ import annotations

import io
import logging
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Optional, Union

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import BaseClient

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


class S3Loader:
    """Upload local files or pandas DataFrames to an S3 lakehouse bucket.

    Args:
        bucket_name: Name of the target S3 bucket (e.g. ``myco-lakehouse-bronze``).
        source_system: Identifier of the originating system (e.g. ``orders_system``).
        entity: Logical entity name (e.g. ``orders``).
        s3_client: Optional pre-configured boto3 S3 client. A new client using
            the default credential chain is created when not provided.
    """

    def __init__(
        self,
        bucket_name: str,
        source_system: str,
        entity: str,
        s3_client: Optional[BaseClient] = None,
    ) -> None:
        self.bucket_name = bucket_name
        self.source_system = source_system
        self.entity = entity
        self._s3: BaseClient = s3_client or boto3.client("s3")

    # ── Public API ────────────────────────────────────────────────────────────

    def upload_file(
        self,
        local_path: Union[str, Path],
        partition_date: Optional[date] = None,
        s3_filename: Optional[str] = None,
        extra_args: Optional[dict] = None,
    ) -> str:
        """Upload a local file to the correct S3 partition.

        Args:
            local_path: Path to the local file.
            partition_date: Date used for year/month/day partition.
                Defaults to today's UTC date.
            s3_filename: Override the destination filename.
                Defaults to the local file's basename.
            extra_args: Extra arguments forwarded to ``put_object``
                (e.g. ``{"ServerSideEncryption": "AES256"}``).

        Returns:
            Full S3 URI of the uploaded object.
        """
        local_path = Path(local_path)
        if not local_path.is_file():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        partition_date = partition_date or date.today()
        key = self._build_key(
            filename=s3_filename or local_path.name,
            partition_date=partition_date,
        )

        logger.info("Uploading %s → s3://%s/%s", local_path, self.bucket_name, key)
        self._s3.upload_file(
            Filename=str(local_path),
            Bucket=self.bucket_name,
            Key=key,
            ExtraArgs=extra_args or {},
        )
        return f"s3://{self.bucket_name}/{key}"

    def upload_dataframe(
        self,
        df: "pd.DataFrame",
        filename: Optional[str] = None,
        partition_date: Optional[date] = None,
        compression: str = "snappy",
        extra_args: Optional[dict] = None,
    ) -> str:
        """Serialise a pandas DataFrame to Parquet and upload to S3.

        Args:
            df: The DataFrame to upload.
            filename: Destination filename (without extension).
                Defaults to ``<entity>_<timestamp>.parquet``.
            partition_date: Date used for year/month/day partition.
                Defaults to today's UTC date.
            compression: Parquet compression codec (default: ``snappy``).
            extra_args: Extra arguments forwarded to ``put_object``.

        Returns:
            Full S3 URI of the uploaded object.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for upload_dataframe.")

        partition_date = partition_date or date.today()
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        filename = filename or f"{self.entity}_{ts}.parquet"
        if not filename.endswith(".parquet"):
            filename = f"{filename}.parquet"

        key = self._build_key(filename=filename, partition_date=partition_date)

        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, buffer, compression=compression)
        buffer.seek(0)

        logger.info(
            "Uploading DataFrame (%d rows) → s3://%s/%s",
            len(df),
            self.bucket_name,
            key,
        )
        self._s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=buffer.getvalue(),
            **(extra_args or {}),
        )
        return f"s3://{self.bucket_name}/{key}"

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _build_key(self, filename: str, partition_date: date) -> str:
        """Build the full S3 object key following the lakehouse partition scheme.

        Scheme: {source_system}/{entity}/{year}/{month}/{day}/{filename}
        """
        return (
            f"{self.source_system}"
            f"/{self.entity}"
            f"/{partition_date.year:04d}"
            f"/{partition_date.month:02d}"
            f"/{partition_date.day:02d}"
            f"/{filename}"
        )

    def get_partition_prefix(self, partition_date: Optional[date] = None) -> str:
        """Return the S3 prefix for a given partition date (useful for listing).

        Args:
            partition_date: Target date. Defaults to today.

        Returns:
            S3 prefix string (without leading slash).
        """
        partition_date = partition_date or date.today()
        return (
            f"{self.source_system}"
            f"/{self.entity}"
            f"/{partition_date.year:04d}"
            f"/{partition_date.month:02d}"
            f"/{partition_date.day:02d}/"
        )
