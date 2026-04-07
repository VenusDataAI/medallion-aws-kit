"""Tests for ingestion.s3_loader.S3Loader."""

from __future__ import annotations

import io
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from ingestion.s3_loader import S3Loader


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_s3():
    """Return a mock boto3 S3 client."""
    return MagicMock()


@pytest.fixture
def loader(mock_s3):
    """Return an S3Loader wired to a mock S3 client."""
    return S3Loader(
        bucket_name="myco-lakehouse-bronze",
        source_system="orders_system",
        entity="orders",
        s3_client=mock_s3,
    )


# ── Partition path tests ───────────────────────────────────────────────────────

class TestBuildKey:
    def test_standard_date(self, loader):
        key = loader._build_key("orders_20240307T143200.parquet", date(2024, 3, 7))
        assert key == "orders_system/orders/2024/03/07/orders_20240307T143200.parquet"

    def test_single_digit_month_day_zero_padded(self, loader):
        key = loader._build_key("file.parquet", date(2024, 1, 5))
        assert key == "orders_system/orders/2024/01/05/file.parquet"

    def test_december_last_day(self, loader):
        key = loader._build_key("data.parquet", date(2024, 12, 31))
        assert key == "orders_system/orders/2024/12/31/data.parquet"

    def test_key_starts_with_source_system(self, loader):
        key = loader._build_key("x.parquet", date(2024, 6, 15))
        assert key.startswith("orders_system/")

    def test_key_contains_entity(self, loader):
        key = loader._build_key("x.parquet", date(2024, 6, 15))
        assert "/orders/" in key


class TestGetPartitionPrefix:
    def test_returns_prefix_with_trailing_slash(self, loader):
        prefix = loader.get_partition_prefix(date(2024, 3, 7))
        assert prefix.endswith("/")

    def test_prefix_structure(self, loader):
        prefix = loader.get_partition_prefix(date(2024, 3, 7))
        assert prefix == "orders_system/orders/2024/03/07/"

    def test_defaults_to_today(self, loader):
        # Should not raise
        prefix = loader.get_partition_prefix()
        assert prefix.count("/") >= 4


# ── upload_file tests ─────────────────────────────────────────────────────────

class TestUploadFile:
    def test_calls_upload_file_with_correct_args(self, loader, mock_s3, tmp_path):
        local_file = tmp_path / "orders_20240307T143200.parquet"
        local_file.write_bytes(b"fake parquet data")
        target_date = date(2024, 3, 7)

        uri = loader.upload_file(local_path=local_file, partition_date=target_date)

        mock_s3.upload_file.assert_called_once_with(
            Filename=str(local_file),
            Bucket="myco-lakehouse-bronze",
            Key="orders_system/orders/2024/03/07/orders_20240307T143200.parquet",
            ExtraArgs={},
        )
        assert uri == "s3://myco-lakehouse-bronze/orders_system/orders/2024/03/07/orders_20240307T143200.parquet"

    def test_custom_s3_filename(self, loader, mock_s3, tmp_path):
        local_file = tmp_path / "local_name.parquet"
        local_file.write_bytes(b"data")
        target_date = date(2024, 3, 7)

        loader.upload_file(
            local_path=local_file,
            partition_date=target_date,
            s3_filename="custom_name.parquet",
        )

        call_kwargs = mock_s3.upload_file.call_args.kwargs
        assert call_kwargs["Key"].endswith("custom_name.parquet")

    def test_file_not_found_raises(self, loader):
        with pytest.raises(FileNotFoundError):
            loader.upload_file("/nonexistent/path/file.parquet", date(2024, 1, 1))

    def test_extra_args_forwarded(self, loader, mock_s3, tmp_path):
        local_file = tmp_path / "file.parquet"
        local_file.write_bytes(b"data")
        extra = {"ServerSideEncryption": "AES256"}

        loader.upload_file(local_file, date(2024, 1, 1), extra_args=extra)

        call_kwargs = mock_s3.upload_file.call_args.kwargs
        assert call_kwargs["ExtraArgs"] == extra

    def test_returns_s3_uri_string(self, loader, mock_s3, tmp_path):
        local_file = tmp_path / "file.parquet"
        local_file.write_bytes(b"data")

        uri = loader.upload_file(local_file, date(2024, 1, 1))

        assert uri.startswith("s3://myco-lakehouse-bronze/")


# ── upload_dataframe tests ─────────────────────────────────────────────────────

class TestUploadDataframe:
    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "order_id": ["ORD-001", "ORD-002"],
            "customer_id": ["CUST-A", "CUST-B"],
            "total_amount": [100.0, 200.0],
        })

    def test_calls_put_object(self, loader, mock_s3):
        df = self._make_df()
        target_date = date(2024, 3, 7)

        uri = loader.upload_dataframe(df, filename="orders_test", partition_date=target_date)

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "myco-lakehouse-bronze"
        assert "orders_system/orders/2024/03/07/orders_test.parquet" in call_kwargs["Key"]

    def test_appends_parquet_extension_if_missing(self, loader, mock_s3):
        df = self._make_df()
        loader.upload_dataframe(df, filename="myfile", partition_date=date(2024, 1, 1))
        call_kwargs = mock_s3.put_object.call_args.kwargs
        assert call_kwargs["Key"].endswith(".parquet")

    def test_does_not_double_parquet_extension(self, loader, mock_s3):
        df = self._make_df()
        loader.upload_dataframe(df, filename="myfile.parquet", partition_date=date(2024, 1, 1))
        call_kwargs = mock_s3.put_object.call_args.kwargs
        assert call_kwargs["Key"].endswith(".parquet")
        assert ".parquet.parquet" not in call_kwargs["Key"]

    def test_uploaded_bytes_are_valid_parquet(self, loader, mock_s3):
        df = self._make_df()
        loader.upload_dataframe(df, filename="test", partition_date=date(2024, 1, 1))

        call_kwargs = mock_s3.put_object.call_args.kwargs
        body_bytes = call_kwargs["Body"]
        buffer = io.BytesIO(body_bytes)
        result_df = pq.read_table(buffer).to_pandas()

        assert list(result_df.columns) == list(df.columns)
        assert len(result_df) == len(df)

    def test_returns_s3_uri(self, loader, mock_s3):
        df = self._make_df()
        uri = loader.upload_dataframe(df, filename="test", partition_date=date(2024, 1, 1))
        assert uri.startswith("s3://myco-lakehouse-bronze/")

    def test_partition_date_defaults_to_today(self, loader, mock_s3):
        df = self._make_df()
        # Should not raise; date is today
        loader.upload_dataframe(df, filename="test")
        mock_s3.put_object.assert_called_once()
