"""Tests for dbt macro SQL output.

Strategy: read the macro files and verify the SQL strings they generate
using lightweight string/regex assertions. This avoids needing a live
Redshift connection in CI while still catching regressions in macro logic.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

MACROS_DIR = Path(__file__).parent.parent / "dbt" / "macros"


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_macro(name: str) -> str:
    """Read a macro file by name (without .sql extension)."""
    path = MACROS_DIR / f"{name}.sql"
    return path.read_text(encoding="utf-8")


def normalise(sql: str) -> str:
    """Collapse whitespace for easier pattern matching."""
    return re.sub(r"\s+", " ", sql).strip()


# ── deduplicate macro ─────────────────────────────────────────────────────────

class TestDeduplicateMacro:
    def test_file_exists(self):
        assert (MACROS_DIR / "deduplicate.sql").exists()

    def test_macro_name_declared(self):
        src = read_macro("deduplicate")
        assert "macro deduplicate(" in src

    def test_contains_row_number(self):
        src = read_macro("deduplicate")
        assert "row_number()" in src.lower()

    def test_contains_partition_by(self):
        src = read_macro("deduplicate")
        assert "partition by" in src.lower()

    def test_contains_order_by(self):
        src = read_macro("deduplicate")
        assert "order by" in src.lower()

    def test_filters_on_rn_equals_1(self):
        src = read_macro("deduplicate")
        # Matches "_dedup_rn = 1" or "rn = 1" with optional whitespace
        assert re.search(r"_dedup_rn\s*=\s*1", src), "Expected rn = 1 filter"

    def test_unique_key_join(self):
        src = read_macro("deduplicate")
        # The macro should join the unique_key list
        assert "unique_key | join" in src or "join(', ')" in src

    def test_string_unique_key_coerced_to_list(self):
        src = read_macro("deduplicate")
        assert "is string" in src

    def test_has_docstring(self):
        src = read_macro("deduplicate")
        assert "{#" in src and "#}" in src


# ── standardize_boolean macro ─────────────────────────────────────────────────

class TestStandardizeBooleanMacro:
    def test_file_exists(self):
        assert (MACROS_DIR / "standardize_boolean.sql").exists()

    def test_macro_name_declared(self):
        src = read_macro("standardize_boolean")
        assert "macro standardize_boolean(" in src

    def test_handles_true_variants(self):
        src = normalise(read_macro("standardize_boolean")).lower()
        assert "'true'" in src

    def test_handles_false_variants(self):
        src = normalise(read_macro("standardize_boolean")).lower()
        assert "'false'" in src

    def test_handles_numeric_1(self):
        src = normalise(read_macro("standardize_boolean")).lower()
        assert "'1'" in src

    def test_handles_numeric_0(self):
        src = normalise(read_macro("standardize_boolean")).lower()
        assert "'0'" in src

    def test_null_fallback_present(self):
        src = normalise(read_macro("standardize_boolean")).lower()
        assert "else null" in src or "null" in src

    def test_uses_case_expression(self):
        src = read_macro("standardize_boolean").lower()
        assert "case" in src and "when" in src and "end" in src

    def test_case_insensitive_comparison(self):
        src = read_macro("standardize_boolean").lower()
        # lower() cast ensures case-insensitive matching
        assert "lower(" in src

    def test_has_docstring(self):
        src = read_macro("standardize_boolean")
        assert "{#" in src and "#}" in src


# ── add_metadata_columns macro ────────────────────────────────────────────────

class TestAddMetadataColumnsMacro:
    def test_file_exists(self):
        assert (MACROS_DIR / "add_metadata_columns.sql").exists()

    def test_macro_name_declared(self):
        src = read_macro("add_metadata_columns")
        assert "macro add_metadata_columns(" in src

    def test_outputs_ingested_at(self):
        src = read_macro("add_metadata_columns")
        assert "_ingested_at" in src

    def test_outputs_updated_at(self):
        src = read_macro("add_metadata_columns")
        assert "_updated_at" in src

    def test_outputs_source_system(self):
        src = read_macro("add_metadata_columns")
        assert "_source_system" in src

    def test_outputs_row_hash(self):
        src = read_macro("add_metadata_columns")
        assert "_row_hash" in src

    def test_uses_md5_for_hash(self):
        src = read_macro("add_metadata_columns").lower()
        assert "md5(" in src

    def test_uses_current_timestamp(self):
        src = read_macro("add_metadata_columns").lower()
        assert "current_timestamp" in src

    def test_business_cols_joined_with_pipe(self):
        src = read_macro("add_metadata_columns")
        # Columns are joined with '|' separator for the hash
        assert "'|'" in src or "| '|' |" in src

    def test_has_docstring(self):
        src = read_macro("add_metadata_columns")
        assert "{#" in src and "#}" in src

    def test_default_args_present(self):
        src = read_macro("add_metadata_columns")
        assert "source_system_col='source_system'" in src
        assert "ingested_at_col='_raw_timestamp'" in src


# ── not_null_proportion test macro ────────────────────────────────────────────

class TestNotNullProportionTest:
    TESTS_DIR = Path(__file__).parent.parent / "dbt" / "tests" / "generic"

    def test_file_exists(self):
        assert (self.TESTS_DIR / "not_null_proportion.sql").exists()

    def test_test_name_declared(self):
        src = (self.TESTS_DIR / "not_null_proportion.sql").read_text()
        assert "test not_null_proportion(" in src

    def test_has_min_proportion_argument(self):
        src = (self.TESTS_DIR / "not_null_proportion.sql").read_text()
        assert "min_proportion" in src

    def test_computes_proportion(self):
        src = (self.TESTS_DIR / "not_null_proportion.sql").read_text().lower()
        # Must divide count(column) by count(*)
        assert "count(" in src
        assert "1.0" in src or "/ nullif" in src

    def test_has_where_clause_filtering_below_threshold(self):
        src = (self.TESTS_DIR / "not_null_proportion.sql").read_text().lower()
        assert "actual_proportion < " in src or "< {{ min_proportion }}" in src

    def test_has_default_proportion(self):
        src = (self.TESTS_DIR / "not_null_proportion.sql").read_text()
        assert "0.95" in src

    def test_has_docstring(self):
        src = (self.TESTS_DIR / "not_null_proportion.sql").read_text()
        assert "{#" in src and "#}" in src
