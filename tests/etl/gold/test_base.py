"""Tests for Gold layer base classes and options.

Tests cover:
- GoldLoadOptions configuration and validation
- Broadcast threshold settings
- Explain plan logging configuration
- Partition pruning options
"""

from __future__ import annotations

import datetime as dt

import pytest

from pv_lakehouse.etl.gold.base import (
    BaseGoldLoader,
    GoldLoadOptions,
    GoldTableConfig,
    SourceTableConfig,
)


class TestGoldLoadOptions:
    """Tests for GoldLoadOptions dataclass."""

    def test_default_values(self):
        """Default options should have sensible values."""
        options = GoldLoadOptions()

        assert options.mode == "incremental"
        assert options.load_strategy == "merge"
        assert options.broadcast_threshold_mb == 10
        assert options.enable_aqe is True
        assert options.partition_pruning is True
        assert options.explain_plan is False
        assert options.explain_mode == "simple"
        assert options.merge_keys is None

    def test_custom_broadcast_threshold(self):
        """Custom broadcast threshold should be accepted."""
        options = GoldLoadOptions(broadcast_threshold_mb=20)
        assert options.broadcast_threshold_mb == 20

    def test_explain_plan_modes(self):
        """All explain modes should be configurable."""
        for mode in ["simple", "extended", "codegen", "cost"]:
            options = GoldLoadOptions(explain_mode=mode)
            assert options.explain_mode == mode

    def test_datetime_options(self):
        """Start and end datetime options should work."""
        start = dt.datetime(2024, 1, 1, 0, 0, 0)
        end = dt.datetime(2024, 1, 31, 23, 59, 59)

        options = GoldLoadOptions(start=start, end=end)

        assert options.start == start
        assert options.end == end

    def test_merge_keys_configuration(self):
        """Merge keys should be configurable as list."""
        keys = ["facility_key", "date_key", "time_key"]
        options = GoldLoadOptions(merge_keys=keys)

        assert options.merge_keys == keys


class TestGoldLoadOptionsValidation:
    """Tests for GoldLoadOptions validation rules."""

    def test_invalid_mode_raises_error(self):
        """Invalid mode should raise ValueError during validation."""
        options = GoldLoadOptions(mode="invalid")

        class TestLoader(BaseGoldLoader):
            source_tables = {}
            gold_tables = {}

            def transform(self, sources):
                return None

        with pytest.raises(ValueError, match="mode must be 'full' or 'incremental'"):
            TestLoader(options)

    def test_invalid_load_strategy_raises_error(self):
        """Invalid load_strategy should raise ValueError."""
        options = GoldLoadOptions(load_strategy="invalid")

        class TestLoader(BaseGoldLoader):
            source_tables = {}
            gold_tables = {}

            def transform(self, sources):
                return None

        with pytest.raises(ValueError, match="load_strategy must be"):
            TestLoader(options)

    def test_invalid_explain_mode_raises_error(self):
        """Invalid explain_mode should raise ValueError."""
        options = GoldLoadOptions(explain_mode="invalid")

        class TestLoader(BaseGoldLoader):
            source_tables = {}
            gold_tables = {}

            def transform(self, sources):
                return None

        with pytest.raises(ValueError, match="explain_mode must be"):
            TestLoader(options)

    def test_negative_broadcast_threshold_raises_error(self):
        """Negative broadcast threshold should raise ValueError."""
        options = GoldLoadOptions(broadcast_threshold_mb=-1)

        class TestLoader(BaseGoldLoader):
            source_tables = {}
            gold_tables = {}

            def transform(self, sources):
                return None

        with pytest.raises(ValueError, match="broadcast_threshold_mb must be non-negative"):
            TestLoader(options)

    def test_end_before_start_raises_error(self):
        """End before start should raise ValueError."""
        options = GoldLoadOptions(
            start=dt.datetime(2024, 2, 1),
            end=dt.datetime(2024, 1, 1),
        )

        class TestLoader(BaseGoldLoader):
            source_tables = {}
            gold_tables = {}

            def transform(self, sources):
                return None

        with pytest.raises(ValueError, match="end must not be before"):
            TestLoader(options)


class TestSourceTableConfig:
    """Tests for SourceTableConfig dataclass."""

    def test_minimal_config(self):
        """Minimal config with only table name should work."""
        config = SourceTableConfig(table_name="lh.silver.test_table")

        assert config.table_name == "lh.silver.test_table"
        assert config.timestamp_column is None
        assert config.required_columns is None

    def test_full_config(self):
        """Full config with all fields should work."""
        config = SourceTableConfig(
            table_name="lh.silver.clean_hourly_energy",
            timestamp_column="date_hour",
            required_columns=["facility_code", "energy_mwh"],
        )

        assert config.table_name == "lh.silver.clean_hourly_energy"
        assert config.timestamp_column == "date_hour"
        assert "facility_code" in config.required_columns


class TestGoldTableConfig:
    """Tests for GoldTableConfig dataclass."""

    def test_minimal_config(self):
        """Minimal config should have empty partition cols."""
        config = GoldTableConfig(
            iceberg_table="lh.gold.test_fact",
            s3_base_path="s3a://lakehouse/gold/test_fact",
        )

        assert config.iceberg_table == "lh.gold.test_fact"
        assert config.s3_base_path == "s3a://lakehouse/gold/test_fact"
        assert config.partition_cols == ()

    def test_with_partition_cols(self):
        """Config with partition columns should work."""
        config = GoldTableConfig(
            iceberg_table="lh.gold.fact_solar",
            s3_base_path="s3a://lakehouse/gold/fact_solar",
            partition_cols=("date_key",),
        )

        assert "date_key" in config.partition_cols
