"""Tests for parsing utilities module."""

from __future__ import annotations

import pytest

from pv_lakehouse.etl.utils.parsing import (
    parse_csv_list,
    parse_uppercase_csv_list,
)


class TestParseCsvList:
    """Tests for parse_csv_list function."""

    def test_parse_csv_list_basic(self) -> None:
        """Test basic comma-separated parsing."""
        result = parse_csv_list("a,b,c")
        assert result == ["a", "b", "c"]

    def test_parse_csv_list_with_whitespace(self) -> None:
        """Test parsing with whitespace around items."""
        result = parse_csv_list("  item1  ,  item2  ,  item3  ")
        assert result == ["item1", "item2", "item3"]

    def test_parse_csv_list_none_input(self) -> None:
        """Test None input returns empty list."""
        result = parse_csv_list(None)
        assert result == []

    def test_parse_csv_list_empty_string(self) -> None:
        """Test empty string returns empty list."""
        result = parse_csv_list("")
        assert result == []

    def test_parse_csv_list_single_item(self) -> None:
        """Test single item without comma."""
        result = parse_csv_list("single")
        assert result == ["single"]

    def test_parse_csv_list_empty_items(self) -> None:
        """Test empty items are filtered out."""
        result = parse_csv_list("a,,b,  ,c")
        assert result == ["a", "b", "c"]

    def test_parse_csv_list_only_whitespace(self) -> None:
        """Test only whitespace returns empty list."""
        result = parse_csv_list("   ,   ,   ")
        assert result == []

    def test_parse_csv_list_preserves_case(self) -> None:
        """Test that case is preserved."""
        result = parse_csv_list("ABC,def,GHI")
        assert result == ["ABC", "def", "GHI"]

    def test_parse_csv_list_complex_values(self) -> None:
        """Test complex values with special characters."""
        result = parse_csv_list("item-1, item_2, item.3")
        assert result == ["item-1", "item_2", "item.3"]

    def test_parse_csv_list_trailing_comma(self) -> None:
        """Test trailing comma is handled."""
        result = parse_csv_list("a,b,c,")
        assert result == ["a", "b", "c"]

    def test_parse_csv_list_leading_comma(self) -> None:
        """Test leading comma is handled."""
        result = parse_csv_list(",a,b,c")
        assert result == ["a", "b", "c"]


class TestParseUppercaseCsvList:
    """Tests for parse_uppercase_csv_list function."""

    def test_parse_uppercase_basic(self) -> None:
        """Test basic uppercase conversion."""
        result = parse_uppercase_csv_list("abc, DEF, ghi")
        assert result == ["ABC", "DEF", "GHI"]

    def test_parse_uppercase_none_input(self) -> None:
        """Test None input returns empty list."""
        result = parse_uppercase_csv_list(None)
        assert result == []

    def test_parse_uppercase_empty_string(self) -> None:
        """Test empty string returns empty list."""
        result = parse_uppercase_csv_list("")
        assert result == []

    def test_parse_uppercase_mixed_case(self) -> None:
        """Test mixed case is converted to uppercase."""
        result = parse_uppercase_csv_list("AbC, dEf, GhI")
        assert result == ["ABC", "DEF", "GHI"]

    def test_parse_uppercase_already_upper(self) -> None:
        """Test already uppercase items remain unchanged."""
        result = parse_uppercase_csv_list("ABC, DEF, GHI")
        assert result == ["ABC", "DEF", "GHI"]

    def test_parse_uppercase_with_numbers(self) -> None:
        """Test items with numbers are handled correctly."""
        result = parse_uppercase_csv_list("yarranl1, gullrwf1")
        assert result == ["YARRANL1", "GULLRWF1"]

    def test_parse_uppercase_empty_items_filtered(self) -> None:
        """Test empty items are filtered out."""
        result = parse_uppercase_csv_list("abc,,def,  ,ghi")
        assert result == ["ABC", "DEF", "GHI"]
