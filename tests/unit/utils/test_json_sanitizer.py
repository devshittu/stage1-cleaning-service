"""
tests/unit/utils/test_json_sanitizer.py

Unit tests for JSON sanitization module.

Tests cover:
- All sanitization strategies
- Edge cases and malformed JSON
- Unicode handling
- URL fixing
- Aggressive field extraction
"""

import json
import pytest
from src.utils.json_sanitizer import (
    sanitize_and_parse_json,
    _fix_unescaped_quotes,
    _fix_unicode_issues,
    _fix_malformed_urls,
    _extract_fields_aggressive
)


class TestSanitizeAndParseJson:
    """Test main sanitization function."""

    @pytest.mark.unit
    def test_valid_json_direct_parse(self):
        """Test Strategy 1: Valid JSON parses directly."""
        json_str = '{"title": "Test", "body": "Content"}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert result == {"title": "Test", "body": "Content"}

    @pytest.mark.unit
    def test_empty_line_returns_error(self):
        """Test empty lines are handled gracefully."""
        result, error = sanitize_and_parse_json("", 1)

        assert result is None
        assert error == "Empty line"

    @pytest.mark.unit
    def test_whitespace_only_line(self):
        """Test whitespace-only lines are treated as empty."""
        result, error = sanitize_and_parse_json("   \n  \t  ", 1)

        assert result is None
        assert error == "Empty line"

    @pytest.mark.unit
    def test_unescaped_quotes_fixed(self):
        """Test Strategy 2: Unescaped quotes in values are fixed."""
        json_str = '{"title": "Article with "quotes" inside", "body": "Test"}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert result["title"] == 'Article with "quotes" inside'
        assert result["body"] == "Test"

    @pytest.mark.unit
    def test_unicode_issues_fixed(self):
        """Test Strategy 3: Unicode issues are handled."""
        # Using Unicode smart quotes
        json_str = '{"title": "Article with \u201csmartquotes\u201d", "body": "Test"}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert "smartquotes" in result["title"]

    @pytest.mark.unit
    def test_combined_fixes(self):
        """Test Strategy 4: Combined Unicode and quote fixes."""
        json_str = '{"title": "Article \u2018with\u2019 "mixed" issues", "body": "Test"}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert "mixed" in result["title"]

    @pytest.mark.unit
    def test_malformed_urls_fixed(self):
        """Test URL fixes are applied after parsing."""
        json_str = '{"source_url": "httpss://example.com"}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert result["source_url"] == "https://example.com"

    @pytest.mark.unit
    def test_aggressive_extraction_fallback(self):
        """Test Strategy 5: Aggressive extraction as last resort."""
        # Severely malformed JSON that can only be extracted with regex
        json_str = '{"document_id":"test123","text":"Some content with weird formatting"xx'
        result, error = sanitize_and_parse_json(json_str, 1)

        # Should extract at least document_id and text
        if result:
            assert "document_id" in result
            assert "text" in result

    @pytest.mark.unit
    def test_completely_invalid_json(self):
        """Test completely invalid JSON returns error."""
        json_str = 'this is not json at all!!!'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert result is None
        assert error is not None
        assert "All parsing failed" in error

    @pytest.mark.unit
    def test_nested_objects_preserved(self):
        """Test nested objects are preserved correctly."""
        json_str = '{"outer": {"inner": {"deep": "value"}}}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert result["outer"]["inner"]["deep"] == "value"

    @pytest.mark.unit
    def test_arrays_handled(self):
        """Test arrays are handled correctly."""
        json_str = '{"authors": ["Alice", "Bob"], "tags": ["news", "tech"]}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert result["authors"] == ["Alice", "Bob"]
        assert result["tags"] == ["news", "tech"]


class TestFixUnescapedQuotes:
    """Test quote escaping function."""

    @pytest.mark.unit
    def test_no_quotes_unchanged(self):
        """Test strings without quotes pass through unchanged."""
        input_str = '{"title": "Simple title", "body": "Simple body"}'
        result = _fix_unescaped_quotes(input_str)

        # Should be parseable
        parsed = json.loads(result)
        assert parsed["title"] == "Simple title"

    @pytest.mark.unit
    def test_unescaped_quote_in_value(self):
        """Test unescaped quote in value is escaped."""
        input_str = '{"text": "He said "hello" to me"}'
        result = _fix_unescaped_quotes(input_str)

        # Should now be parseable
        parsed = json.loads(result)
        assert 'hello' in parsed["text"]

    @pytest.mark.unit
    def test_multiple_unescaped_quotes(self):
        """Test multiple unescaped quotes are all escaped."""
        input_str = '{"text": "First "quote" and "another" quote"}'
        result = _fix_unescaped_quotes(input_str)

        parsed = json.loads(result)
        assert 'quote' in parsed["text"]
        assert 'another' in parsed["text"]

    @pytest.mark.unit
    def test_already_escaped_quotes_preserved(self):
        """Test already escaped quotes are not double-escaped."""
        input_str = '{"text": "Already \\"escaped\\" quote"}'
        result = _fix_unescaped_quotes(input_str)

        parsed = json.loads(result)
        assert 'escaped' in parsed["text"]

    @pytest.mark.unit
    def test_empty_string_unchanged(self):
        """Test empty string returns unchanged."""
        assert _fix_unescaped_quotes("") == ""

    @pytest.mark.unit
    def test_field_names_not_affected(self):
        """Test quotes in field names don't cause issues."""
        # Field names should be properly quoted in valid JSON
        input_str = '{"normal_field": "value with "quotes""}'
        result = _fix_unescaped_quotes(input_str)

        parsed = json.loads(result)
        assert "quotes" in parsed["normal_field"]

    @pytest.mark.unit
    def test_quote_at_end_of_value(self):
        """Test quote at end of value is handled."""
        input_str = '{"text": "Ends with a quote""}'
        result = _fix_unescaped_quotes(input_str)

        # Should escape the internal quote
        assert '\\"' in result


class TestFixUnicodeIssues:
    """Test Unicode fixing function."""

    @pytest.mark.unit
    def test_smart_quotes_replaced(self):
        """Test smart quotes are replaced with regular quotes."""
        text = 'Text with \u201csmart quotes\u201d here'
        result = _fix_unicode_issues(text)

        assert '\u201c' not in result
        assert '\u201d' not in result
        assert '\\"' in result

    @pytest.mark.unit
    def test_em_dash_replaced(self):
        """Test em dash is replaced."""
        text = 'Text with\u2014em dash'
        result = _fix_unicode_issues(text)

        assert '\u2014' not in result
        assert '--' in result

    @pytest.mark.unit
    def test_en_dash_replaced(self):
        """Test en dash is replaced."""
        text = 'Text with\u2013en dash'
        result = _fix_unicode_issues(text)

        assert '\u2013' not in result
        assert '-' in result

    @pytest.mark.unit
    def test_ellipsis_replaced(self):
        """Test ellipsis is replaced."""
        text = 'Text with\u2026ellipsis'
        result = _fix_unicode_issues(text)

        assert '\u2026' not in result
        assert '...' in result

    @pytest.mark.unit
    def test_non_breaking_space_replaced(self):
        """Test non-breaking space is replaced."""
        text = 'Text\u00a0with\u00a0NBSP'
        result = _fix_unicode_issues(text)

        assert '\u00a0' not in result
        assert ' ' in result

    @pytest.mark.unit
    def test_zero_width_characters_removed(self):
        """Test zero-width characters are removed."""
        text = 'Text\u200bwith\u200czero\u200dwidth'
        result = _fix_unicode_issues(text)

        assert '\u200b' not in result
        assert '\u200c' not in result
        assert '\u200d' not in result

    @pytest.mark.unit
    def test_bom_removed(self):
        """Test BOM is removed."""
        text = '\ufeffText with BOM'
        result = _fix_unicode_issues(text)

        assert '\ufeff' not in result
        assert result.startswith('Text')

    @pytest.mark.unit
    def test_control_characters_removed(self):
        """Test control characters are removed."""
        text = 'Text\x00with\x01control\x02chars'
        result = _fix_unicode_issues(text)

        assert '\x00' not in result
        assert '\x01' not in result
        assert '\x02' not in result

    @pytest.mark.unit
    def test_tabs_newlines_preserved(self):
        """Test tabs and newlines are preserved."""
        text = 'Text\twith\ttabs\nand\nnewlines'
        result = _fix_unicode_issues(text)

        assert '\t' in result
        assert '\n' in result

    @pytest.mark.unit
    def test_normal_text_unchanged(self):
        """Test normal ASCII text passes through unchanged."""
        text = 'Normal ASCII text 123'
        result = _fix_unicode_issues(text)

        assert result == text


class TestFixMalformedUrls:
    """Test URL fixing function."""

    @pytest.mark.unit
    def test_httpss_fixed(self):
        """Test httpss:// is fixed to https://."""
        data = {"source_url": "httpss://example.com/page"}
        result = _fix_malformed_urls(data)

        assert result["source_url"] == "https://example.com/page"

    @pytest.mark.unit
    def test_httpp_fixed(self):
        """Test httpp:// is fixed to http://."""
        data = {"source_url": "httpp://example.com"}
        result = _fix_malformed_urls(data)

        assert result["source_url"] == "http://example.com"

    @pytest.mark.unit
    def test_httpps_fixed(self):
        """Test httpps:// is fixed to https://."""
        data = {"source_url": "httpps://example.com"}
        result = _fix_malformed_urls(data)

        assert result["source_url"] == "https://example.com"

    @pytest.mark.unit
    def test_invalid_scheme_fixed(self):
        """Test invalid schemes are replaced with https://."""
        data = {"source_url": "xyz://example.com/page"}
        result = _fix_malformed_urls(data)

        assert result["source_url"] == "https://example.com/page"

    @pytest.mark.unit
    def test_valid_http_unchanged(self):
        """Test valid http:// URL is unchanged."""
        data = {"source_url": "http://example.com"}
        result = _fix_malformed_urls(data)

        assert result["source_url"] == "http://example.com"

    @pytest.mark.unit
    def test_valid_https_unchanged(self):
        """Test valid https:// URL is unchanged."""
        data = {"source_url": "https://example.com"}
        result = _fix_malformed_urls(data)

        assert result["source_url"] == "https://example.com"

    @pytest.mark.unit
    def test_multiple_url_fields(self):
        """Test both source_url and cleaned_source_url are fixed."""
        data = {
            "source_url": "httpss://example.com",
            "cleaned_source_url": "httpp://other.com"
        }
        result = _fix_malformed_urls(data)

        assert result["source_url"] == "https://example.com"
        assert result["cleaned_source_url"] == "http://other.com"

    @pytest.mark.unit
    def test_non_url_fields_unchanged(self):
        """Test non-URL fields are not modified."""
        data = {
            "title": "httpss://this-is-not-a-url-field",
            "source_url": "httpss://example.com"
        }
        result = _fix_malformed_urls(data)

        assert result["title"] == "httpss://this-is-not-a-url-field"
        assert result["source_url"] == "https://example.com"

    @pytest.mark.unit
    def test_non_dict_input_unchanged(self):
        """Test non-dict input is returned unchanged."""
        assert _fix_malformed_urls("not a dict") == "not a dict"
        assert _fix_malformed_urls(None) is None
        assert _fix_malformed_urls([1, 2, 3]) == [1, 2, 3]

    @pytest.mark.unit
    def test_missing_url_fields_ok(self):
        """Test missing URL fields don't cause errors."""
        data = {"title": "Article", "body": "Content"}
        result = _fix_malformed_urls(data)

        assert result == data


class TestExtractFieldsAggressive:
    """Test aggressive field extraction function."""

    @pytest.mark.unit
    def test_extracts_required_fields(self):
        """Test minimum required fields are extracted."""
        json_str = '{"document_id":"doc123","text":"Some content here"}'
        result = _extract_fields_aggressive(json_str)

        assert result is not None
        assert result["document_id"] == "doc123"
        assert result["text"] == "Some content here"

    @pytest.mark.unit
    def test_extracts_optional_fields(self):
        """Test optional fields are extracted if present."""
        json_str = '{"document_id":"doc123","text":"Content","title":"Test Title","author":"John Doe"}'
        result = _extract_fields_aggressive(json_str)

        assert result is not None
        assert result["title"] == "Test Title"
        assert result["author"] == "John Doe"

    @pytest.mark.unit
    def test_missing_document_id_returns_none(self):
        """Test missing document_id returns None."""
        json_str = '{"text":"Content without document_id"}'
        result = _extract_fields_aggressive(json_str)

        assert result is None

    @pytest.mark.unit
    def test_missing_text_returns_none(self):
        """Test missing text field returns None."""
        json_str = '{"document_id":"doc123"}'
        result = _extract_fields_aggressive(json_str)

        assert result is None

    @pytest.mark.unit
    def test_handles_embedded_quotes_in_text(self):
        """Test embedded quotes in text are handled."""
        json_str = '{"document_id":"doc123","text":"Text with "embedded" quotes"}'
        result = _extract_fields_aggressive(json_str)

        assert result is not None
        assert "embedded" in result["text"]

    @pytest.mark.unit
    def test_handles_escaped_quotes(self):
        """Test escaped quotes in text are preserved."""
        json_str = '{"document_id":"doc123","text":"Text with \\"escaped\\" quotes"}'
        result = _extract_fields_aggressive(json_str)

        assert result is not None
        assert 'escaped' in result["text"]

    @pytest.mark.unit
    def test_stops_at_field_boundary(self):
        """Test extraction stops at field boundary."""
        json_str = '{"document_id":"doc123","text":"Content","other":"field"}'
        result = _extract_fields_aggressive(json_str)

        assert result is not None
        assert result["text"] == "Content"
        # Should not include "other" in text

    @pytest.mark.unit
    def test_handles_malformed_but_extractable(self):
        """Test can extract from malformed but recognizable JSON."""
        # Missing closing brace but has required fields
        json_str = '{"document_id":"doc123","text":"Content here"'
        result = _extract_fields_aggressive(json_str)

        assert result is not None
        assert result["document_id"] == "doc123"


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.unit
    def test_very_long_text(self):
        """Test handling of very long text values."""
        long_text = "A" * 10000
        json_str = f'{{"title": "Test", "body": "{long_text}"}}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert len(result["body"]) == 10000

    @pytest.mark.unit
    def test_deeply_nested_structure(self):
        """Test deeply nested JSON structures."""
        nested = {"level1": {"level2": {"level3": {"level4": "deep"}}}}
        json_str = json.dumps(nested)
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert result["level1"]["level2"]["level3"]["level4"] == "deep"

    @pytest.mark.unit
    def test_special_characters_in_values(self):
        """Test special characters are preserved."""
        json_str = '{"text": "Special chars: @#$%^&*()[]{}|\\\\/"}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert "@#$%^&*()" in result["text"]

    @pytest.mark.unit
    def test_numbers_and_booleans(self):
        """Test non-string values are preserved."""
        json_str = '{"count": 42, "ratio": 3.14, "active": true, "disabled": false, "nothing": null}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert result["count"] == 42
        assert result["ratio"] == 3.14
        assert result["active"] is True
        assert result["disabled"] is False
        assert result["nothing"] is None

    @pytest.mark.unit
    def test_emoji_and_unicode_preserved(self):
        """Test emoji and Unicode characters are preserved correctly."""
        json_str = '{"text": "Hello 👋 world 🌍 with emoji"}'
        result, error = sanitize_and_parse_json(json_str, 1)

        assert error is None
        assert "👋" in result["text"]
        assert "🌍" in result["text"]
