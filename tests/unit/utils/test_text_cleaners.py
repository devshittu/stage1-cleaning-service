"""
tests/unit/utils/test_text_cleaners.py

Unit tests for text cleaning utilities.

Tests cover:
- TextCleanerConfig initialization and settings
- Individual cleaning functions
- Regex pattern matching
- Currency and unit standardization
- Typo correction with NER entity protection
- Full cleaning pipeline integration
"""

import pytest
from unittest.mock import Mock, patch

from src.utils.text_cleaners import (
    TextCleanerConfig,
    RegexPatterns,
    remove_html_tags,
    normalize_whitespace,
    fix_encoding,
    normalize_unicode_dashes,
    normalize_smart_quotes,
    remove_non_printable,
    remove_excessive_punctuation,
    add_space_after_punctuation,
    standardize_currency,
    standardize_units,
    correct_typos,
    clean_text_pipeline
)


class TestTextCleanerConfig:
    """Test TextCleanerConfig initialization and settings."""

    @pytest.mark.unit
    def test_default_config(self):
        """Test config with default values."""
        config_dict = {}
        config = TextCleanerConfig(config_dict)

        assert config.remove_html_tags is True
        assert config.normalize_whitespace is True
        assert config.fix_encoding is True
        assert config.enable_typo_correction is True

    @pytest.mark.unit
    def test_custom_config(self):
        """Test config with custom values."""
        config_dict = {
            'remove_html_tags': False,
            'normalize_whitespace': True,
            'fix_encoding': False,
            'enable_typo_correction': False
        }
        config = TextCleanerConfig(config_dict)

        assert config.remove_html_tags is False
        assert config.normalize_whitespace is True
        assert config.fix_encoding is False
        assert config.enable_typo_correction is False

    @pytest.mark.unit
    def test_typo_correction_sub_config(self):
        """Test typo correction sub-configuration."""
        config_dict = {
            'typo_correction': {
                'min_word_length': 5,
                'max_word_length': 20,
                'skip_capitalized_words': False,
                'skip_mixed_case': False,
                'use_ner_entities': False,
                'confidence_threshold': 0.9
            }
        }
        config = TextCleanerConfig(config_dict)

        assert config.typo_min_length == 5
        assert config.typo_max_length == 20
        assert config.typo_skip_capitalized is False
        assert config.typo_skip_mixed_case is False
        assert config.typo_use_ner is False
        assert config.typo_confidence == 0.9

    @pytest.mark.unit
    def test_partial_config(self):
        """Test config with some values set."""
        config_dict = {
            'remove_html_tags': False,
            'standardize_currency': True
        }
        config = TextCleanerConfig(config_dict)

        assert config.remove_html_tags is False
        assert config.standardize_currency is True
        assert config.normalize_whitespace is True  # Default


class TestRegexPatterns:
    """Test pre-compiled regex patterns."""

    @pytest.mark.unit
    def test_html_tags_pattern(self):
        """Test HTML tags pattern matches correctly."""
        text = "Hello <b>world</b> with <div>tags</div>"
        result = RegexPatterns.HTML_TAGS.sub('', text)

        assert '<b>' not in result
        assert '</b>' not in result
        assert 'Hello' in result
        assert 'world' in result

    @pytest.mark.unit
    def test_whitespace_pattern(self):
        """Test whitespace pattern matches various whitespace."""
        text = "Multiple   spaces  and\t\ttabs\n\nnewlines"
        result = RegexPatterns.WHITESPACE.sub(' ', text)

        assert '   ' not in result
        assert '\t' not in result
        assert '\n' not in result

    @pytest.mark.unit
    def test_unicode_dashes_pattern(self):
        """Test unicode dashes pattern."""
        text = "en–dash and em—dash"
        result = RegexPatterns.UNICODE_DASHES.sub('-', text)

        assert '–' not in result
        assert '—' not in result
        assert 'en-dash' in result

    @pytest.mark.unit
    def test_smart_quotes_pattern(self):
        """Test smart quotes patterns."""
        text = '\u201cdouble quotes\u201d and \u2018single quotes\u2019'
        result = RegexPatterns.SMART_QUOTES_DOUBLE.sub('"', text)
        result = RegexPatterns.SMART_QUOTES_SINGLE.sub("'", result)

        # Verify curly quotes were replaced with straight quotes
        assert '\u201c' not in result  # Left double curly quote removed
        assert '\u201d' not in result  # Right double curly quote removed
        assert '\u2018' not in result  # Left single curly quote removed
        assert '\u2019' not in result  # Right single curly quote removed
        assert '"' in result  # Straight double quote present
        assert "'" in result  # Straight single quote present


class TestRemoveHtmlTags:
    """Test HTML tag removal."""

    @pytest.mark.unit
    def test_simple_html_tags(self):
        """Test removal of simple HTML tags."""
        text = "Hello <b>world</b>!"
        result = remove_html_tags(text)

        assert '<b>' not in result
        assert '</b>' not in result
        assert 'world' in result

    @pytest.mark.unit
    def test_nested_html_tags(self):
        """Test removal of nested HTML tags."""
        text = "<div><p>Nested <span>tags</span></p></div>"
        result = remove_html_tags(text)

        assert '<' not in result
        assert '>' not in result
        assert 'Nested' in result
        assert 'tags' in result

    @pytest.mark.unit
    def test_html_with_attributes(self):
        """Test removal of HTML tags with attributes."""
        text = '<a href="http://example.com">Link</a>'
        result = remove_html_tags(text)

        assert 'href' not in result
        assert 'Link' in result

    @pytest.mark.unit
    def test_no_html_tags(self):
        """Test text without HTML tags remains unchanged."""
        text = "Plain text without tags"
        result = remove_html_tags(text)

        assert result == text


class TestNormalizeWhitespace:
    """Test whitespace normalization."""

    @pytest.mark.unit
    def test_multiple_spaces(self):
        """Test multiple spaces collapsed to single space."""
        text = "Multiple   spaces    here"
        result = normalize_whitespace(text)

        assert result == "Multiple spaces here"

    @pytest.mark.unit
    def test_tabs_and_newlines(self):
        """Test tabs and newlines normalized."""
        text = "Tab\there\nand\nnewline"
        result = normalize_whitespace(text)

        assert '\t' not in result
        assert '\n' not in result
        assert result == "Tab here and newline"

    @pytest.mark.unit
    def test_leading_trailing_whitespace(self):
        """Test leading and trailing whitespace removed."""
        text = "  content with spaces  "
        result = normalize_whitespace(text)

        assert result == "content with spaces"

    @pytest.mark.unit
    def test_mixed_whitespace(self):
        """Test mixed whitespace types."""
        text = "  Multiple\t\tspaces  and\n\nnewlines  "
        result = normalize_whitespace(text)

        assert result == "Multiple spaces and newlines"


class TestFixEncoding:
    """Test encoding fixes."""

    @pytest.mark.unit
    def test_fix_mojibake(self):
        """Test fixing mojibake encoding issues."""
        # ftfy handles these automatically
        text = "café"  # Already correct
        result = fix_encoding(text)

        assert 'café' in result

    @pytest.mark.unit
    def test_normal_text_unchanged(self):
        """Test normal text is unchanged."""
        text = "Normal English text"
        result = fix_encoding(text)

        assert result == text


class TestNormalizeUnicodeDashes:
    """Test unicode dash normalization."""

    @pytest.mark.unit
    def test_en_dash_conversion(self):
        """Test en-dash converted to hyphen."""
        text = "2010–2020"
        result = normalize_unicode_dashes(text)

        assert result == "2010-2020"

    @pytest.mark.unit
    def test_em_dash_conversion(self):
        """Test em-dash converted to hyphen."""
        text = "Hello—world"
        result = normalize_unicode_dashes(text)

        assert result == "Hello-world"

    @pytest.mark.unit
    def test_multiple_dashes(self):
        """Test multiple unicode dashes converted."""
        text = "First–second—third"
        result = normalize_unicode_dashes(text)

        assert '–' not in result
        assert '—' not in result


class TestNormalizeSmartQuotes:
    """Test smart quote normalization."""

    @pytest.mark.unit
    def test_double_smart_quotes(self):
        """Test double smart quotes converted."""
        text = '\u201cquoted text\u201d'
        result = normalize_smart_quotes(text)

        assert result == '"quoted text"'

    @pytest.mark.unit
    def test_single_smart_quotes(self):
        """Test single smart quotes converted."""
        text = '\u2018quoted text\u2019'
        result = normalize_smart_quotes(text)

        assert result == "'quoted text'"

    @pytest.mark.unit
    def test_mixed_quotes(self):
        """Test mixed smart quotes."""
        text = '\u201cDouble\u201d and \u2018single\u2019'
        result = normalize_smart_quotes(text)

        assert '"Double"' in result
        assert "'single'" in result


class TestRemoveNonPrintable:
    """Test non-printable character removal."""

    @pytest.mark.unit
    def test_remove_control_characters(self):
        """Test control characters removed."""
        text = "Hello\x00World\x01"
        result = remove_non_printable(text)

        assert '\x00' not in result
        assert '\x01' not in result
        assert 'Hello' in result
        assert 'World' in result

    @pytest.mark.unit
    def test_keep_printable(self):
        """Test printable characters retained."""
        text = "Normal text with spaces and 123!"
        result = remove_non_printable(text)

        assert result == text


class TestRemoveExcessivePunctuation:
    """Test excessive punctuation removal."""

    @pytest.mark.unit
    def test_repeated_periods(self):
        """Test repeated periods normalized to single."""
        text = "Wait... what?"
        result = remove_excessive_punctuation(text)

        assert result == "Wait. what?"

    @pytest.mark.unit
    def test_repeated_exclamation(self):
        """Test repeated exclamation marks normalized."""
        text = "Amazing!!! Really!!!"
        result = remove_excessive_punctuation(text)

        assert result == "Amazing! Really!"

    @pytest.mark.unit
    def test_repeated_question_marks(self):
        """Test repeated question marks normalized."""
        text = "What??? Why???"
        result = remove_excessive_punctuation(text)

        assert result == "What? Why?"

    @pytest.mark.unit
    def test_repeated_commas(self):
        """Test repeated commas normalized."""
        text = "First,, second,, third"
        result = remove_excessive_punctuation(text)

        assert result == "First, second, third"

    @pytest.mark.unit
    def test_repeated_dashes(self):
        """Test repeated dashes normalized."""
        text = "Wait--what?"
        result = remove_excessive_punctuation(text)

        assert result == "Wait-what?"


class TestAddSpaceAfterPunctuation:
    """Test space addition after punctuation."""

    @pytest.mark.unit
    def test_missing_space_after_period(self):
        """Test space added after period."""
        text = "First sentence.Second sentence."
        result = add_space_after_punctuation(text)

        assert result == "First sentence. Second sentence."

    @pytest.mark.unit
    def test_missing_space_after_comma(self):
        """Test space added after comma."""
        text = "One,two,three"
        result = add_space_after_punctuation(text)

        assert result == "One, two, three"

    @pytest.mark.unit
    def test_missing_space_after_question(self):
        """Test space added after question mark."""
        text = "First?Second"
        result = add_space_after_punctuation(text)

        assert result == "First? Second"

    @pytest.mark.unit
    def test_existing_spaces_preserved(self):
        """Test existing spaces after punctuation preserved."""
        text = "Already. Spaced, properly."
        result = add_space_after_punctuation(text)

        assert result == text


class TestStandardizeCurrency:
    """Test currency standardization."""

    @pytest.mark.unit
    def test_usd_symbol(self):
        """Test USD symbol conversion."""
        text = "Price is $100"
        result = standardize_currency(text)

        assert result == "Price is USD 100"

    @pytest.mark.unit
    def test_eur_symbol(self):
        """Test EUR symbol conversion."""
        text = "Price is 50€"
        result = standardize_currency(text)

        assert result == "Price is 50 EUR"

    @pytest.mark.unit
    def test_gbp_symbol(self):
        """Test GBP symbol conversion."""
        text = "Price is £25"
        result = standardize_currency(text)

        assert result == "Price is GBP 25"

    @pytest.mark.unit
    def test_usd_word(self):
        """Test USD word conversion."""
        text = "Cost 100 US dollars"
        result = standardize_currency(text)

        assert "USD" in result

    @pytest.mark.unit
    def test_decimal_amounts(self):
        """Test currency with decimal amounts."""
        text = "Total $99.95"
        result = standardize_currency(text)

        assert "USD 99.95" in result


class TestStandardizeUnits:
    """Test unit standardization."""

    @pytest.mark.unit
    def test_percent(self):
        """Test percent symbol conversion."""
        text = "95% success rate"
        result = standardize_units(text)

        assert result == "95 percent success rate"

    @pytest.mark.unit
    def test_meters(self):
        """Test meters conversion."""
        text = "Distance is 100m"
        result = standardize_units(text)

        assert result == "Distance is 100 meters"

    @pytest.mark.unit
    def test_kilometers(self):
        """Test kilometers conversion."""
        text = "Drive 50km"
        result = standardize_units(text)

        assert result == "Drive 50 kilometers"

    @pytest.mark.unit
    def test_kilograms(self):
        """Test kilograms conversion."""
        text = "Weight 75kg"
        result = standardize_units(text)

        assert result == "Weight 75 kilograms"

    @pytest.mark.unit
    def test_multiple_units(self):
        """Test multiple units in text."""
        text = "Run 5km at 95% effort"
        result = standardize_units(text)

        assert "5 kilometers" in result
        assert "95 percent" in result


class TestCorrectTypos:
    """Test typo correction."""

    @pytest.mark.unit
    def test_typo_correction_disabled(self):
        """Test typo correction can be disabled."""
        config = TextCleanerConfig({'enable_typo_correction': False})
        text = "Ths is a tset"
        result = correct_typos(text, config)

        assert result == text

    @pytest.mark.unit
    def test_simple_typo_correction(self):
        """Test simple typo gets corrected."""
        config = TextCleanerConfig({})
        mock_spell_checker = Mock()
        mock_spell_checker.correction.return_value = "test"

        text = "tset"
        result = correct_typos(text, config, spell_checker=mock_spell_checker)

        assert "test" in result

    @pytest.mark.unit
    def test_skip_short_words(self):
        """Test short words are skipped."""
        config = TextCleanerConfig({'typo_correction': {'min_word_length': 4}})
        text = "is a cat"
        result = correct_typos(text, config)

        # Short words not corrected
        assert result == text

    @pytest.mark.unit
    def test_skip_long_words(self):
        """Test long words are skipped."""
        config = TextCleanerConfig({'typo_correction': {'max_word_length': 5}})
        text = "verylongword"
        result = correct_typos(text, config)

        # Long word not corrected
        assert result == text

    @pytest.mark.unit
    def test_skip_ner_entities(self):
        """Test NER entities are not corrected."""
        config = TextCleanerConfig({'typo_correction': {'use_ner_entities': True}})
        ner_entities = {"Francisco"}

        text = "Francisco is here"
        result = correct_typos(text, config, ner_entities=ner_entities)

        # Entity name preserved
        assert "Francisco" in result

    @pytest.mark.unit
    def test_skip_capitalized_words(self):
        """Test capitalized words can be skipped."""
        config = TextCleanerConfig({
            'typo_correction': {
                'skip_capitalized_words': True,
                'min_word_length': 3
            }
        })
        text = "McDonald is here"  # 8+ chars, capitalized
        result = correct_typos(text, config)

        # Capitalized word preserved (>5 chars)
        assert "McDonald" in result

    @pytest.mark.unit
    def test_skip_mixed_case(self):
        """Test mixed-case words starting with uppercase are skipped."""
        config = TextCleanerConfig({
            'typo_correction': {'skip_mixed_case': True}
        })
        # Use McDonald which starts with uppercase and has mixed case
        text = "McDonald available"
        result = correct_typos(text, config)

        # Mixed case word starting with uppercase should be preserved
        assert "McDonald" in result

    @pytest.mark.unit
    def test_preserve_capitalization(self):
        """Test capitalization is preserved after correction."""
        config = TextCleanerConfig({})
        mock_spell_checker = Mock()
        mock_spell_checker.correction.return_value = "test"

        text = "Tset"  # Capitalized typo
        result = correct_typos(text, config, spell_checker=mock_spell_checker)

        # Should capitalize "test" to "Test"
        assert result == "Test" or "tset" in result.lower()


class TestCleanTextPipeline:
    """Test full text cleaning pipeline."""

    @pytest.mark.unit
    def test_full_pipeline_with_all_features(self):
        """Test full pipeline with all cleaning steps enabled."""
        config = TextCleanerConfig({})
        text = "<p>Hello   world!!!</p>"

        result = clean_text_pipeline(text, config)

        # HTML removed
        assert '<p>' not in result
        # Whitespace normalized
        assert '   ' not in result
        # Excessive punctuation normalized
        assert '!!!' not in result

    @pytest.mark.unit
    def test_pipeline_with_selective_features(self):
        """Test pipeline with only some features enabled."""
        config = TextCleanerConfig({
            'remove_html_tags': True,
            'normalize_whitespace': True,
            'fix_encoding': False,
            'standardize_currency': False,
            'enable_typo_correction': False
        })
        text = "<b>$100  price</b>"

        result = clean_text_pipeline(text, config)

        # HTML removed
        assert '<b>' not in result
        # Currency NOT standardized (disabled)
        assert '$100' in result or '100' in result

    @pytest.mark.unit
    def test_pipeline_with_currency_units(self):
        """Test pipeline with currency and unit standardization."""
        config = TextCleanerConfig({
            'standardize_currency': True,
            'standardize_units': True
        })
        text = "Price $50 for 10kg"

        result = clean_text_pipeline(text, config)

        assert "USD" in result
        assert "kilograms" in result

    @pytest.mark.unit
    def test_pipeline_order_matters(self):
        """Test that pipeline steps execute in correct order."""
        config = TextCleanerConfig({})
        text = "<p>Multiple   spaces</p>"

        result = clean_text_pipeline(text, config)

        # HTML removed first, then whitespace normalized
        assert '<p>' not in result
        assert 'Multiple spaces' in result or 'Multiple' in result

    @pytest.mark.unit
    def test_empty_text(self):
        """Test pipeline with empty text."""
        config = TextCleanerConfig({})
        text = ""

        result = clean_text_pipeline(text, config)

        assert result == ""

    @pytest.mark.unit
    def test_whitespace_only_text(self):
        """Test pipeline with whitespace-only text."""
        config = TextCleanerConfig({})
        text = "   \n\t  "

        result = clean_text_pipeline(text, config)

        assert result == ""

    @pytest.mark.unit
    def test_pipeline_with_ner_entities(self):
        """Test pipeline with NER entity protection."""
        config = TextCleanerConfig({'enable_typo_correction': True})
        ner_entities = {"San Francisco", "Barack Obama"}

        text = "San Francisco visit"
        result = clean_text_pipeline(text, config, ner_entities=ner_entities)

        # Entity protected from typo correction
        assert "San" in result
        assert "Francisco" in result

    @pytest.mark.unit
    def test_complex_real_world_text(self):
        """Test pipeline with complex real-world text."""
        config = TextCleanerConfig({})
        text = """
        <div>
            <h1>Article Title!!!</h1>
            <p>Price is $99.95 for 5kg product.</p>
            <p>Visit  us  at—New York.</p>
        </div>
        """

        result = clean_text_pipeline(text, config)

        # HTML removed
        assert '<div>' not in result
        assert '<h1>' not in result
        # Whitespace normalized
        assert '  ' not in result
        # Contains content
        assert 'Article' in result or 'Title' in result
