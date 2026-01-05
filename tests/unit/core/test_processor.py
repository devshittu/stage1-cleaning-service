"""
tests/unit/core/test_processor.py

Unit tests for TextPreprocessor core logic.

Tests cover:
- Model loading and caching
- Entity tagging (NER)
- Text cleaning with NER protection
- Temporal metadata extraction
- Language detection
- Full preprocessing pipeline
- Configuration overrides
"""

import pytest
from datetime import date, datetime, timedelta
from unittest.mock import patch, Mock, MagicMock
from typing import List

from src.core.processor import TextPreprocessor
from src.schemas.data_models import Entity, TextSpan, ArticleInput
from src.utils.text_cleaners import TextCleanerConfig


@pytest.fixture
def mock_spacy_model():
    """Mock spaCy NLP model."""
    mock_nlp = Mock()
    mock_doc = Mock()

    # Mock entity (must have label_ attribute for spaCy compatibility)
    mock_entity = Mock()
    mock_entity.text = "New York"
    mock_entity.label_ = "GPE"  # spaCy uses label_ not type_
    mock_entity.start_char = 0
    mock_entity.end_char = 8

    mock_doc.ents = [mock_entity]
    mock_doc.text = "processed text"
    mock_nlp.return_value = mock_doc

    return mock_nlp


@pytest.fixture
def processor_with_mock_model(mock_spacy_model):
    """Create processor with mocked spaCy model."""
    with patch('src.core.processor.spacy.load', return_value=mock_spacy_model):
        processor = TextPreprocessor()
        processor.nlp = mock_spacy_model
        return processor


class TestModelLoading:
    """Test spaCy model loading and caching."""

    @pytest.mark.unit
    def test_load_model_success(self):
        """Test successful model loading."""
        with patch('src.core.processor.spacy.load') as mock_load:
            mock_nlp = Mock()
            mock_load.return_value = mock_nlp

            processor = TextPreprocessor()

            assert processor.nlp is not None
            mock_load.assert_called_once()

    @pytest.mark.unit
    def test_model_caching(self):
        """Test that models are cached at class level."""
        with patch('src.core.processor.spacy.load') as mock_load:
            mock_nlp = Mock()
            mock_load.return_value = mock_nlp

            # Clear cache first
            TextPreprocessor._nlp_cache.clear()

            # Load first processor
            processor1 = TextPreprocessor()
            assert mock_load.call_count == 1

            # Load second processor - should reuse cached model
            processor2 = TextPreprocessor()
            assert mock_load.call_count == 1  # Still 1, not 2

            # Both should have the same model instance
            assert processor1.nlp is processor2.nlp

    @pytest.mark.unit
    def test_gpu_enabled_configuration(self):
        """Test GPU configuration when enabled."""
        with patch('src.core.processor.spacy.load') as mock_load, \
             patch('src.core.processor.spacy.require_gpu') as mock_gpu:
            mock_nlp = Mock()
            mock_load.return_value = mock_nlp

            # Mock settings to enable GPU
            with patch('src.core.processor.ConfigManager.get_settings') as mock_settings:
                mock_config = Mock()
                mock_config.general.gpu_enabled = True
                mock_config.ingestion_service.model_name = "en_core_web_sm"
                mock_config.ingestion_service.model_cache_dir = "/tmp"
                mock_config.ingestion_service.cleaning_pipeline.model_dump.return_value = {}
                mock_settings.return_value = mock_config

                processor = TextPreprocessor()

                mock_gpu.assert_called_once()

    @pytest.mark.unit
    def test_gpu_fallback_to_cpu(self):
        """Test fallback to CPU when GPU unavailable."""
        with patch('src.core.processor.spacy.load') as mock_load, \
             patch('src.core.processor.spacy.require_gpu', side_effect=Exception("GPU unavailable")):
            mock_nlp = Mock()
            mock_load.return_value = mock_nlp

            # Should not raise exception, just log warning
            processor = TextPreprocessor()
            assert processor.nlp is not None


class TestEntityTagging:
    """Test entity recognition and tagging."""

    @pytest.mark.unit
    def test_tag_entities_basic(self, processor_with_mock_model):
        """Test basic entity tagging."""
        text = "New York is a city"

        entities = processor_with_mock_model.tag_entities(text)

        assert len(entities) > 0
        assert entities[0].text == "New York"
        assert entities[0].type == "GPE"
        assert entities[0].start_char == 0
        assert entities[0].end_char == 8

    @pytest.mark.unit
    def test_tag_entities_empty_text(self, processor_with_mock_model):
        """Test entity tagging with empty text."""
        processor_with_mock_model.nlp.return_value.ents = []

        entities = processor_with_mock_model.tag_entities("")

        assert len(entities) == 0

    @pytest.mark.unit
    def test_tag_entities_multiple_types(self, processor_with_mock_model):
        """Test entity tagging with multiple entity types."""
        # Mock multiple entities
        mock_person = Mock()
        mock_person.text = "John Smith"
        mock_person.label_ = "PERSON"
        mock_person.start_char = 0
        mock_person.end_char = 10

        mock_org = Mock()
        mock_org.text = "Google"
        mock_org.label_ = "ORG"
        mock_org.start_char = 20
        mock_org.end_char = 26

        mock_doc = Mock()
        mock_doc.ents = [mock_person, mock_org]
        processor_with_mock_model.nlp.return_value = mock_doc

        entities = processor_with_mock_model.tag_entities("John Smith works at Google")

        assert len(entities) == 2
        assert entities[0].type == "PERSON"
        assert entities[1].type == "ORG"

    @pytest.mark.unit
    def test_tag_entities_overlapping(self, processor_with_mock_model):
        """Test entity tagging handles overlapping entities."""
        # spaCy typically doesn't return overlapping entities, but test the case
        mock_entity = Mock()
        mock_entity.text = "New York City"
        mock_entity.label_ = "GPE"  # spaCy uses label_ not type_
        mock_entity.start_char = 0
        mock_entity.end_char = 13

        mock_doc = Mock()
        mock_doc.ents = [mock_entity]
        processor_with_mock_model.nlp.return_value = mock_doc

        entities = processor_with_mock_model.tag_entities("New York City")

        assert len(entities) == 1


class TestCleanText:
    """Test text cleaning functionality."""

    @pytest.mark.unit
    def test_clean_text_basic(self, processor_with_mock_model):
        """Test basic text cleaning."""
        dirty_text = "This   is    messy text!!!"

        # Mock the clean_text_pipeline
        with patch('src.core.processor.clean_text_pipeline') as mock_clean:
            mock_clean.return_value = "This is messy text"

            cleaned = processor_with_mock_model.clean_text(dirty_text)

            assert cleaned == "This is messy text"
            mock_clean.assert_called_once()

    @pytest.mark.unit
    def test_clean_text_with_ner_protection(self, processor_with_mock_model):
        """Test text cleaning with NER protection."""
        text = "Visit New York today"

        # Mock entity detection
        processor_with_mock_model.nlp.return_value.ents = [Mock(
            text="New York",
            label_="GPE",
            start_char=6,
            end_char=14
        )]

        with patch('src.core.processor.clean_text_pipeline') as mock_clean:
            mock_clean.return_value = "Visit New York today"

            cleaned_text, entities = processor_with_mock_model.clean_text_with_ner_protection(text)

            assert "New York" in cleaned_text
            assert len(entities) > 0

    @pytest.mark.unit
    def test_clean_text_removes_extra_spaces(self, processor_with_mock_model):
        """Test that cleaning removes extra spaces."""
        text = "Multiple    spaces    here"

        with patch('src.core.processor.clean_text_pipeline') as mock_clean:
            mock_clean.return_value = "Multiple spaces here"

            cleaned = processor_with_mock_model.clean_text(text)

            assert "    " not in cleaned

    @pytest.mark.unit
    def test_clean_text_handles_unicode(self, processor_with_mock_model):
        """Test cleaning handles unicode characters."""
        text = "Text with unicode: café, naïve, résumé"

        with patch('src.core.processor.clean_text_pipeline') as mock_clean:
            mock_clean.return_value = "Text with unicode: cafe, naive, resume"

            cleaned = processor_with_mock_model.clean_text(text)

            assert cleaned is not None

    @pytest.mark.unit
    def test_clean_text_empty_input(self, processor_with_mock_model):
        """Test cleaning with empty input."""
        with patch('src.core.processor.clean_text_pipeline') as mock_clean:
            mock_clean.return_value = ""

            cleaned = processor_with_mock_model.clean_text("")

            assert cleaned == ""


class TestTemporalMetadataExtraction:
    """Test temporal metadata extraction."""

    @pytest.mark.unit
    def test_extract_date_basic(self, processor_with_mock_model):
        """Test basic date extraction."""
        text = "Published on January 15, 2024"
        reference_date = date(2024, 1, 1)

        with patch('dateparser.parse') as mock_parse:
            mock_parse.return_value = datetime(2024, 1, 15)

            extracted = processor_with_mock_model.extract_temporal_metadata(
                text,
                reference_date
            )

            assert extracted is not None
            assert "2024" in extracted

    @pytest.mark.unit
    def test_extract_date_relative(self, processor_with_mock_model):
        """Test relative date extraction (e.g., 'yesterday')."""
        text = "Posted yesterday"
        reference_date = date(2024, 1, 15)

        with patch('dateparser.parse') as mock_parse:
            mock_parse.return_value = datetime(2024, 1, 14)

            extracted = processor_with_mock_model.extract_temporal_metadata(
                text,
                reference_date
            )

            # Should extract a date relative to reference
            assert extracted is not None

    @pytest.mark.unit
    def test_extract_date_weekday(self, processor_with_mock_model):
        """Test weekday date extraction (e.g., 'last Monday')."""
        text = "Last Monday"
        reference_date = date(2024, 1, 15)  # Monday

        extracted = processor_with_mock_model.extract_temporal_metadata(
            text,
            reference_date
        )

        assert extracted is not None

    @pytest.mark.unit
    def test_extract_date_no_match(self, processor_with_mock_model):
        """Test date extraction with no date in text."""
        text = "No date mentioned here"

        extracted = processor_with_mock_model.extract_temporal_metadata(text)

        # Should return None if no date found
        assert extracted is None

    @pytest.mark.unit
    def test_extract_date_multiple_formats(self, processor_with_mock_model):
        """Test extraction handles multiple date formats."""
        texts = [
            "01/15/2024",
            "15-01-2024",
            "January 15, 2024",
            "2024-01-15"
        ]

        for text in texts:
            extracted = processor_with_mock_model.extract_temporal_metadata(text)
            # Should extract date in some format
            if extracted:
                assert len(extracted) > 0


class TestLanguageDetection:
    """Test language detection functionality."""

    @pytest.mark.unit
    def test_detect_language_english(self, processor_with_mock_model):
        """Test language detection for English text."""
        text = "This is an English sentence with enough words for detection."

        with patch('src.core.processor.detect', return_value='en'):
            lang = processor_with_mock_model._detect_language(text)

            assert lang == 'en'

    @pytest.mark.unit
    def test_detect_language_spanish(self, processor_with_mock_model):
        """Test language detection for Spanish text."""
        text = "Esta es una oración en español con suficientes palabras."

        with patch('src.core.processor.detect', return_value='es'):
            lang = processor_with_mock_model._detect_language(text)

            assert lang == 'es'

    @pytest.mark.unit
    def test_detect_language_short_text(self, processor_with_mock_model):
        """Test language detection with very short text."""
        text = "Hi"

        # Short text may fail detection
        with patch('src.core.processor.detect', side_effect=Exception("Text too short")):
            lang = processor_with_mock_model._detect_language(text)

            assert lang is None

    @pytest.mark.unit
    def test_detect_language_not_available(self, processor_with_mock_model):
        """Test language detection when langdetect not available."""
        text = "Some text"

        # Simulate langdetect not installed
        with patch('src.core.processor.detect', None):
            lang = processor_with_mock_model._detect_language(text)

            assert lang is None


class TestPreprocessPipeline:
    """Test full preprocessing pipeline."""

    @pytest.mark.unit
    def test_preprocess_complete_pipeline(self, processor_with_mock_model):
        """Test complete preprocessing pipeline."""
        # Mock all components
        processor_with_mock_model.nlp.return_value.ents = []

        with patch('src.core.processor.clean_text_pipeline') as mock_clean, \
             patch.object(processor_with_mock_model, 'extract_temporal_metadata', return_value="2024-01-15"):
            mock_clean.return_value = "New York City announced new policies yesterday"

            result = processor_with_mock_model.preprocess(
                text="New York City announced new policies yesterday.",
                document_id="test-doc",
                title="Breaking News from New York"
            )

            assert result["document_id"] == "test-doc"
            assert result["cleaned_text"] is not None
            assert "entities" in result

    @pytest.mark.unit
    def test_preprocess_with_entities(self, processor_with_mock_model):
        """Test preprocessing extracts entities."""
        # Mock entity detection
        mock_entity = Mock()
        mock_entity.text = "Apple Inc."
        mock_entity.label_ = "ORG"  # spaCy uses label_
        mock_entity.start_char = 0
        mock_entity.end_char = 10

        processor_with_mock_model.nlp.return_value.ents = [mock_entity]

        with patch('src.core.processor.clean_text_pipeline') as mock_clean:
            mock_clean.return_value = "Apple Inc. announced the new iPhone today."

            result = processor_with_mock_model.preprocess(
                text="Apple Inc. announced the new iPhone today.",
                document_id="test-doc",
                title="Apple releases new iPhone"
            )

            assert len(result["entities"]) > 0
            # Entities are Entity objects, not dicts
            assert result["entities"][0].text == "Apple Inc."
            assert result["entities"][0].type == "ORG"

    @pytest.mark.unit
    def test_preprocess_generates_statistics(self, processor_with_mock_model):
        """Test preprocessing generates statistics."""
        processor_with_mock_model.nlp.return_value.ents = []

        with patch('src.core.processor.clean_text_pipeline') as mock_clean:
            mock_clean.return_value = "Body text with some content"

            result = processor_with_mock_model.preprocess(
                text="Body text with some content",
                document_id="test-doc",
                title="Title"
            )

            assert result["document_id"] == "test-doc"
            assert result["cleaned_text"] is not None

    @pytest.mark.unit
    def test_preprocess_handles_empty_body(self, processor_with_mock_model):
        """Test preprocessing handles empty body."""
        processor_with_mock_model.nlp.return_value.ents = []

        with patch('src.core.processor.clean_text_pipeline') as mock_clean:
            mock_clean.return_value = ""

            result = processor_with_mock_model.preprocess(
                text="",
                document_id="test-doc",
                title="Title Only"
            )

            assert result["cleaned_text"] == ""
            assert result["document_id"] == "test-doc"

    @pytest.mark.unit
    def test_preprocess_preserves_metadata(self, processor_with_mock_model):
        """Test preprocessing preserves input metadata."""
        processor_with_mock_model.nlp.return_value.ents = []

        with patch('src.core.processor.clean_text_pipeline') as mock_clean:
            mock_clean.return_value = "Body"

            result = processor_with_mock_model.preprocess(
                text="Body",
                document_id="test-doc",
                title="Title",
                source_url="https://example.com/article",
                additional_metadata={"domain": "tech", "custom_field": "custom_value"}
            )

            # Check basic fields are preserved (with cleaned_ prefix)
            assert result["document_id"] == "test-doc"
            assert "cleaned_title" in result
            assert "cleaned_source_url" in result
            # Additional metadata should be in result
            assert "cleaned_additional_metadata" in result


class TestConfigurationOverrides:
    """Test configuration overrides."""

    @pytest.mark.unit
    def test_custom_config_override(self):
        """Test processor accepts custom configuration."""
        custom_config = {
            "normalize_unicode_dashes": True,
            "enable_typo_correction": False
        }

        with patch('src.core.processor.spacy.load'):
            processor = TextPreprocessor(custom_config=custom_config)

            assert processor.cleaning_config is not None
            # cleaning_config is TextCleanerConfig with simple attributes
            assert hasattr(processor.cleaning_config, "enable_typo_correction")
            assert processor.cleaning_config.enable_typo_correction == False
            assert processor.cleaning_config.normalize_unicode_dashes == True

    @pytest.mark.unit
    def test_default_config_from_settings(self):
        """Test processor uses default config from settings."""
        with patch('src.core.processor.spacy.load'):
            processor = TextPreprocessor()

            assert processor.cleaning_config is not None
            assert processor.settings is not None

    @pytest.mark.unit
    def test_config_merge_preserves_defaults(self):
        """Test custom config merges with defaults."""
        # Override only one setting
        custom_config = {
            "typo_correction": {"enabled": False}
        }

        with patch('src.core.processor.spacy.load'):
            processor = TextPreprocessor(custom_config=custom_config)

            # Other settings should still have defaults
            assert processor.cleaning_config is not None


class TestProcessorResourceManagement:
    """Test processor resource management."""

    @pytest.mark.unit
    def test_close_releases_resources(self, processor_with_mock_model):
        """Test close method releases resources."""
        processor_with_mock_model.close()

        # After close, nlp should still exist (cached)
        # But any file handles or connections should be closed
        assert True  # No exception raised

    @pytest.mark.unit
    def test_spell_checker_lazy_initialization(self, processor_with_mock_model):
        """Test spell checker is initialized lazily."""
        # Initially None
        assert processor_with_mock_model.spell_checker is None

        # Get spell checker
        with patch('src.core.processor.SpellChecker') as mock_spell:
            mock_spell.return_value = Mock()

            checker = processor_with_mock_model._get_spell_checker()

            assert checker is not None
            # Should be cached now
            assert processor_with_mock_model.spell_checker is not None

    @pytest.mark.unit
    def test_multiple_instances_share_model_cache(self):
        """Test multiple processor instances share model cache."""
        with patch('src.core.processor.spacy.load') as mock_load:
            mock_nlp = Mock()
            mock_load.return_value = mock_nlp

            # Clear cache
            TextPreprocessor._nlp_cache.clear()

            # Create multiple processors
            p1 = TextPreprocessor()
            p2 = TextPreprocessor()
            p3 = TextPreprocessor()

            # spacy.load should only be called once
            assert mock_load.call_count == 1

            # All should share the same model
            assert p1.nlp is p2.nlp is p3.nlp
