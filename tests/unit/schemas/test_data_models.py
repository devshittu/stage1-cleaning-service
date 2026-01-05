"""
tests/unit/schemas/test_data_models.py

Unit tests for Pydantic data models.

Tests cover:
- Model validation
- Field constraints
- Optional fields
- Type coercion
- Error handling
"""

import pytest
from datetime import date
from pydantic import ValidationError

from src.schemas.data_models import (
    TextSpan,
    Entity,
    CleaningConfigOverride,
    ArticleInput,
    PreprocessSingleRequest
)


class TestTextSpan:
    """Test TextSpan model."""

    @pytest.mark.unit
    def test_valid_text_span(self):
        """Test valid text span creation."""
        span = TextSpan(text="Hello", start_char=0, end_char=5)

        assert span.text == "Hello"
        assert span.start_char == 0
        assert span.end_char == 5

    @pytest.mark.unit
    def test_missing_required_field_raises_error(self):
        """Test missing required field raises validation error."""
        with pytest.raises(ValidationError):
            TextSpan(text="Hello", start_char=0)  # Missing end_char


class TestEntity:
    """Test Entity model."""

    @pytest.mark.unit
    def test_valid_entity(self):
        """Test valid entity creation."""
        entity = Entity(
            text="John Doe",
            type="PERSON",
            start_char=0,
            end_char=8
        )

        assert entity.text == "John Doe"
        assert entity.type == "PERSON"
        assert entity.start_char == 0
        assert entity.end_char == 8

    @pytest.mark.unit
    def test_various_entity_types(self):
        """Test different entity types."""
        types = ["PERSON", "ORG", "GPE", "LOC", "DATE"]

        for entity_type in types:
            entity = Entity(text="Test", type=entity_type, start_char=0, end_char=4)
            assert entity.type == entity_type


class TestCleaningConfigOverride:
    """Test CleaningConfigOverride model."""

    @pytest.mark.unit
    def test_all_fields_optional(self):
        """Test all fields are optional."""
        config = CleaningConfigOverride()
        assert config.remove_html_tags is None
        assert config.normalize_whitespace is None

    @pytest.mark.unit
    def test_partial_override(self):
        """Test partial configuration override."""
        config = CleaningConfigOverride(
            remove_html_tags=True,
            normalize_whitespace=False
        )

        assert config.remove_html_tags is True
        assert config.normalize_whitespace is False
        assert config.fix_encoding is None

    @pytest.mark.unit
    def test_typo_correction_settings(self):
        """Test typo correction sub-settings."""
        config = CleaningConfigOverride(
            enable_typo_correction=True,
            typo_min_length=3,
            typo_max_length=20,
            typo_confidence=0.8
        )

        assert config.enable_typo_correction is True
        assert config.typo_min_length == 3
        assert config.typo_max_length == 20
        assert config.typo_confidence == 0.8


class TestArticleInput:
    """Test ArticleInput model."""

    @pytest.mark.unit
    def test_minimal_valid_article(self):
        """Test minimal valid article with required fields only."""
        article = ArticleInput(document_id="doc1", text="Article content")

        assert article.document_id == "doc1"
        assert article.text == "Article content"
        assert article.title is None

    @pytest.mark.unit
    def test_full_article_with_all_fields(self):
        """Test article with all optional fields."""
        article = ArticleInput(
            document_id="doc1",
            text="Full article content",
            title="Test Title",
            excerpt="Test excerpt",
            author="John Doe",
            publication_date=date(2024, 1, 15),
            source_url="https://example.com",
            categories=["news", "tech"],
            tags=["ai", "ml"],
            word_count=500,
            publisher="Test Publisher"
        )

        assert article.document_id == "doc1"
        assert article.title == "Test Title"
        assert article.publication_date == date(2024, 1, 15)
        assert len(article.categories) == 2
        assert len(article.tags) == 2

    @pytest.mark.unit
    def test_invalid_url_raises_error(self):
        """Test invalid URL raises validation error."""
        with pytest.raises(ValidationError):
            ArticleInput(
                document_id="doc1",
                text="Content",
                source_url="not-a-valid-url"
            )

    @pytest.mark.unit
    def test_date_parsing(self):
        """Test date fields accept various formats."""
        article = ArticleInput(
            document_id="doc1",
            text="Content",
            publication_date="2024-01-15"  # String date
        )

        assert article.publication_date == date(2024, 1, 15)

    @pytest.mark.unit
    def test_empty_text_allowed(self):
        """Test empty text is technically allowed (validated elsewhere)."""
        article = ArticleInput(document_id="doc1", text="")
        assert article.text == ""


class TestPreprocessSingleRequest:
    """Test PreprocessSingleRequest model."""

    @pytest.mark.unit
    def test_minimal_request(self):
        """Test minimal valid request."""
        article = ArticleInput(document_id="doc1", text="Content")
        request = PreprocessSingleRequest(article=article)

        assert request.article.document_id == "doc1"
        assert request.persist_to_backends is None
        assert request.cleaning_config is None

    @pytest.mark.unit
    def test_with_persist_backends(self):
        """Test request with storage backends."""
        article = ArticleInput(document_id="doc1", text="Content")
        request = PreprocessSingleRequest(
            article=article,
            persist_to_backends=["jsonl", "postgresql"]
        )

        assert "jsonl" in request.persist_to_backends
        assert "postgresql" in request.persist_to_backends

    @pytest.mark.unit
    def test_with_cleaning_config_override(self):
        """Test request with cleaning configuration override."""
        article = ArticleInput(document_id="doc1", text="Content")
        config = CleaningConfigOverride(remove_html_tags=False)
        request = PreprocessSingleRequest(
            article=article,
            cleaning_config=config
        )

        assert request.cleaning_config.remove_html_tags is False

    @pytest.mark.unit
    def test_nested_validation(self):
        """Test nested model validation works."""
        with pytest.raises(ValidationError):
            # Missing required article field
            PreprocessSingleRequest(article={"document_id": "doc1"})


class TestModelSerialization:
    """Test model serialization and deserialization."""

    @pytest.mark.unit
    def test_article_to_dict(self):
        """Test model can be converted to dict."""
        article = ArticleInput(
            document_id="doc1",
            text="Content",
            title="Title"
        )

        data = article.model_dump()

        assert data["document_id"] == "doc1"
        assert data["text"] == "Content"
        assert data["title"] == "Title"

    @pytest.mark.unit
    def test_from_dict(self):
        """Test model can be created from dict."""
        data = {
            "document_id": "doc1",
            "text": "Content",
            "title": "Title"
        }

        article = ArticleInput(**data)

        assert article.document_id == "doc1"
        assert article.text == "Content"

    @pytest.mark.unit
    def test_json_serialization(self):
        """Test model can be serialized to JSON."""
        article = ArticleInput(
            document_id="doc1",
            text="Content",
            publication_date=date(2024, 1, 15)
        )

        json_str = article.model_dump_json()

        assert "doc1" in json_str
        assert "2024-01-15" in json_str
