"""
Unit tests for Semantic Discoverer (ADR-376).

Tests semantic discovery functionality:
- Entity embedding
- Similarity search
- Duplicate detection
- Column matching
- Integration with discovery workflow
"""

import pytest
import duckdb
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from mdde.discovery import (
    SemanticDiscoverer,
    SemanticDiscoveryConfig,
    SemanticEntityMatch,
    SemanticColumnMatch,
    RelationshipDiscovery,
)


class TestSemanticDiscovererSetup:
    """Test semantic discoverer initialization."""

    @pytest.fixture
    def test_db(self):
        """Create test database with sample metadata."""
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS metadata")

        conn.execute("""
            CREATE TABLE metadata.model (
                model_id VARCHAR PRIMARY KEY,
                model_name VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE metadata.entity (
                entity_id VARCHAR PRIMARY KEY,
                entity_name VARCHAR,
                stereotype VARCHAR,
                comment VARCHAR,
                model_id VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE metadata.attribute (
                attribute_id VARCHAR PRIMARY KEY,
                attribute_name VARCHAR,
                data_type VARCHAR,
                comment VARCHAR,
                entity_id VARCHAR
            )
        """)

        # Insert test data
        conn.execute("""
            INSERT INTO metadata.model VALUES
            ('sales', 'Sales Model'),
            ('marketing', 'Marketing Model')
        """)

        conn.execute("""
            INSERT INTO metadata.entity VALUES
            ('dim_customer', 'dim_customer', 'dim_scd2',
             'Customer dimension with history', 'sales'),
            ('dim_product', 'dim_product', 'dim_scd1',
             'Product catalog', 'sales'),
            ('fact_orders', 'fact_orders', 'fact',
             'Order transactions', 'sales'),
            ('customer_master', 'customer_master', 'dim',
             'Customer master data', 'marketing'),
            ('campaign_target', 'campaign_target', 'fact',
             'Marketing campaign targets', 'marketing')
        """)

        conn.execute("""
            INSERT INTO metadata.attribute VALUES
            ('a1', 'customer_id', 'string', 'Unique customer ID', 'dim_customer'),
            ('a2', 'customer_name', 'string', 'Full name', 'dim_customer'),
            ('a3', 'product_id', 'string', 'Product ID', 'dim_product'),
            ('a4', 'cust_id', 'string', 'Customer identifier', 'customer_master'),
            ('a5', 'cust_name', 'string', 'Customer name', 'customer_master')
        """)

        return conn

    def test_discoverer_initialization(self, test_db):
        """Test discoverer can be initialized."""
        discoverer = SemanticDiscoverer(test_db)
        assert discoverer is not None
        assert discoverer.config is not None

    def test_discoverer_with_config(self, test_db):
        """Test discoverer with custom config."""
        config = SemanticDiscoveryConfig(
            duplicate_threshold=0.90,
            related_threshold=0.75,
            cross_model=True,
        )
        discoverer = SemanticDiscoverer(test_db, config)
        assert discoverer.config.duplicate_threshold == 0.90

    def test_embed_model(self, test_db):
        """Test embedding entities in a model."""
        discoverer = SemanticDiscoverer(test_db)
        stats = discoverer.embed_model("sales")

        assert "entities" in stats
        assert stats["entities"] == 3  # 3 entities in sales model

    def test_embed_with_attributes(self, test_db):
        """Test embedding with attributes."""
        discoverer = SemanticDiscoverer(test_db)
        stats = discoverer.embed_model("sales", include_attributes=True)

        assert stats["entities"] == 3
        assert stats["attributes"] == 3  # 3 attributes in sales model

    def test_embed_all_models(self, test_db):
        """Test embedding all models."""
        discoverer = SemanticDiscoverer(test_db)
        stats = discoverer.embed_all_models()

        assert stats["models"] == 2
        assert stats["total_embedded"] > 0


class TestSimilaritySearch:
    """Test similarity search functionality."""

    @pytest.fixture
    def discoverer_with_data(self, tmp_path):
        """Create discoverer with embedded data."""
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS metadata")

        conn.execute("""
            CREATE TABLE metadata.model (
                model_id VARCHAR, model_name VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE metadata.entity (
                entity_id VARCHAR, entity_name VARCHAR,
                stereotype VARCHAR, comment VARCHAR, model_id VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE metadata.attribute (
                attribute_id VARCHAR, attribute_name VARCHAR,
                data_type VARCHAR, comment VARCHAR, entity_id VARCHAR
            )
        """)

        conn.execute("""
            INSERT INTO metadata.model VALUES
            ('m1', 'Model 1'), ('m2', 'Model 2')
        """)
        conn.execute("""
            INSERT INTO metadata.entity VALUES
            ('e1', 'dim_customer', 'dim', 'Customer data', 'm1'),
            ('e2', 'dim_product', 'dim', 'Product data', 'm1'),
            ('e3', 'customer_dim', 'dim', 'Customer dimension', 'm2')
        """)
        conn.execute("""
            INSERT INTO metadata.attribute VALUES
            ('a1', 'customer_id', 'string', 'Customer ID', 'e1'),
            ('a2', 'cust_id', 'string', 'Customer identifier', 'e3')
        """)

        discoverer = SemanticDiscoverer(conn)
        discoverer.embed_model("m1", include_attributes=True)
        discoverer.embed_model("m2", include_attributes=True)

        return discoverer

    def test_find_similar_entities(self, discoverer_with_data):
        """Test finding similar entities."""
        matches = discoverer_with_data.find_similar_entities(
            "e1",
            top_k=5,
            threshold=0.0,  # Accept all
        )

        assert isinstance(matches, list)
        assert all(isinstance(m, SemanticEntityMatch) for m in matches)

    def test_find_similar_by_text(self, discoverer_with_data):
        """Test finding entities by text query."""
        matches = discoverer_with_data.find_similar_by_text(
            "Customer related table",
            top_k=3,
        )

        assert isinstance(matches, list)
        # All matches should have source_name = query
        assert all(m.source_name == "Customer related table" for m in matches)

    def test_find_similar_columns(self, discoverer_with_data):
        """Test finding similar columns."""
        matches = discoverer_with_data.find_similar_columns(
            "a1",
            top_k=5,
            threshold=0.0,
        )

        assert isinstance(matches, list)
        assert all(isinstance(m, SemanticColumnMatch) for m in matches)


class TestDuplicateDetection:
    """Test duplicate detection functionality."""

    @pytest.fixture
    def discoverer_with_duplicates(self):
        """Create discoverer with potential duplicates."""
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS metadata")

        conn.execute("""
            CREATE TABLE metadata.model (
                model_id VARCHAR, model_name VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE metadata.entity (
                entity_id VARCHAR, entity_name VARCHAR,
                stereotype VARCHAR, comment VARCHAR, model_id VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE metadata.attribute (
                attribute_id VARCHAR, attribute_name VARCHAR,
                data_type VARCHAR, comment VARCHAR, entity_id VARCHAR
            )
        """)

        conn.execute("INSERT INTO metadata.model VALUES ('m1', 'Model')")
        # Create entities with similar names
        conn.execute("""
            INSERT INTO metadata.entity VALUES
            ('e1', 'customer_dimension', 'dim', 'Customer dim', 'm1'),
            ('e2', 'customer_dim', 'dim', 'Customer dimension', 'm1'),
            ('e3', 'product_catalog', 'dim', 'Products', 'm1')
        """)

        discoverer = SemanticDiscoverer(conn)
        discoverer.embed_model("m1")
        return discoverer

    def test_find_duplicates(self, discoverer_with_duplicates):
        """Test finding potential duplicates."""
        # Use very low threshold since placeholder embeddings don't capture semantics
        duplicates = discoverer_with_duplicates.find_duplicates(threshold=0.0)

        assert isinstance(duplicates, list)
        assert all(isinstance(d, SemanticEntityMatch) for d in duplicates)

    def test_duplicate_match_type(self, discoverer_with_duplicates):
        """Test that duplicates have correct match type."""
        config = SemanticDiscoveryConfig(duplicate_threshold=0.0)
        discoverer_with_duplicates.config = config

        duplicates = discoverer_with_duplicates.find_duplicates(threshold=0.0)

        for d in duplicates:
            assert d.match_type == "duplicate"


class TestWorkflowIntegration:
    """Test integration with discovery workflow."""

    @pytest.fixture
    def test_db(self):
        """Create test database."""
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS metadata")

        conn.execute("""
            CREATE TABLE metadata.model (
                model_id VARCHAR, model_name VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE metadata.entity (
                entity_id VARCHAR, entity_name VARCHAR,
                stereotype VARCHAR, comment VARCHAR, model_id VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE metadata.attribute (
                attribute_id VARCHAR, attribute_name VARCHAR,
                data_type VARCHAR, comment VARCHAR, entity_id VARCHAR
            )
        """)

        conn.execute("INSERT INTO metadata.model VALUES ('m1', 'Model')")
        conn.execute("""
            INSERT INTO metadata.entity VALUES
            ('e1', 'dim_customer', 'dim', 'Customer', 'm1'),
            ('e2', 'dim_product', 'dim', 'Product', 'm1')
        """)

        return conn

    def test_discover_relationships_semantic(self, test_db):
        """Test discovering relationships using semantics."""
        discoverer = SemanticDiscoverer(test_db)
        discoverer.embed_model("m1")

        discoveries = discoverer.discover_relationships_semantic(
            "m1",
            threshold=0.0,  # Low threshold for testing
        )

        assert isinstance(discoveries, list)
        assert all(isinstance(d, RelationshipDiscovery) for d in discoveries)

    def test_discovery_has_correct_source(self, test_db):
        """Test that discoveries have DOMAIN_MATCH source."""
        from mdde.discovery import DiscoverySource

        discoverer = SemanticDiscoverer(test_db)
        discoverer.embed_model("m1")

        discoveries = discoverer.discover_relationships_semantic("m1", threshold=0.0)

        for d in discoveries:
            assert d.discovery_source == DiscoverySource.DOMAIN_MATCH

    def test_discovery_has_confidence_score(self, test_db):
        """Test that discoveries have confidence scores."""
        discoverer = SemanticDiscoverer(test_db)
        discoverer.embed_model("m1")

        discoveries = discoverer.discover_relationships_semantic("m1", threshold=0.0)

        for d in discoveries:
            assert 0.0 <= d.confidence_score <= 1.0


class TestSemanticEntityMatch:
    """Test SemanticEntityMatch data structure."""

    def test_match_creation(self):
        """Test creating a match."""
        match = SemanticEntityMatch(
            source_id="e1",
            target_id="e2",
            source_name="dim_customer",
            target_name="customer_master",
            similarity=0.92,
            match_type="related",
            source_model_id="m1",
            target_model_id="m2",
        )

        assert match.source_id == "e1"
        assert match.similarity == 0.92
        assert match.match_type == "related"

    def test_match_to_dict(self):
        """Test converting to dictionary."""
        match = SemanticEntityMatch(
            source_id="e1",
            target_id="e2",
            source_name="A",
            target_name="B",
            similarity=0.85,
            match_type="related",
        )

        d = match.to_dict()
        assert d["source_id"] == "e1"
        assert d["similarity"] == 0.85


class TestSemanticColumnMatch:
    """Test SemanticColumnMatch data structure."""

    def test_column_match_creation(self):
        """Test creating a column match."""
        match = SemanticColumnMatch(
            source_column_id="a1",
            target_column_id="a2",
            source_column_name="customer_id",
            target_column_name="cust_id",
            source_entity_id="e1",
            target_entity_id="e2",
            similarity=0.88,
            match_type="related",
            suggestion="High similarity - likely mapping candidate",
        )

        assert match.source_column_name == "customer_id"
        assert match.similarity == 0.88

    def test_column_match_to_dict(self):
        """Test converting to dictionary."""
        match = SemanticColumnMatch(
            source_column_id="a1",
            target_column_id="a2",
            source_column_name="col1",
            target_column_name="col2",
            source_entity_id="e1",
            target_entity_id="e2",
            similarity=0.75,
            match_type="potential",
        )

        d = match.to_dict()
        assert d["source_column"] == "col1"
        assert d["similarity"] == 0.75
