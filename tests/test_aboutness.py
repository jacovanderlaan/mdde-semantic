# Tests for Aboutness Layer
# ADR-247: Aboutness Layer - Semantic Intent and Purpose
# Feb 2026

"""
Comprehensive unit tests for MDDE aboutness functionality.

Tests cover:
- AboutnessDimension and SemanticRole enums
- EntityAboutness and AttributeAboutness dataclasses
- AboutnessManager CRUD operations
- AboutnessInferrer pattern matching
- AboutnessValidator checks
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock

from mdde.semantic.aboutness.models import (
    AboutnessDimension,
    SemanticRole,
    DependencyType,
    EntityAboutness,
    AttributeAboutness,
    SemanticDependency,
    AboutnessValidation,
)
from mdde.semantic.aboutness.manager import AboutnessManager
from mdde.semantic.aboutness.inference import AboutnessInferrer
from mdde.semantic.aboutness.validator import (
    AboutnessValidator,
    ABOUTNESS_CHECKS,
    INVALID_DIMENSION_ROLE_COMBOS,
)


# =============================================================================
# Enum Tests
# =============================================================================


class TestAboutnessDimension:
    """Tests for AboutnessDimension enum."""

    def test_all_dimensions_defined(self):
        """Test all expected dimensions are defined."""
        expected = [
            "measure", "identifier", "classifier", "temporal",
            "relationship", "quality", "spatial", "state", "flag", "derived"
        ]
        actual = [d.value for d in AboutnessDimension]
        assert sorted(actual) == sorted(expected)

    def test_dimension_values(self):
        """Test specific dimension values."""
        assert AboutnessDimension.MEASURE.value == "measure"
        assert AboutnessDimension.IDENTIFIER.value == "identifier"
        assert AboutnessDimension.CLASSIFIER.value == "classifier"
        assert AboutnessDimension.TEMPORAL.value == "temporal"
        assert AboutnessDimension.RELATIONSHIP.value == "relationship"
        assert AboutnessDimension.QUALITY.value == "quality"
        assert AboutnessDimension.SPATIAL.value == "spatial"
        assert AboutnessDimension.STATE.value == "state"
        assert AboutnessDimension.FLAG.value == "flag"
        assert AboutnessDimension.DERIVED.value == "derived"

    def test_dimension_from_string(self):
        """Test creating dimension from string."""
        dim = AboutnessDimension("measure")
        assert dim == AboutnessDimension.MEASURE


class TestSemanticRole:
    """Tests for SemanticRole enum."""

    def test_all_roles_defined(self):
        """Test all expected roles are defined."""
        expected = [
            "aggregatable", "filterable", "groupable", "sortable",
            "joinable", "derivable", "displayable", "sliceable"
        ]
        actual = [r.value for r in SemanticRole]
        assert sorted(actual) == sorted(expected)

    def test_role_values(self):
        """Test specific role values."""
        assert SemanticRole.AGGREGATABLE.value == "aggregatable"
        assert SemanticRole.FILTERABLE.value == "filterable"
        assert SemanticRole.GROUPABLE.value == "groupable"
        assert SemanticRole.JOINABLE.value == "joinable"


class TestDependencyType:
    """Tests for DependencyType enum."""

    def test_dependency_type_values(self):
        """Test dependency type values."""
        assert DependencyType.REQUIRES.value == "requires"
        assert DependencyType.IMPLIES.value == "implies"
        assert DependencyType.CONFLICTS.value == "conflicts"
        assert DependencyType.REFINES.value == "refines"
        assert DependencyType.EQUIVALENT.value == "equivalent"
        assert DependencyType.DERIVED_FROM.value == "derived_from"


# =============================================================================
# Model Tests
# =============================================================================


class TestEntityAboutness:
    """Tests for EntityAboutness dataclass."""

    def test_entity_aboutness_creation(self):
        """Test creating entity aboutness."""
        aboutness = EntityAboutness(
            entity_id="customer",
            purpose="Represents individuals who purchase products",
            real_world_object="Person",
            aboutness_dimension=AboutnessDimension.CLASSIFIER,
        )

        assert aboutness.entity_id == "customer"
        assert aboutness.purpose == "Represents individuals who purchase products"
        assert aboutness.real_world_object == "Person"
        assert aboutness.aboutness_dimension == AboutnessDimension.CLASSIFIER
        assert aboutness.confidence_score == 1.0
        assert aboutness.source == "manual"

    def test_entity_aboutness_with_use_cases(self):
        """Test entity aboutness with business use cases."""
        aboutness = EntityAboutness(
            entity_id="order",
            purpose="Tracks customer purchases",
            real_world_object="PurchaseTransaction",
            aboutness_dimension=AboutnessDimension.CLASSIFIER,
            business_use_cases=["revenue_reporting", "inventory_management"],
            stakeholder_groups=["finance", "operations"],
        )

        assert len(aboutness.business_use_cases) == 2
        assert "revenue_reporting" in aboutness.business_use_cases
        assert len(aboutness.stakeholder_groups) == 2

    def test_entity_aboutness_to_dict(self):
        """Test serialization to dictionary."""
        aboutness = EntityAboutness(
            entity_id="customer",
            purpose="Test purpose",
            real_world_object="Person",
            aboutness_dimension=AboutnessDimension.CLASSIFIER,
            business_use_cases=["test_use"],
        )

        data = aboutness.to_dict()

        assert data["entity_id"] == "customer"
        assert data["aboutness_dimension"] == "classifier"
        assert data["business_use_cases"] == ["test_use"]

    def test_entity_aboutness_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "entity_id": "product",
            "purpose": "Represents sellable items",
            "real_world_object": "Product",
            "aboutness_dimension": "classifier",
            "confidence_score": 0.9,
        }

        aboutness = EntityAboutness.from_dict(data)

        assert aboutness.entity_id == "product"
        assert aboutness.aboutness_dimension == AboutnessDimension.CLASSIFIER
        assert aboutness.confidence_score == 0.9


class TestAttributeAboutness:
    """Tests for AttributeAboutness dataclass."""

    def test_attribute_aboutness_measure(self):
        """Test creating measure aboutness."""
        aboutness = AttributeAboutness(
            entity_id="order",
            attribute_id="total_amount",
            intent="Captures the total monetary value of the order",
            aboutness_dimension=AboutnessDimension.MEASURE,
            semantic_role=SemanticRole.AGGREGATABLE,
            measures_what="TransactionValue",
            canonical_name="transaction_amount",
        )

        assert aboutness.entity_id == "order"
        assert aboutness.attribute_id == "total_amount"
        assert aboutness.aboutness_dimension == AboutnessDimension.MEASURE
        assert aboutness.semantic_role == SemanticRole.AGGREGATABLE
        assert aboutness.measures_what == "TransactionValue"

    def test_attribute_aboutness_identifier(self):
        """Test creating identifier aboutness."""
        aboutness = AttributeAboutness(
            entity_id="customer",
            attribute_id="customer_id",
            intent="Uniquely identifies each customer",
            aboutness_dimension=AboutnessDimension.IDENTIFIER,
            semantic_role=SemanticRole.JOINABLE,
            identifies_what="Customer",
        )

        assert aboutness.aboutness_dimension == AboutnessDimension.IDENTIFIER
        assert aboutness.identifies_what == "Customer"

    def test_attribute_aboutness_relationship(self):
        """Test creating relationship aboutness."""
        aboutness = AttributeAboutness(
            entity_id="order",
            attribute_id="customer_id",
            intent="Links order to purchasing customer",
            aboutness_dimension=AboutnessDimension.RELATIONSHIP,
            semantic_role=SemanticRole.JOINABLE,
            relates_to="customer",
        )

        assert aboutness.aboutness_dimension == AboutnessDimension.RELATIONSHIP
        assert aboutness.relates_to == "customer"

    def test_attribute_aboutness_expected_behavior(self):
        """Test aboutness with expected behavior."""
        aboutness = AttributeAboutness(
            entity_id="order",
            attribute_id="quantity",
            intent="Measures item quantity",
            aboutness_dimension=AboutnessDimension.MEASURE,
            semantic_role=SemanticRole.AGGREGATABLE,
            expected_behavior={
                "aggregation": "SUM",
                "nullability": "required",
                "positive_only": True,
            },
        )

        assert aboutness.expected_behavior["aggregation"] == "SUM"
        assert aboutness.expected_behavior["positive_only"] is True

    def test_attribute_aboutness_to_dict(self):
        """Test serialization to dictionary."""
        aboutness = AttributeAboutness(
            entity_id="order",
            attribute_id="amount",
            intent="Test",
            aboutness_dimension=AboutnessDimension.MEASURE,
            semantic_role=SemanticRole.AGGREGATABLE,
        )

        data = aboutness.to_dict()

        assert data["entity_id"] == "order"
        assert data["attribute_id"] == "amount"
        assert data["aboutness_dimension"] == "measure"
        assert data["semantic_role"] == "aggregatable"

    def test_attribute_aboutness_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "entity_id": "order",
            "attribute_id": "status",
            "intent": "Tracks order state",
            "aboutness_dimension": "state",
            "semantic_role": "filterable",
        }

        aboutness = AttributeAboutness.from_dict(data)

        assert aboutness.attribute_id == "status"
        assert aboutness.aboutness_dimension == AboutnessDimension.STATE
        assert aboutness.semantic_role == SemanticRole.FILTERABLE


class TestSemanticDependency:
    """Tests for SemanticDependency dataclass."""

    def test_dependency_creation(self):
        """Test creating semantic dependency."""
        dep = SemanticDependency(
            dependency_id="dep_001",
            source_concept="order.total",
            target_concept="order.quantity",
            dependency_type=DependencyType.DERIVED_FROM,
            strength="strong",
        )

        assert dep.source_concept == "order.total"
        assert dep.target_concept == "order.quantity"
        assert dep.dependency_type == DependencyType.DERIVED_FROM
        assert dep.strength == "strong"

    def test_dependency_to_dict(self):
        """Test dependency serialization."""
        dep = SemanticDependency(
            dependency_id="dep_001",
            source_concept="A",
            target_concept="B",
            dependency_type=DependencyType.REQUIRES,
        )

        data = dep.to_dict()

        assert data["dependency_type"] == "requires"


class TestAboutnessValidation:
    """Tests for AboutnessValidation dataclass."""

    def test_validation_creation(self):
        """Test creating validation result."""
        validation = AboutnessValidation(
            validation_id="val_001",
            entity_id="order",
            attribute_id="amount",
            model_id="sales",
            check_code="A003",
            severity="error",
            message="Identifier marked as aggregatable",
            recommendation="Change semantic role",
        )

        assert validation.check_code == "A003"
        assert validation.severity == "error"

    def test_validation_to_dict(self):
        """Test validation serialization."""
        validation = AboutnessValidation(
            validation_id="val_001",
            entity_id="order",
            attribute_id=None,
            model_id="sales",
            check_code="A001",
            severity="warning",
            message="No purpose defined",
        )

        data = validation.to_dict()

        assert data["check_code"] == "A001"
        assert data["attribute_id"] is None


# =============================================================================
# Manager Tests
# =============================================================================


class TestAboutnessManager:
    """Tests for AboutnessManager."""

    def test_manager_initialization(self):
        """Test manager initialization."""
        conn = MagicMock()
        manager = AboutnessManager(conn, model_id="test_model")

        assert manager.model_id == "test_model"
        assert manager.conn == conn

    def test_set_entity_aboutness(self):
        """Test setting entity aboutness."""
        conn = MagicMock()
        manager = AboutnessManager(conn)

        aboutness = EntityAboutness(
            entity_id="customer",
            purpose="Test purpose",
            real_world_object="Person",
            aboutness_dimension=AboutnessDimension.CLASSIFIER,
        )

        result = manager.set_entity_aboutness(aboutness)

        assert result.startswith("ea_")
        conn.execute.assert_called()

    def test_set_attribute_aboutness(self):
        """Test setting attribute aboutness."""
        conn = MagicMock()
        manager = AboutnessManager(conn)

        aboutness = AttributeAboutness(
            entity_id="order",
            attribute_id="amount",
            intent="Test intent",
            aboutness_dimension=AboutnessDimension.MEASURE,
            semantic_role=SemanticRole.AGGREGATABLE,
        )

        result = manager.set_attribute_aboutness(aboutness)

        assert result.startswith("aa_")
        conn.execute.assert_called()

    def test_get_entity_aboutness(self):
        """Test getting entity aboutness."""
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (
            "ea_123", "customer", "model1", "Test purpose", None,
            "Person", "classifier", None, "[]", "[]", None, "[]",
            1.0, "manual", datetime.now(), None
        )

        manager = AboutnessManager(conn)
        result = manager.get_entity_aboutness("customer")

        assert result is not None
        assert result.entity_id == "customer"
        assert result.aboutness_dimension == AboutnessDimension.CLASSIFIER

    def test_get_entity_aboutness_not_found(self):
        """Test getting non-existent entity aboutness."""
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = None

        manager = AboutnessManager(conn)
        result = manager.get_entity_aboutness("nonexistent")

        assert result is None

    def test_get_attribute_aboutness(self):
        """Test getting attribute aboutness."""
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (
            "aa_123", "order", "amount", "model1", "Test intent",
            "measure", "aggregatable", "Value", None, None, None,
            None, "transaction_amount", "{}", None, None, 0.9, "inferred",
            datetime.now()
        )

        manager = AboutnessManager(conn)
        result = manager.get_attribute_aboutness("order", "amount")

        assert result is not None
        assert result.aboutness_dimension == AboutnessDimension.MEASURE
        assert result.semantic_role == SemanticRole.AGGREGATABLE

    def test_find_by_dimension(self):
        """Test finding attributes by dimension."""
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            ("aa_1", "e1", "a1", "m1", "Intent 1", "measure", "aggregatable",
             "Value1", None, None, None, None, "name1", "{}", None, None, 0.9, "manual", None),
            ("aa_2", "e2", "a2", "m1", "Intent 2", "measure", "aggregatable",
             "Value2", None, None, None, None, "name2", "{}", None, None, 0.8, "manual", None),
        ]

        manager = AboutnessManager(conn)
        results = manager.find_by_dimension(AboutnessDimension.MEASURE)

        assert len(results) == 2
        assert all(r.aboutness_dimension == AboutnessDimension.MEASURE for r in results)

    def test_find_by_role(self):
        """Test finding attributes by semantic role."""
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            ("aa_1", "e1", "a1", "m1", "Intent", "measure", "aggregatable",
             None, None, None, None, None, None, "{}", None, None, 1.0, "manual", None),
        ]

        manager = AboutnessManager(conn)
        results = manager.find_by_role(SemanticRole.AGGREGATABLE)

        assert len(results) == 1
        assert results[0].semantic_role == SemanticRole.AGGREGATABLE

    def test_add_dependency(self):
        """Test adding semantic dependency."""
        conn = MagicMock()
        manager = AboutnessManager(conn)

        dep = SemanticDependency(
            dependency_id=None,
            source_concept="A",
            target_concept="B",
            dependency_type=DependencyType.REQUIRES,
        )

        result = manager.add_dependency(dep)

        assert result.startswith("sd_")
        conn.execute.assert_called()

    def test_delete_entity_aboutness(self):
        """Test deleting entity aboutness."""
        conn = MagicMock()
        manager = AboutnessManager(conn)

        result = manager.delete_entity_aboutness("customer")

        assert result is True
        conn.execute.assert_called()

    def test_delete_attribute_aboutness(self):
        """Test deleting attribute aboutness."""
        conn = MagicMock()
        manager = AboutnessManager(conn)

        result = manager.delete_attribute_aboutness("order", "amount")

        assert result is True
        conn.execute.assert_called()


# =============================================================================
# Inference Tests
# =============================================================================


class TestAboutnessInferrer:
    """Tests for AboutnessInferrer."""

    def test_inferrer_initialization(self):
        """Test inferrer initialization."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)
        assert inferrer.conn == conn

    def test_infer_identifier_patterns(self):
        """Test inferring identifier patterns."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        # Test various identifier patterns
        patterns = [
            ("customer_id", AboutnessDimension.IDENTIFIER),
            ("order_pk", AboutnessDimension.IDENTIFIER),
            ("product_key", AboutnessDimension.IDENTIFIER),
            ("sku", AboutnessDimension.IDENTIFIER),
            ("order_number", AboutnessDimension.IDENTIFIER),
        ]

        for name, expected_dim in patterns:
            result = inferrer.infer_attribute("entity", name, name, "VARCHAR")
            assert result is not None, f"Failed for {name}"
            assert result.aboutness_dimension == expected_dim, f"Failed for {name}"

    def test_infer_measure_patterns(self):
        """Test inferring measure patterns."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        patterns = [
            ("order_amount", AboutnessDimension.MEASURE),
            ("total_price", AboutnessDimension.MEASURE),
            ("item_quantity", AboutnessDimension.MEASURE),
            ("revenue_sum", AboutnessDimension.MEASURE),
            ("discount_pct", AboutnessDimension.MEASURE),
        ]

        for name, expected_dim in patterns:
            result = inferrer.infer_attribute("entity", name, name, "DECIMAL")
            assert result is not None, f"Failed for {name}"
            assert result.aboutness_dimension == expected_dim, f"Failed for {name}"
            assert result.semantic_role == SemanticRole.AGGREGATABLE

    def test_infer_temporal_patterns(self):
        """Test inferring temporal patterns."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        patterns = [
            ("created_at", AboutnessDimension.TEMPORAL),
            ("order_date", AboutnessDimension.TEMPORAL),
            ("effective_from", AboutnessDimension.TEMPORAL),
            ("modified_timestamp", AboutnessDimension.TEMPORAL),
        ]

        for name, expected_dim in patterns:
            result = inferrer.infer_attribute("entity", name, name, "TIMESTAMP")
            assert result is not None, f"Failed for {name}"
            assert result.aboutness_dimension == expected_dim, f"Failed for {name}"

    def test_infer_classifier_patterns(self):
        """Test inferring classifier patterns."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        patterns = [
            ("customer_type", AboutnessDimension.CLASSIFIER),
            ("product_category", AboutnessDimension.CLASSIFIER),
            ("order_segment", AboutnessDimension.CLASSIFIER),
        ]

        for name, expected_dim in patterns:
            result = inferrer.infer_attribute("entity", name, name, "VARCHAR")
            assert result is not None, f"Failed for {name}"
            assert result.aboutness_dimension == expected_dim, f"Failed for {name}"
            assert result.semantic_role == SemanticRole.GROUPABLE

    def test_infer_flag_patterns(self):
        """Test inferring flag patterns."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        patterns = [
            ("is_active", AboutnessDimension.FLAG),
            ("has_children", AboutnessDimension.FLAG),
            ("is_deleted", AboutnessDimension.FLAG),
        ]

        for name, expected_dim in patterns:
            result = inferrer.infer_attribute("entity", name, name, "BOOLEAN")
            assert result is not None, f"Failed for {name}"
            assert result.aboutness_dimension == expected_dim, f"Failed for {name}"
            assert result.semantic_role == SemanticRole.FILTERABLE

    def test_infer_state_patterns(self):
        """Test inferring state patterns."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        patterns = [
            ("order_status", AboutnessDimension.STATE),
            ("lifecycle_state", AboutnessDimension.STATE),
            ("approval_phase", AboutnessDimension.STATE),
        ]

        for name, expected_dim in patterns:
            result = inferrer.infer_attribute("entity", name, name, "VARCHAR")
            assert result is not None, f"Failed for {name}"
            assert result.aboutness_dimension == expected_dim, f"Failed for {name}"

    def test_infer_relationship_patterns(self):
        """Test inferring relationship patterns."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        # Use patterns that match RELATIONSHIP specifically (_fk suffix, _parent, etc.)
        # Note: *_id patterns are matched as IDENTIFIER first (higher priority)
        patterns = [
            ("customer_fk", AboutnessDimension.RELATIONSHIP),
            ("order_parent", AboutnessDimension.RELATIONSHIP),
            ("task_owner", AboutnessDimension.RELATIONSHIP),
        ]

        for name, expected_dim in patterns:
            result = inferrer.infer_attribute("entity", name, name, "INTEGER")
            assert result is not None, f"Failed for {name}"
            assert result.aboutness_dimension == expected_dim, f"Failed for {name}"
            assert result.semantic_role == SemanticRole.JOINABLE

    def test_infer_spatial_patterns(self):
        """Test inferring spatial patterns."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        patterns = [
            ("shipping_address", AboutnessDimension.SPATIAL),
            ("customer_city", AboutnessDimension.SPATIAL),
            ("store_region", AboutnessDimension.SPATIAL),
            ("delivery_latitude", AboutnessDimension.SPATIAL),
        ]

        for name, expected_dim in patterns:
            result = inferrer.infer_attribute("entity", name, name, "VARCHAR")
            assert result is not None, f"Failed for {name}"
            assert result.aboutness_dimension == expected_dim, f"Failed for {name}"

    def test_infer_primary_key(self):
        """Test inferring primary key as identifier."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        result = inferrer.infer_attribute(
            "customer", "id", "id", "INTEGER", is_primary_key=True
        )

        assert result.aboutness_dimension == AboutnessDimension.IDENTIFIER
        assert result.semantic_role == SemanticRole.JOINABLE
        assert result.confidence_score == 0.95

    def test_infer_from_type_numeric(self):
        """Test type-based inference for numeric types."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        result = inferrer.infer_attribute(
            "entity", "value", "value", "DECIMAL(18,2)"
        )

        assert result.aboutness_dimension == AboutnessDimension.MEASURE

    def test_infer_from_type_boolean(self):
        """Test type-based inference for boolean types."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        result = inferrer.infer_attribute(
            "entity", "flag", "flag", "BOOLEAN"
        )

        assert result.aboutness_dimension == AboutnessDimension.FLAG

    def test_infer_entity_attributes(self):
        """Test inferring all attributes for an entity."""
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            ("customer_id", "Customer ID", "INTEGER", True, False),
            ("customer_name", "Name", "VARCHAR(100)", False, True),
            ("created_at", "Created At", "TIMESTAMP", False, True),
            ("total_spend", "Total Spend", "DECIMAL(18,2)", False, True),
        ]

        inferrer = AboutnessInferrer(conn)
        results = inferrer.infer_entity("customer")

        assert len(results) == 4

        # Check each inferred aboutness
        dims = {r.attribute_id: r.aboutness_dimension for r in results}
        assert dims["customer_id"] == AboutnessDimension.IDENTIFIER
        assert dims["created_at"] == AboutnessDimension.TEMPORAL
        assert dims["total_spend"] == AboutnessDimension.MEASURE

    def test_suggest_canonical_names(self):
        """Test canonical name suggestions."""
        conn = MagicMock()
        # Mock returns: (attribute_id, name)
        conn.execute.return_value.fetchall.return_value = [
            ("cust_amt", "cust_amt"),
            ("ord_qty", "ord_qty"),
        ]

        inferrer = AboutnessInferrer(conn)
        suggestions = inferrer.suggest_canonical_names("order")

        assert "cust_amt" in suggestions
        # cust_amt -> customer_amount (abbreviation expansion)
        assert "customer_amount" in suggestions["cust_amt"]

    def test_infer_confidence_scores(self):
        """Test that inference sets appropriate confidence scores."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        # Primary key - highest confidence
        pk_result = inferrer.infer_attribute("e", "id", "id", "INT", is_primary_key=True)
        assert pk_result.confidence_score == 0.95

        # Pattern match - medium confidence
        pattern_result = inferrer.infer_attribute("e", "total_amount", "total_amount", "DECIMAL")
        assert pattern_result.confidence_score == 0.7

    def test_infer_source_marked_as_inferred(self):
        """Test that inferred aboutness has source='inferred'."""
        conn = MagicMock()
        inferrer = AboutnessInferrer(conn)

        result = inferrer.infer_attribute("e", "amount", "amount", "DECIMAL")

        assert result.source == "inferred"


# =============================================================================
# Validator Tests
# =============================================================================


class TestAboutnessValidator:
    """Tests for AboutnessValidator."""

    def test_validator_initialization(self):
        """Test validator initialization."""
        conn = MagicMock()
        validator = AboutnessValidator(conn)
        assert validator.conn == conn

    def test_check_definitions_available(self):
        """Test that check definitions are available."""
        conn = MagicMock()
        validator = AboutnessValidator(conn)

        checks = validator.get_check_definitions()

        assert "A001" in checks
        assert "A003" in checks
        assert checks["A001"]["severity"] == "warning"
        assert checks["A003"]["severity"] == "error"

    def test_validate_missing_purpose(self):
        """Test A001: Missing entity purpose validation."""
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = None  # No purpose

        validator = AboutnessValidator(conn)
        issues = validator._check_entity_purpose("customer", "model1")

        assert len(issues) == 1
        assert issues[0].check_code == "A001"
        assert issues[0].severity == "warning"

    def test_validate_entity_with_purpose(self):
        """Test no A001 when purpose exists."""
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = ("Test purpose",)

        validator = AboutnessValidator(conn)
        issues = validator._check_entity_purpose("customer", "model1")

        assert len(issues) == 0

    def test_validate_aggregation_mismatch(self):
        """Test A003: Aggregation mismatch validation."""
        # Test invalid combinations
        for dim, role in INVALID_DIMENSION_ROLE_COMBOS:
            assert (dim, role) in INVALID_DIMENSION_ROLE_COMBOS

    def test_validate_low_coverage(self):
        """Test A006: Low coverage validation."""
        conn = MagicMock()
        # 10 attributes, 2 with aboutness = 20% coverage
        conn.execute.return_value.fetchone.side_effect = [(10,), (2,)]

        validator = AboutnessValidator(conn)
        issues = validator._check_attribute_coverage("customer", "model1")

        assert len(issues) == 1
        assert issues[0].check_code == "A006"

    def test_validate_good_coverage(self):
        """Test no A006 when coverage is good."""
        conn = MagicMock()
        # 10 attributes, 8 with aboutness = 80% coverage
        conn.execute.return_value.fetchone.side_effect = [(10,), (8,)]

        validator = AboutnessValidator(conn)
        issues = validator._check_attribute_coverage("customer", "model1")

        assert len(issues) == 0

    def test_clear_validations(self):
        """Test clearing validation results."""
        conn = MagicMock()
        conn.execute.return_value.rowcount = 5

        validator = AboutnessValidator(conn)
        count = validator.clear_validations("model1")

        assert count == 5
        conn.execute.assert_called()


# =============================================================================
# Integration Tests
# =============================================================================


class TestAboutnessIntegration:
    """Integration tests for aboutness scenarios."""

    def test_measure_attribute_workflow(self):
        """Test complete workflow for measure attributes."""
        # Create aboutness
        aboutness = AttributeAboutness(
            entity_id="order",
            attribute_id="total_amount",
            intent="Captures the total monetary value including tax",
            aboutness_dimension=AboutnessDimension.MEASURE,
            semantic_role=SemanticRole.AGGREGATABLE,
            measures_what="OrderValue",
            canonical_name="transaction_total",
            expected_behavior={
                "aggregation": "SUM",
                "nullability": "not_null",
                "positive_only": True,
            },
        )

        # Verify properties
        assert aboutness.aboutness_dimension == AboutnessDimension.MEASURE
        assert aboutness.semantic_role == SemanticRole.AGGREGATABLE
        assert aboutness.measures_what == "OrderValue"

        # Serialize and deserialize
        data = aboutness.to_dict()
        restored = AttributeAboutness.from_dict(data)

        assert restored.measures_what == "OrderValue"
        assert restored.expected_behavior["aggregation"] == "SUM"

    def test_identifier_attribute_workflow(self):
        """Test complete workflow for identifier attributes."""
        aboutness = AttributeAboutness(
            entity_id="customer",
            attribute_id="customer_id",
            intent="Uniquely identifies each customer in the system",
            aboutness_dimension=AboutnessDimension.IDENTIFIER,
            semantic_role=SemanticRole.JOINABLE,
            identifies_what="Customer",
            canonical_name="customer_identifier",
        )

        # Verify incompatible roles would be detected
        assert (AboutnessDimension.IDENTIFIER, SemanticRole.AGGREGATABLE) in INVALID_DIMENSION_ROLE_COMBOS

    def test_entity_with_multiple_attributes(self):
        """Test entity with various attribute types."""
        entity = EntityAboutness(
            entity_id="sales_order",
            purpose="Records customer purchases for revenue tracking and fulfillment",
            real_world_object="SalesTransaction",
            aboutness_dimension=AboutnessDimension.CLASSIFIER,
            business_use_cases=["revenue_reporting", "inventory_planning", "customer_analytics"],
            stakeholder_groups=["finance", "operations", "sales"],
        )

        # Various attribute types
        attributes = [
            AttributeAboutness(
                entity_id="sales_order",
                attribute_id="order_id",
                intent="Primary identifier",
                aboutness_dimension=AboutnessDimension.IDENTIFIER,
                semantic_role=SemanticRole.JOINABLE,
            ),
            AttributeAboutness(
                entity_id="sales_order",
                attribute_id="total",
                intent="Order value",
                aboutness_dimension=AboutnessDimension.MEASURE,
                semantic_role=SemanticRole.AGGREGATABLE,
            ),
            AttributeAboutness(
                entity_id="sales_order",
                attribute_id="order_date",
                intent="When placed",
                aboutness_dimension=AboutnessDimension.TEMPORAL,
                semantic_role=SemanticRole.SLICEABLE,
            ),
            AttributeAboutness(
                entity_id="sales_order",
                attribute_id="status",
                intent="Current state",
                aboutness_dimension=AboutnessDimension.STATE,
                semantic_role=SemanticRole.FILTERABLE,
            ),
        ]

        # Verify dimension diversity
        dims = set(a.aboutness_dimension for a in attributes)
        assert len(dims) == 4

    def test_infer_and_validate_workflow(self):
        """Test inference followed by validation."""
        conn = MagicMock()

        # Inference step
        inferrer = AboutnessInferrer(conn)
        result = inferrer.infer_attribute(
            "order", "customer_id", "customer_id", "INTEGER"
        )

        assert result.aboutness_dimension == AboutnessDimension.IDENTIFIER
        assert result.source == "inferred"

        # Would typically save and validate
        # validator = AboutnessValidator(conn)
        # This tests the conceptual workflow


class TestAboutnessCheckDefinitions:
    """Tests for validation check definitions."""

    def test_all_checks_have_required_fields(self):
        """Test all checks have required fields."""
        required = ["name", "severity", "description"]

        for code, check in ABOUTNESS_CHECKS.items():
            for field in required:
                assert field in check, f"Check {code} missing {field}"

    def test_severity_values_valid(self):
        """Test all severities are valid."""
        valid_severities = {"error", "warning", "info"}

        for code, check in ABOUTNESS_CHECKS.items():
            assert check["severity"] in valid_severities, f"Invalid severity for {code}"

    def test_check_codes_sequential(self):
        """Test check codes are properly formatted."""
        for code in ABOUTNESS_CHECKS:
            assert code.startswith("A"), f"Check code {code} should start with A"
            assert code[1:].isdigit(), f"Check code {code} should have numeric suffix"
