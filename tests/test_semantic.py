"""
Tests for Semantic Module (ADR-244, 245, 246).

Tests cover:
- Ontology: Concepts, Properties, Links (ADR-244)
- Metrics: MetricDefinition, Dimensions, Queries (ADR-245)
- Agent Context: KnowledgePlane, EntityContext, AgentQuery (ADR-246)
"""

import pytest
from datetime import datetime, timezone

from mdde.semantic import (
    # ==================== Ontology (ADR-244) ====================
    ConceptType,
    PropertyType,
    PropertyCharacteristic,
    LinkType,
    Ontology,
    OntologyConcept,
    OntologyProperty,
    OntologyRestriction,
    EntityConceptLink,
    AttributeSemanticLink,
    ConceptHierarchy,
    STANDARD_NAMESPACES,
    XSD_DATATYPES,
    # ==================== Metrics (ADR-245) ====================
    MetricType,
    AggregationType,
    TimeGrain,
    DimensionRole,
    MetricStatus,
    MetricDefinition,
    MetricDimension,
    MetricFilter,
    DerivedMetricFormula,
    MetricGoal,
    MetricAlert,
    MetricQuery,
    MetricQueryResult,
    # ==================== Agent Context (ADR-246) ====================
    ContextType,
    QueryIntent,
    ResponseFormat,
    ContextRequest,
    EntityContext,
    RelationshipContext,
    LineageContext,
    GlossaryContext,
    MetricContext,
    ConceptContext,
    DomainContext,
    QualityContext,
    KnowledgePlane,
    AgentQuery,
    AgentResponse,
)


# =============================================================================
# ONTOLOGY TESTS (ADR-244)
# =============================================================================


class TestConceptType:
    """Tests for ConceptType enum."""

    def test_all_types_defined(self):
        """Test all concept types are defined."""
        assert ConceptType.CLASS.value == "class"
        assert ConceptType.INDIVIDUAL.value == "individual"
        assert ConceptType.DATATYPE.value == "datatype"


class TestPropertyType:
    """Tests for PropertyType enum."""

    def test_all_types_defined(self):
        """Test all property types are defined."""
        assert PropertyType.OBJECT_PROPERTY.value == "object_property"
        assert PropertyType.DATA_PROPERTY.value == "data_property"
        assert PropertyType.ANNOTATION_PROPERTY.value == "annotation_property"


class TestPropertyCharacteristic:
    """Tests for PropertyCharacteristic enum."""

    def test_all_characteristics_defined(self):
        """Test all property characteristics are defined."""
        assert PropertyCharacteristic.FUNCTIONAL.value == "functional"
        assert PropertyCharacteristic.INVERSE_FUNCTIONAL.value == "inverse_functional"
        assert PropertyCharacteristic.TRANSITIVE.value == "transitive"
        assert PropertyCharacteristic.SYMMETRIC.value == "symmetric"
        assert PropertyCharacteristic.ASYMMETRIC.value == "asymmetric"
        assert PropertyCharacteristic.REFLEXIVE.value == "reflexive"
        assert PropertyCharacteristic.IRREFLEXIVE.value == "irreflexive"


class TestLinkType:
    """Tests for LinkType enum."""

    def test_all_types_defined(self):
        """Test all link types are defined."""
        assert LinkType.INSTANCE_OF.value == "instance_of"
        assert LinkType.REPRESENTS.value == "represents"
        assert LinkType.RELATED_TO.value == "related_to"
        assert LinkType.DERIVED_FROM.value == "derived_from"


class TestOntology:
    """Tests for Ontology model."""

    def test_creation(self):
        """Test ontology is created correctly."""
        onto = Ontology(
            ontology_id="ONT_sales",
            name="Sales Ontology",
            namespace="http://example.com/sales#",
            version="1.0",
            description="Sales domain ontology",
            prefix="sales",
        )
        assert onto.ontology_id == "ONT_sales"
        assert onto.name == "Sales Ontology"
        assert onto.namespace == "http://example.com/sales#"
        assert onto.prefix == "sales"

    def test_get_uri(self):
        """Test get_uri method."""
        onto = Ontology(
            ontology_id="ONT_test",
            name="Test",
            namespace="http://example.com/test#",
        )
        uri = onto.get_uri("Customer")
        assert uri == "http://example.com/test/Customer"

    def test_get_prefixed(self):
        """Test get_prefixed method."""
        onto = Ontology(
            ontology_id="ONT_test",
            name="Test",
            namespace="http://example.com/test#",
            prefix="test",
        )
        prefixed = onto.get_prefixed("Customer")
        assert prefixed == "test:Customer"

    def test_timestamps_auto_set(self):
        """Test timestamps are auto-set."""
        onto = Ontology(
            ontology_id="ONT_test",
            name="Test",
            namespace="http://example.com/test#",
        )
        assert onto.created_at is not None
        assert onto.updated_at is not None


class TestOntologyConcept:
    """Tests for OntologyConcept model."""

    def test_creation(self):
        """Test concept is created correctly."""
        concept = OntologyConcept(
            concept_id="CON_customer",
            ontology_id="ONT_sales",
            label="Customer",
            definition="A person or organization that purchases products",
            concept_type=ConceptType.CLASS,
        )
        assert concept.concept_id == "CON_customer"
        assert concept.label == "Customer"
        assert concept.concept_type == ConceptType.CLASS

    def test_local_name_from_uri(self):
        """Test local_name property with URI."""
        concept = OntologyConcept(
            concept_id="CON_test",
            ontology_id="ONT_test",
            label="Test",
            concept_uri="http://example.com/test#Customer",
        )
        assert concept.local_name == "Customer"

    def test_local_name_fallback(self):
        """Test local_name fallback to concept_id."""
        concept = OntologyConcept(
            concept_id="CON_test",
            ontology_id="ONT_test",
            label="Test",
        )
        assert concept.local_name == "CON_test"

    def test_superclass_relationship(self):
        """Test superclass relationship."""
        concept = OntologyConcept(
            concept_id="CON_premium_customer",
            ontology_id="ONT_sales",
            label="Premium Customer",
            superclass_ids=["CON_customer"],
        )
        assert "CON_customer" in concept.superclass_ids


class TestOntologyProperty:
    """Tests for OntologyProperty model."""

    def test_creation(self):
        """Test property is created correctly."""
        prop = OntologyProperty(
            property_id="PROP_hasOrder",
            ontology_id="ONT_sales",
            label="has Order",
            definition="Relates a customer to their orders",
            property_type=PropertyType.OBJECT_PROPERTY,
            domain_concept_ids=["CON_customer"],
            range_concept_ids=["CON_order"],
        )
        assert prop.property_id == "PROP_hasOrder"
        assert prop.property_type == PropertyType.OBJECT_PROPERTY
        assert "CON_customer" in prop.domain_concept_ids

    def test_is_functional(self):
        """Test is_functional property."""
        prop = OntologyProperty(
            property_id="PROP_test",
            ontology_id="ONT_test",
            label="Test",
            characteristics=[PropertyCharacteristic.FUNCTIONAL],
        )
        assert prop.is_functional is True
        assert prop.is_transitive is False

    def test_data_property(self):
        """Test data property with datatype."""
        prop = OntologyProperty(
            property_id="PROP_customerName",
            ontology_id="ONT_sales",
            label="customer name",
            property_type=PropertyType.DATA_PROPERTY,
            range_datatype="xsd:string",
        )
        assert prop.property_type == PropertyType.DATA_PROPERTY
        assert prop.range_datatype == "xsd:string"


class TestEntityConceptLink:
    """Tests for EntityConceptLink model."""

    def test_creation(self):
        """Test link is created correctly."""
        link = EntityConceptLink(
            link_id="LNK_001",
            entity_id="ENT_customer",
            concept_id="CON_customer",
            link_type=LinkType.INSTANCE_OF,
            confidence=0.95,
            rationale="Entity maps directly to Customer concept",
        )
        assert link.link_id == "LNK_001"
        assert link.link_type == LinkType.INSTANCE_OF
        assert link.confidence == 0.95


class TestConceptHierarchy:
    """Tests for ConceptHierarchy model."""

    def test_creation(self):
        """Test hierarchy is created correctly."""
        hierarchy = ConceptHierarchy(
            concept_id="CON_premium_customer",
            depth=2,
            path=["CON_entity", "CON_customer", "CON_premium_customer"],
            children=["CON_vip_customer"],
            descendant_count=3,
        )
        assert hierarchy.depth == 2
        assert len(hierarchy.path) == 3
        assert hierarchy.descendant_count == 3


class TestOntologyConstants:
    """Tests for ontology constants."""

    def test_standard_namespaces(self):
        """Test standard namespaces are defined."""
        assert "owl" in STANDARD_NAMESPACES
        assert "rdf" in STANDARD_NAMESPACES
        assert "rdfs" in STANDARD_NAMESPACES
        assert "xsd" in STANDARD_NAMESPACES
        assert STANDARD_NAMESPACES["owl"] == "http://www.w3.org/2002/07/owl#"

    def test_xsd_datatypes(self):
        """Test XSD datatypes are defined."""
        assert "string" in XSD_DATATYPES
        assert "integer" in XSD_DATATYPES
        assert "decimal" in XSD_DATATYPES
        assert XSD_DATATYPES["string"] == "xsd:string"


# =============================================================================
# METRICS TESTS (ADR-245)
# =============================================================================


class TestMetricType:
    """Tests for MetricType enum."""

    def test_all_types_defined(self):
        """Test all metric types are defined."""
        assert MetricType.SIMPLE.value == "simple"
        assert MetricType.DERIVED.value == "derived"
        assert MetricType.RATIO.value == "ratio"
        assert MetricType.CUMULATIVE.value == "cumulative"
        assert MetricType.PERIOD_OVER_PERIOD.value == "period_over_period"
        assert MetricType.WINDOW.value == "window"


class TestAggregationType:
    """Tests for AggregationType enum."""

    def test_all_types_defined(self):
        """Test all aggregation types are defined."""
        assert AggregationType.SUM.value == "SUM"
        assert AggregationType.COUNT.value == "COUNT"
        assert AggregationType.COUNT_DISTINCT.value == "COUNT_DISTINCT"
        assert AggregationType.AVG.value == "AVG"
        assert AggregationType.MIN.value == "MIN"
        assert AggregationType.MAX.value == "MAX"


class TestTimeGrain:
    """Tests for TimeGrain enum."""

    def test_all_grains_defined(self):
        """Test all time grains are defined."""
        assert TimeGrain.SECOND.value == "second"
        assert TimeGrain.MINUTE.value == "minute"
        assert TimeGrain.HOUR.value == "hour"
        assert TimeGrain.DAY.value == "day"
        assert TimeGrain.WEEK.value == "week"
        assert TimeGrain.MONTH.value == "month"
        assert TimeGrain.QUARTER.value == "quarter"
        assert TimeGrain.YEAR.value == "year"


class TestDimensionRole:
    """Tests for DimensionRole enum."""

    def test_all_roles_defined(self):
        """Test all dimension roles are defined."""
        assert DimensionRole.SLICE.value == "slice"
        assert DimensionRole.FILTER.value == "filter"
        assert DimensionRole.GROUP_BY.value == "group_by"
        assert DimensionRole.DRILL_DOWN.value == "drill_down"


class TestMetricStatus:
    """Tests for MetricStatus enum."""

    def test_all_statuses_defined(self):
        """Test all metric statuses are defined."""
        assert MetricStatus.DRAFT.value == "draft"
        assert MetricStatus.ACTIVE.value == "active"
        assert MetricStatus.DEPRECATED.value == "deprecated"
        assert MetricStatus.ARCHIVED.value == "archived"


class TestMetricDefinition:
    """Tests for MetricDefinition model."""

    def test_creation(self):
        """Test metric is created correctly."""
        metric = MetricDefinition(
            metric_id="MET_total_revenue",
            metric_name="total_revenue",
            display_name="Total Revenue",
            description="Sum of all order amounts",
            metric_type=MetricType.SIMPLE,
            entity_id="ENT_order",
            attribute_id="ATT_amount",
            aggregation=AggregationType.SUM,
            unit="USD",
        )
        assert metric.metric_id == "MET_total_revenue"
        assert metric.display_name == "Total Revenue"
        assert metric.aggregation == AggregationType.SUM
        assert metric.unit == "USD"

    def test_derived_metric(self):
        """Test derived metric with dependencies."""
        metric = MetricDefinition(
            metric_id="MET_avg_order_value",
            metric_name="avg_order_value",
            display_name="Average Order Value",
            metric_type=MetricType.DERIVED,
            expression="total_revenue / order_count",
            depends_on_metrics=["MET_total_revenue", "MET_order_count"],
        )
        assert metric.metric_type == MetricType.DERIVED
        assert len(metric.depends_on_metrics) == 2


class TestMetricDimension:
    """Tests for MetricDimension model."""

    def test_creation(self):
        """Test dimension is created correctly."""
        dim = MetricDimension(
            dimension_id="DIM_region",
            metric_id="MET_total_revenue",
            attribute_id="ATT_region",
            dimension_name="region",
            display_name="Sales Region",
            role=DimensionRole.GROUP_BY,
        )
        assert dim.dimension_id == "DIM_region"
        assert dim.role == DimensionRole.GROUP_BY


class TestMetricQuery:
    """Tests for MetricQuery model."""

    def test_creation(self):
        """Test query is created correctly."""
        query = MetricQuery(
            metric_ids=["MET_total_revenue", "MET_order_count"],
            dimensions=["region", "product_category"],
            filters={"region": "North America"},
            time_grain=TimeGrain.MONTH,
            limit=100,
        )
        assert len(query.metric_ids) == 2
        assert len(query.dimensions) == 2
        assert query.time_grain == TimeGrain.MONTH


class TestMetricQueryResult:
    """Tests for MetricQueryResult model."""

    def test_creation(self):
        """Test query result is created correctly."""
        query = MetricQuery(metric_ids=["MET_total_revenue"])
        result = MetricQueryResult(
            query=query,
            columns=["region", "total_revenue"],
            data=[{"region": "North", "total_revenue": 1000000}],
            row_count=1,
            generated_sql="SELECT region, SUM(amount) FROM orders GROUP BY region",
            execution_time_ms=45.5,
        )
        assert result.row_count == 1
        assert result.execution_time_ms == 45.5


# =============================================================================
# AGENT CONTEXT TESTS (ADR-246)
# =============================================================================


class TestContextType:
    """Tests for ContextType enum."""

    def test_all_types_defined(self):
        """Test all context types are defined."""
        assert ContextType.ENTITY.value == "entity"
        assert ContextType.RELATIONSHIP.value == "relationship"
        assert ContextType.LINEAGE.value == "lineage"
        assert ContextType.GLOSSARY.value == "glossary"
        assert ContextType.METRIC.value == "metric"
        assert ContextType.CONCEPT.value == "concept"
        assert ContextType.DOMAIN.value == "domain"
        assert ContextType.QUALITY.value == "quality"


class TestQueryIntent:
    """Tests for QueryIntent enum."""

    def test_all_intents_defined(self):
        """Test all query intents are defined."""
        assert QueryIntent.EXPLORE.value == "explore"
        assert QueryIntent.UNDERSTAND.value == "understand"
        assert QueryIntent.FIND.value == "find"
        assert QueryIntent.ANALYZE.value == "analyze"
        assert QueryIntent.GENERATE.value == "generate"
        assert QueryIntent.VALIDATE.value == "validate"
        assert QueryIntent.SUGGEST.value == "suggest"


class TestResponseFormat:
    """Tests for ResponseFormat enum."""

    def test_all_formats_defined(self):
        """Test all response formats are defined."""
        assert ResponseFormat.TEXT.value == "text"
        assert ResponseFormat.MARKDOWN.value == "markdown"
        assert ResponseFormat.JSON.value == "json"
        assert ResponseFormat.SQL.value == "sql"
        assert ResponseFormat.YAML.value == "yaml"
        assert ResponseFormat.TABLE.value == "table"


class TestContextRequest:
    """Tests for ContextRequest model."""

    def test_creation(self):
        """Test request is created correctly."""
        request = ContextRequest(
            request_id="REQ_001",
            context_types=[ContextType.ENTITY, ContextType.RELATIONSHIP],
            entity_ids=["ENT_customer", "ENT_order"],
            domain="sales",
            max_depth=3,
        )
        assert request.request_id == "REQ_001"
        assert len(request.context_types) == 2
        assert request.max_depth == 3


class TestEntityContext:
    """Tests for EntityContext model."""

    def test_creation(self):
        """Test entity context is created correctly."""
        ctx = EntityContext(
            entity_id="ENT_customer",
            entity_name="customer",
            display_name="Customer",
            description="A customer in the system",
            domain="sales",
            stereotype="dim_dimension",
            layer="integration",
            attributes=[
                {"name": "customer_id", "data_type": "INTEGER"},
                {"name": "customer_name", "data_type": "VARCHAR"},
            ],
            primary_key=["customer_id"],
        )
        assert ctx.entity_id == "ENT_customer"
        assert ctx.display_name == "Customer"
        assert len(ctx.attributes) == 2
        assert "customer_id" in ctx.primary_key


class TestRelationshipContext:
    """Tests for RelationshipContext model."""

    def test_creation(self):
        """Test relationship context is created correctly."""
        ctx = RelationshipContext(
            from_entity="customer",
            to_entity="order",
            relationship_type="one_to_many",
            cardinality="1:N",
            description="A customer can have many orders",
            join_columns=[{"from": "customer_id", "to": "customer_id"}],
        )
        assert ctx.from_entity == "customer"
        assert ctx.to_entity == "order"
        assert ctx.cardinality == "1:N"


class TestLineageContext:
    """Tests for LineageContext model."""

    def test_creation(self):
        """Test lineage context is created correctly."""
        ctx = LineageContext(
            entity_id="ENT_customer_dim",
            entity_name="customer_dim",
            upstream=[{"entity_id": "ENT_stg_customer", "entity_name": "stg_customer"}],
            downstream=[{"entity_id": "ENT_fact_sales", "entity_name": "fact_sales"}],
            transformations=["SCD Type 2"],
        )
        assert len(ctx.upstream) == 1
        assert len(ctx.downstream) == 1


class TestKnowledgePlane:
    """Tests for KnowledgePlane model."""

    def test_creation(self):
        """Test knowledge plane is created correctly."""
        request = ContextRequest(request_id="REQ_001")
        plane = KnowledgePlane(
            request=request,
            entities=[
                EntityContext(
                    entity_id="ENT_customer",
                    entity_name="customer",
                    display_name="Customer",
                    description="Customer entity",
                )
            ],
            relationships=[
                RelationshipContext(
                    from_entity="customer",
                    to_entity="order",
                    relationship_type="one_to_many",
                    cardinality="1:N",
                )
            ],
            total_entities=1,
            total_relationships=1,
        )
        assert len(plane.entities) == 1
        assert len(plane.relationships) == 1
        assert plane.total_entities == 1

    def test_to_prompt_context(self):
        """Test to_prompt_context method."""
        request = ContextRequest(request_id="REQ_001")
        plane = KnowledgePlane(
            request=request,
            entities=[
                EntityContext(
                    entity_id="ENT_customer",
                    entity_name="customer",
                    display_name="Customer",
                    description="A customer in the system",
                    attributes=[
                        {"name": "customer_id", "data_type": "INTEGER", "description": "Primary key"},
                    ],
                )
            ],
            domains=[
                DomainContext(
                    domain_id="DOM_sales",
                    domain_name="Sales",
                    description="Sales domain",
                    entities=["customer", "order"],
                )
            ],
        )

        context = plane.to_prompt_context(max_tokens=8000)

        assert "## Entities" in context or "## Data Domains" in context
        assert "customer" in context.lower()

    def test_to_json_context(self):
        """Test to_json_context method."""
        request = ContextRequest(request_id="REQ_001")
        plane = KnowledgePlane(
            request=request,
            entities=[
                EntityContext(
                    entity_id="ENT_customer",
                    entity_name="customer",
                    display_name="Customer",
                    description="Customer entity",
                )
            ],
            total_entities=1,
        )

        json_ctx = plane.to_json_context()

        assert "summary" in json_ctx
        assert "entities" in json_ctx
        assert json_ctx["summary"]["total_entities"] == 1


class TestAgentQuery:
    """Tests for AgentQuery model."""

    def test_creation(self):
        """Test agent query is created correctly."""
        query = AgentQuery(
            query_id="QRY_001",
            query_text="What entities are in the sales domain?",
            intent=QueryIntent.EXPLORE,
            response_format=ResponseFormat.MARKDOWN,
            max_context_tokens=4000,
        )
        assert query.query_id == "QRY_001"
        assert query.intent == QueryIntent.EXPLORE
        assert query.response_format == ResponseFormat.MARKDOWN


class TestAgentResponse:
    """Tests for AgentResponse model."""

    def test_creation(self):
        """Test agent response is created correctly."""
        response = AgentResponse(
            query_id="QRY_001",
            response_text="The sales domain contains 5 entities...",
            response_format=ResponseFormat.MARKDOWN,
            confidence=0.95,
            sources=["ENT_customer", "ENT_order"],
            suggestions=["Consider adding customer_segment entity"],
        )
        assert response.query_id == "QRY_001"
        assert response.confidence == 0.95
        assert len(response.sources) == 2


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


class TestSemanticIntegration:
    """Integration tests for semantic module."""

    def test_ontology_concept_hierarchy(self):
        """Test building concept hierarchy."""
        # Create ontology
        onto = Ontology(
            ontology_id="ONT_test",
            name="Test Ontology",
            namespace="http://example.com/test#",
        )

        # Create concepts
        parent = OntologyConcept(
            concept_id="CON_entity",
            ontology_id=onto.ontology_id,
            label="Entity",
        )

        child = OntologyConcept(
            concept_id="CON_customer",
            ontology_id=onto.ontology_id,
            label="Customer",
            superclass_ids=[parent.concept_id],
        )

        # Verify hierarchy
        assert parent.concept_id in child.superclass_ids

    def test_metric_with_dimensions(self):
        """Test metric with dimensions."""
        metric = MetricDefinition(
            metric_id="MET_revenue",
            metric_name="revenue",
            display_name="Revenue",
            aggregation=AggregationType.SUM,
        )

        dim = MetricDimension(
            dimension_id="DIM_region",
            metric_id=metric.metric_id,
            attribute_id="ATT_region",
            dimension_name="region",
            role=DimensionRole.GROUP_BY,
        )

        assert dim.metric_id == metric.metric_id

    def test_agent_context_roundtrip(self):
        """Test building and querying agent context."""
        # Create request
        request = ContextRequest(
            request_id="REQ_test",
            context_types=[ContextType.ENTITY, ContextType.METRIC],
            entity_ids=["ENT_customer"],
        )

        # Build knowledge plane
        plane = KnowledgePlane(
            request=request,
            entities=[
                EntityContext(
                    entity_id="ENT_customer",
                    entity_name="customer",
                    display_name="Customer",
                    description="Customer entity",
                )
            ],
            metrics=[
                MetricContext(
                    metric_id="MET_revenue",
                    metric_name="revenue",
                    display_name="Revenue",
                    description="Total revenue",
                    calculation="SUM(amount)",
                )
            ],
        )

        # Generate context
        prompt_context = plane.to_prompt_context()
        json_context = plane.to_json_context()

        assert plane.request.request_id == "REQ_test"
        assert len(json_context["entities"]) == 1
        assert len(json_context["metrics"]) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
