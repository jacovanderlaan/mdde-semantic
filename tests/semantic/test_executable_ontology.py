"""
Tests for SQL-Executable Ontology (ADR-364).

Tests the integration of semantic meaning with SQL execution.
"""

import pytest
from mdde.semantic.ontology import (
    ExecutableMetric,
    SemanticRelationship,
    ExecutableOntology,
    OntologyQuery,
    QueryContext,
    ExecutableQuery,
    RelationshipType,
    JoinType,
    Cardinality,
    FilterCompositionMode,
    ComposedFilter,
    OptimizationHint,
)


class TestExecutableMetric:
    """Tests for ExecutableMetric."""

    @pytest.fixture
    def revenue_metric(self):
        """Sample revenue metric."""
        return ExecutableMetric(
            metric_id="monthly_revenue",
            name="Monthly Revenue",
            definition="Total revenue from orders per month",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            compatible_dimensions=["customer_segment", "region", "product_category"],
            named_filters={
                "enterprise_only": "customer_segment = 'enterprise'",
                "last_12_months": "order_date >= DATEADD(month, -12, CURRENT_DATE)",
            },
            owner="data-analytics@company.com",
            certified=True,
            tags=["finance", "core_metric"],
        )

    def test_metric_creation(self, revenue_metric):
        """Test basic metric creation."""
        assert revenue_metric.metric_id == "monthly_revenue"
        assert revenue_metric.name == "Monthly Revenue"
        assert revenue_metric.sql_expression == "SUM(order_total)"
        assert revenue_metric.certified is True

    def test_metric_to_sql_simple(self, revenue_metric):
        """Test SQL generation without context."""
        sql = revenue_metric.to_sql()
        assert sql == "SUM(order_total)"

    def test_metric_to_sql_with_alias(self, revenue_metric):
        """Test SQL generation with alias."""
        sql = revenue_metric.to_sql(alias="total_revenue")
        assert sql == "SUM(order_total) AS total_revenue"

    def test_metric_to_dict(self, revenue_metric):
        """Test conversion to dictionary."""
        data = revenue_metric.to_dict()

        assert data["metric_id"] == "monthly_revenue"
        assert data["sql_expression"] == "SUM(order_total)"
        assert "customer_segment" in data["compatible_dimensions"]
        assert "enterprise_only" in data["named_filters"]

    def test_metric_from_dict(self, revenue_metric):
        """Test creation from dictionary."""
        data = revenue_metric.to_dict()
        restored = ExecutableMetric.from_dict(data)

        assert restored.metric_id == revenue_metric.metric_id
        assert restored.sql_expression == revenue_metric.sql_expression
        assert restored.certified == revenue_metric.certified


class TestSemanticRelationship:
    """Tests for SemanticRelationship."""

    @pytest.fixture
    def customer_orders_rel(self):
        """Customer to orders relationship."""
        return SemanticRelationship(
            relationship_id="customer_has_orders",
            source_entity_id="dim_customer",
            target_entity_id="fact_orders",
            relationship_type=RelationshipType.HAS,
            cardinality=Cardinality.ONE_TO_MANY,
            name="Customer Orders",
            description="A customer has zero or more orders",
            join_type=JoinType.LEFT,
            join_condition="dim_customer.customer_id = fact_orders.customer_id",
            traversal_cost=1.0,
        )

    def test_relationship_creation(self, customer_orders_rel):
        """Test basic relationship creation."""
        assert customer_orders_rel.relationship_id == "customer_has_orders"
        assert customer_orders_rel.relationship_type == RelationshipType.HAS
        assert customer_orders_rel.cardinality == Cardinality.ONE_TO_MANY

    def test_relationship_to_join_clause(self, customer_orders_rel):
        """Test JOIN clause generation."""
        join = customer_orders_rel.to_join_clause()

        assert "LEFT JOIN" in join
        assert "fact_orders" in join
        assert "ON" in join
        assert "customer_id" in join

    def test_relationship_to_join_with_aliases(self, customer_orders_rel):
        """Test JOIN clause with aliases."""
        join = customer_orders_rel.to_join_clause(
            source_alias="c",
            target_alias="o",
        )

        assert "LEFT JOIN o" in join
        # Should have replaced entity references with aliases

    def test_relationship_to_dict(self, customer_orders_rel):
        """Test conversion to dictionary."""
        data = customer_orders_rel.to_dict()

        assert data["relationship_id"] == "customer_has_orders"
        assert data["relationship_type"] == "has"
        assert data["cardinality"] == "1:N"
        assert "customer_id" in data["join_condition"]

    def test_relationship_from_dict(self, customer_orders_rel):
        """Test creation from dictionary."""
        data = customer_orders_rel.to_dict()
        restored = SemanticRelationship.from_dict(data)

        assert restored.relationship_id == customer_orders_rel.relationship_id
        assert restored.relationship_type == customer_orders_rel.relationship_type


class TestExecutableOntology:
    """Tests for ExecutableOntology."""

    @pytest.fixture
    def sample_ontology(self):
        """Create sample ontology with metrics and relationships."""
        ontology = ExecutableOntology()

        # Register entities
        ontology.register_entity(
            "dim_customer",
            table_name="dim_customer",
            primary_key="customer_id",
            schema="gold",
        )
        ontology.register_entity(
            "fact_orders",
            table_name="fact_orders",
            primary_key="order_id",
            schema="gold",
        )
        ontology.register_entity(
            "dim_product",
            table_name="dim_product",
            primary_key="product_id",
            schema="gold",
        )

        # Add metrics
        ontology.add_metric(ExecutableMetric(
            metric_id="total_revenue",
            name="Total Revenue",
            definition="Sum of all order totals",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            compatible_dimensions=["customer_segment", "product_category"],
        ))

        ontology.add_metric(ExecutableMetric(
            metric_id="order_count",
            name="Order Count",
            definition="Count of orders",
            sql_expression="COUNT(*)",
            grain_entity_id="fact_orders",
            compatible_dimensions=["customer_segment", "product_category"],
        ))

        # Add relationships
        ontology.add_relationship(SemanticRelationship(
            relationship_id="customer_orders",
            source_entity_id="dim_customer",
            target_entity_id="fact_orders",
            relationship_type=RelationshipType.HAS,
            cardinality=Cardinality.ONE_TO_MANY,
            join_condition="dim_customer.customer_id = fact_orders.customer_id",
        ))

        ontology.add_relationship(SemanticRelationship(
            relationship_id="order_product",
            source_entity_id="fact_orders",
            target_entity_id="dim_product",
            relationship_type=RelationshipType.REFERENCES,
            cardinality=Cardinality.MANY_TO_ONE,
            join_condition="fact_orders.product_id = dim_product.product_id",
        ))

        # Register dimensions
        ontology.register_dimension("customer_segment", "dim_customer", "segment")
        ontology.register_dimension("product_category", "dim_product", "category")

        return ontology

    def test_ontology_get_metric(self, sample_ontology):
        """Test retrieving a metric."""
        metric = sample_ontology.get_metric("total_revenue")

        assert metric is not None
        assert metric.name == "Total Revenue"

    def test_ontology_get_relationship(self, sample_ontology):
        """Test retrieving a relationship."""
        rel = sample_ontology.get_relationship("customer_orders")

        assert rel is not None
        assert rel.source_entity_id == "dim_customer"

    def test_ontology_find_path_direct(self, sample_ontology):
        """Test finding direct path between entities."""
        path = sample_ontology.find_path("dim_customer", "fact_orders")

        assert len(path) == 1
        assert path[0].relationship_id == "customer_orders"

    def test_ontology_find_path_multi_hop(self, sample_ontology):
        """Test finding multi-hop path."""
        path = sample_ontology.find_path("dim_customer", "dim_product")

        assert len(path) == 2
        # Should go customer -> orders -> product

    def test_ontology_find_path_same_entity(self, sample_ontology):
        """Test path to same entity."""
        path = sample_ontology.find_path("dim_customer", "dim_customer")

        assert len(path) == 0

    def test_ontology_execute_metric_simple(self, sample_ontology):
        """Test executing a simple metric."""
        result = sample_ontology.execute_metric("total_revenue")

        assert isinstance(result, ExecutableQuery)
        assert "SUM(order_total)" in result.sql
        assert "total_revenue" in result.metrics_used

    def test_ontology_execute_metric_with_dimensions(self, sample_ontology):
        """Test executing metric with dimensions."""
        result = sample_ontology.execute_metric(
            "total_revenue",
            dimensions=["customer_segment"],
        )

        assert "GROUP BY" in result.sql
        assert "customer_segment" in result.dimensions_used

    def test_ontology_to_dict(self, sample_ontology):
        """Test export to dictionary."""
        data = sample_ontology.to_dict()

        assert "metrics" in data
        assert "relationships" in data
        assert "entities" in data
        assert "total_revenue" in data["metrics"]

    def test_ontology_from_dict(self, sample_ontology):
        """Test import from dictionary."""
        data = sample_ontology.to_dict()
        restored = ExecutableOntology.from_dict(data)

        assert restored.get_metric("total_revenue") is not None
        assert restored.get_relationship("customer_orders") is not None


class TestOntologyQuery:
    """Tests for OntologyQuery."""

    @pytest.fixture
    def query_engine(self):
        """Create query engine with sample ontology."""
        ontology = ExecutableOntology()

        ontology.register_entity(
            "fact_orders",
            table_name="fact_orders",
            primary_key="order_id",
            schema="gold",
        )

        ontology.add_metric(ExecutableMetric(
            metric_id="revenue",
            name="revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            compatible_dimensions=["segment"],
        ))

        ontology.register_dimension("segment", "dim_customer", "segment")

        return OntologyQuery(ontology)

    def test_query_single_metric(self, query_engine):
        """Test querying a single metric."""
        result = query_engine.query(metrics=["revenue"])

        assert isinstance(result, ExecutableQuery)
        assert "revenue" in result.metrics_used

    def test_query_with_dimensions(self, query_engine):
        """Test querying with dimensions."""
        result = query_engine.query(
            metrics=["revenue"],
            dimensions=["segment"],
        )

        assert "segment" in result.dimensions_used

    def test_natural_language_simple(self, query_engine):
        """Test simple natural language query."""
        result = query_engine.natural_language_to_sql(
            "Show me the revenue"
        )

        assert result is not None
        assert "revenue" in result.metrics_used

    def test_natural_language_with_dimension(self, query_engine):
        """Test NL query with dimension."""
        result = query_engine.natural_language_to_sql(
            "Show me revenue by segment"
        )

        assert result is not None
        assert "segment" in result.dimensions_used

    def test_natural_language_no_match(self, query_engine):
        """Test NL query with no matching metric."""
        result = query_engine.natural_language_to_sql(
            "What is the weather today?"
        )

        assert result is None


class TestQueryContext:
    """Tests for QueryContext."""

    def test_default_context(self):
        """Test default context values."""
        ctx = QueryContext()

        assert ctx.filters == {}
        assert ctx.dimensions == []
        assert ctx.dialect == "ansi"
        assert ctx.limit is None

    def test_context_with_values(self):
        """Test context with values."""
        from datetime import datetime

        ctx = QueryContext(
            time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
            filters={"status": "active"},
            dimensions=["region", "product"],
            grain="month",
            limit=100,
            dialect="databricks",
        )

        assert ctx.filters["status"] == "active"
        assert "region" in ctx.dimensions
        assert ctx.limit == 100
        assert ctx.dialect == "databricks"


class TestComposedFilter:
    """Tests for ComposedFilter."""

    def test_filter_creation(self):
        """Test basic filter creation."""
        f = ComposedFilter(
            name="active_customers",
            sql_expression="status = 'active'",
            description="Only active customers",
        )

        assert f.name == "active_customers"
        assert f.sql_expression == "status = 'active'"

    def test_filter_compose_and(self):
        """Test composing filters with AND."""
        f1 = ComposedFilter(
            name="active",
            sql_expression="status = 'active'",
        )
        f2 = ComposedFilter(
            name="enterprise",
            sql_expression="segment = 'enterprise'",
        )

        composed = f1.compose_with(f2, FilterCompositionMode.AND)

        assert "AND" in composed.sql_expression
        assert "status = 'active'" in composed.sql_expression
        assert "segment = 'enterprise'" in composed.sql_expression

    def test_filter_compose_or(self):
        """Test composing filters with OR."""
        f1 = ComposedFilter(
            name="us",
            sql_expression="region = 'US'",
        )
        f2 = ComposedFilter(
            name="eu",
            sql_expression="region = 'EU'",
        )

        composed = f1.compose_with(f2, FilterCompositionMode.OR)

        assert "OR" in composed.sql_expression

    def test_filter_negate(self):
        """Test negating a filter."""
        f = ComposedFilter(
            name="active",
            sql_expression="status = 'active'",
        )

        negated = f.negate()

        assert "NOT" in negated.sql_expression
        assert negated.name == "not_active"


class TestOptimizationHint:
    """Tests for OptimizationHint."""

    def test_hint_creation(self):
        """Test basic hint creation."""
        hint = OptimizationHint(
            hint_type="broadcast",
            target="dim_customer",
            parameters={},
            priority=10,
        )

        assert hint.hint_type == "broadcast"
        assert hint.target == "dim_customer"

    def test_databricks_broadcast_hint(self):
        """Test Databricks broadcast hint."""
        hint = OptimizationHint(
            hint_type="broadcast",
            target="dim_customer",
        )

        sql_comment = hint.to_sql_comment("databricks")

        assert "BROADCAST" in sql_comment
        assert "dim_customer" in sql_comment

    def test_databricks_repartition_hint(self):
        """Test Databricks repartition hint."""
        hint = OptimizationHint(
            hint_type="repartition",
            target="fact_orders",
            parameters={"columns": ["customer_id", "order_date"]},
        )

        sql_comment = hint.to_sql_comment("databricks")

        assert "REPARTITION" in sql_comment

    def test_generic_hint(self):
        """Test generic hint for unknown dialect."""
        hint = OptimizationHint(
            hint_type="custom",
            target="table",
        )

        sql_comment = hint.to_sql_comment("unknown")

        assert "Hint:" in sql_comment


class TestMetricInheritance:
    """Tests for metric inheritance."""

    def test_metric_with_parent(self):
        """Test creating metric with parent."""
        metric = ExecutableMetric(
            metric_id="enterprise_revenue",
            name="Enterprise Revenue",
            definition="Revenue from enterprise customers",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            parent_metric_id="revenue",
        )

        assert metric.parent_metric_id == "revenue"

    def test_resolve_inheritance_dimensions(self):
        """Test inheriting dimensions from parent."""
        parent = ExecutableMetric(
            metric_id="revenue",
            name="Revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            compatible_dimensions=["region", "product"],
        )

        child = ExecutableMetric(
            metric_id="enterprise_revenue",
            name="Enterprise Revenue",
            definition="Revenue from enterprise customers",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            parent_metric_id="revenue",
            compatible_dimensions=["customer_segment"],
            inherit_dimensions=True,
        )

        resolved = child.resolve_inheritance(parent)

        # Should have both parent and child dimensions
        assert "region" in resolved.compatible_dimensions
        assert "product" in resolved.compatible_dimensions
        assert "customer_segment" in resolved.compatible_dimensions

    def test_resolve_inheritance_filters(self):
        """Test inheriting filters from parent."""
        parent = ExecutableMetric(
            metric_id="revenue",
            name="Revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            named_filters={"last_30_days": "order_date >= CURRENT_DATE - 30"},
        )

        child = ExecutableMetric(
            metric_id="enterprise_revenue",
            name="Enterprise Revenue",
            definition="Revenue from enterprise customers",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            parent_metric_id="revenue",
            named_filters={"enterprise_only": "segment = 'enterprise'"},
            inherit_filters=True,
        )

        resolved = child.resolve_inheritance(parent)

        # Should have both parent and child filters
        assert "last_30_days" in resolved.named_filters
        assert "enterprise_only" in resolved.named_filters

    def test_resolve_inheritance_no_inherit(self):
        """Test not inheriting when disabled."""
        parent = ExecutableMetric(
            metric_id="revenue",
            name="Revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            compatible_dimensions=["region"],
            named_filters={"filter1": "x = 1"},
        )

        child = ExecutableMetric(
            metric_id="custom_revenue",
            name="Custom Revenue",
            definition="Custom revenue calculation",
            sql_expression="SUM(custom_amount)",
            grain_entity_id="fact_orders",
            parent_metric_id="revenue",
            inherit_dimensions=False,
            inherit_filters=False,
        )

        resolved = child.resolve_inheritance(parent)

        # Should NOT have parent dimensions/filters
        assert "region" not in resolved.compatible_dimensions
        assert "filter1" not in resolved.named_filters

    def test_resolve_inheritance_transformation(self):
        """Test applying transformation to parent SQL."""
        parent = ExecutableMetric(
            metric_id="revenue",
            name="Revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
        )

        child = ExecutableMetric(
            metric_id="revenue_growth_pct",
            name="Revenue Growth %",
            definition="Revenue growth as percentage",
            sql_expression="",  # Will use transformation
            grain_entity_id="fact_orders",
            parent_metric_id="revenue",
            transformation="* 100 / LAG(SUM(order_total))",
        )

        resolved = child.resolve_inheritance(parent)

        assert "SUM(order_total)" in resolved.sql_expression
        assert "* 100" in resolved.sql_expression

    def test_ontology_get_resolved_metric(self):
        """Test getting resolved metric from ontology."""
        ontology = ExecutableOntology()

        ontology.add_metric(ExecutableMetric(
            metric_id="revenue",
            name="Revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            compatible_dimensions=["region"],
        ))

        ontology.add_metric(ExecutableMetric(
            metric_id="enterprise_revenue",
            name="Enterprise Revenue",
            definition="Revenue from enterprise",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            parent_metric_id="revenue",
            compatible_dimensions=["segment"],
        ))

        resolved = ontology.get_resolved_metric("enterprise_revenue")

        assert resolved is not None
        assert "region" in resolved.compatible_dimensions
        assert "segment" in resolved.compatible_dimensions


class TestOntologyFilterComposition:
    """Tests for filter composition in ontology."""

    def test_create_composed_filter(self):
        """Test creating composed filter from ontology."""
        ontology = ExecutableOntology()

        ontology.add_metric(ExecutableMetric(
            metric_id="revenue",
            name="Revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            named_filters={
                "active": "status = 'active'",
                "enterprise": "segment = 'enterprise'",
            },
        ))

        composed = ontology.create_composed_filter(
            ["active", "enterprise"],
            FilterCompositionMode.AND,
        )

        assert composed is not None
        assert "status = 'active'" in composed.sql_expression
        assert "segment = 'enterprise'" in composed.sql_expression
        assert "AND" in composed.sql_expression


class TestOptimizationHintsIntegration:
    """Tests for optimization hints in ontology."""

    def test_metric_with_hints(self):
        """Test metric with optimization hints."""
        metric = ExecutableMetric(
            metric_id="revenue",
            name="Revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            optimization_hints=[
                OptimizationHint(
                    hint_type="broadcast",
                    target="dim_customer",
                    priority=10,
                ),
            ],
        )

        assert len(metric.optimization_hints) == 1
        assert metric.optimization_hints[0].hint_type == "broadcast"

    def test_ontology_get_optimization_hints(self):
        """Test getting optimization hints from ontology."""
        ontology = ExecutableOntology()

        ontology.add_metric(ExecutableMetric(
            metric_id="revenue",
            name="Revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            optimization_hints=[
                OptimizationHint(
                    hint_type="broadcast",
                    target="dim_customer",
                ),
            ],
        ))

        hints = ontology.get_optimization_hints("revenue", dialect="databricks")

        assert len(hints) == 1
        assert "BROADCAST" in hints[0]

    def test_metric_serialization_with_hints(self):
        """Test that hints survive serialization."""
        metric = ExecutableMetric(
            metric_id="revenue",
            name="Revenue",
            definition="Total revenue",
            sql_expression="SUM(order_total)",
            grain_entity_id="fact_orders",
            optimization_hints=[
                OptimizationHint(
                    hint_type="broadcast",
                    target="dim_customer",
                    priority=5,
                ),
            ],
        )

        data = metric.to_dict()
        restored = ExecutableMetric.from_dict(data)

        assert len(restored.optimization_hints) == 1
        assert restored.optimization_hints[0].hint_type == "broadcast"
        assert restored.optimization_hints[0].priority == 5


class TestIntegration:
    """Integration tests for executable ontology."""

    def test_full_workflow(self):
        """Test complete workflow: create ontology, add metrics, execute."""
        # Create ontology
        ontology = ExecutableOntology()

        # Register entities
        ontology.register_entity(
            "customers",
            table_name="dim_customer",
            primary_key="customer_id",
            schema="analytics",
        )
        ontology.register_entity(
            "orders",
            table_name="fact_orders",
            primary_key="order_id",
            schema="analytics",
        )

        # Add metrics
        ontology.add_metric(ExecutableMetric(
            metric_id="aov",
            name="Average Order Value",
            definition="Average value per order",
            sql_expression="AVG(order_total)",
            grain_entity_id="orders",
            compatible_dimensions=["customer_type"],
            certified=True,
        ))

        # Add relationship
        ontology.add_relationship(SemanticRelationship(
            relationship_id="cust_ord",
            source_entity_id="customers",
            target_entity_id="orders",
            relationship_type=RelationshipType.HAS,
            cardinality=Cardinality.ONE_TO_MANY,
            join_condition="customers.customer_id = orders.customer_id",
        ))

        # Register dimension
        ontology.register_dimension("customer_type", "customers", "customer_type")

        # Execute query
        query_engine = OntologyQuery(ontology)
        result = query_engine.query(
            metrics=["aov"],
            dimensions=["customer_type"],
        )

        # Verify
        assert result is not None
        assert "AVG(order_total)" in result.sql
        assert "aov" in result.metrics_used

    def test_roundtrip_serialization(self):
        """Test that ontology survives serialization roundtrip."""
        ontology = ExecutableOntology()

        ontology.add_metric(ExecutableMetric(
            metric_id="test_metric",
            name="Test",
            definition="Test metric",
            sql_expression="COUNT(*)",
            grain_entity_id="test_entity",
        ))

        ontology.add_relationship(SemanticRelationship(
            relationship_id="test_rel",
            source_entity_id="a",
            target_entity_id="b",
            relationship_type=RelationshipType.REFERENCES,
            cardinality=Cardinality.ONE_TO_ONE,
            join_condition="a.id = b.a_id",
        ))

        # Serialize and deserialize
        data = ontology.to_dict()
        restored = ExecutableOntology.from_dict(data)

        # Verify
        metric = restored.get_metric("test_metric")
        assert metric is not None
        assert metric.sql_expression == "COUNT(*)"

        rel = restored.get_relationship("test_rel")
        assert rel is not None
        assert rel.relationship_type == RelationshipType.REFERENCES
