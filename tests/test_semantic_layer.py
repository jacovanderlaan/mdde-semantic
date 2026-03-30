"""
Tests for Semantic Layer Module (ADR-301).

Tests the Patrick Okare-inspired semantic layer:
- SemanticModel with metrics, dimensions, hierarchies
- SemanticLayerManager CRUD operations
- Exporters (dbt, Power BI, Looker)

Feb 2026
"""

import pytest
import duckdb
from datetime import datetime, timezone

from mdde.semantic.types import (
    MetricType,
    AggregationType,
    DimensionType,
    HierarchyType,
    TimeGrain,
    MetricFilter,
    Metric,
    Dimension,
    HierarchyLevel,
    Hierarchy,
    generate_id,
)
from mdde.semantic.model import SemanticModel
from mdde.semantic.manager import SemanticLayerManager
from mdde.semantic.exporter import (
    DbtSemanticExporter,
    PowerBISemanticExporter,
    LookerSemanticExporter,
)


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def conn():
    """Create in-memory DuckDB connection."""
    return duckdb.connect(":memory:")


@pytest.fixture
def manager(conn):
    """Create SemanticLayerManager with schema."""
    return SemanticLayerManager(conn)


@pytest.fixture
def sample_model(manager):
    """Create a sample semantic model with metrics, dimensions, hierarchies."""
    model = manager.create_model(
        name="Sales Analytics",
        description="Core sales metrics and dimensions",
        domain="sales",
        owner="analytics_team",
        default_time_dimension="order_date",
        default_granularity="day",
    )

    # Add metrics
    manager.add_metric(
        model_id=model.model_id,
        name="Total Revenue",
        description="Sum of all order totals",
        metric_type="simple",
        expression="order_total",
        entity_id="fact_orders",
        attribute_id="order_total",
        aggregation="sum",
        unit="$",
        format="#,##0.00",
        certified=True,
        time_grains=["day", "week", "month", "quarter", "year"],
        tags=["revenue", "certified"],
    )

    manager.add_metric(
        model_id=model.model_id,
        name="Order Count",
        description="Number of orders",
        metric_type="simple",
        expression="order_id",
        entity_id="fact_orders",
        attribute_id="order_id",
        aggregation="count",
        certified=True,
    )

    manager.add_metric(
        model_id=model.model_id,
        name="Average Order Value",
        description="Average revenue per order",
        metric_type="derived",
        expression="total_revenue / order_count",
        entity_id="fact_orders",
        unit="$",
    )

    # Add dimensions
    manager.add_dimension(
        model_id=model.model_id,
        name="Order Date",
        entity_id="dim_date",
        attribute_id="date_key",
        dimension_type="time",
        time_granularity="day",
        label="Order Date",
    )

    manager.add_dimension(
        model_id=model.model_id,
        name="Customer Segment",
        entity_id="dim_customer",
        attribute_id="segment",
        dimension_type="categorical",
        allowed_values=["Enterprise", "SMB", "Consumer"],
    )

    manager.add_dimension(
        model_id=model.model_id,
        name="Region",
        entity_id="dim_geography",
        attribute_id="region",
        dimension_type="geographic",
        geo_type="region",
    )

    # Add hierarchy
    manager.add_hierarchy(
        model_id=model.model_id,
        name="Time Hierarchy",
        hierarchy_type="time",
        levels=[
            {"name": "Year", "dimension_id": "dim_year"},
            {"name": "Quarter", "dimension_id": "dim_quarter"},
            {"name": "Month", "dimension_id": "dim_month"},
            {"name": "Day", "dimension_id": "dim_date"},
        ],
    )

    return manager.get_model(model.model_id)


# =============================================================================
# TYPES TESTS
# =============================================================================


class TestMetricType:
    """Tests for MetricType enum."""

    def test_all_types_defined(self):
        """Test all metric types are defined."""
        assert MetricType.SIMPLE.value == "simple"
        assert MetricType.DERIVED.value == "derived"
        assert MetricType.CUMULATIVE.value == "cumulative"
        assert MetricType.RATIO.value == "ratio"
        assert MetricType.CONVERSION.value == "conversion"


class TestAggregationType:
    """Tests for AggregationType enum."""

    def test_all_types_defined(self):
        """Test all aggregation types are defined."""
        assert AggregationType.SUM.value == "sum"
        assert AggregationType.COUNT.value == "count"
        assert AggregationType.COUNT_DISTINCT.value == "count_distinct"
        assert AggregationType.AVG.value == "avg"
        assert AggregationType.MIN.value == "min"
        assert AggregationType.MAX.value == "max"
        assert AggregationType.MEDIAN.value == "median"
        assert AggregationType.PERCENTILE.value == "percentile"


class TestDimensionType:
    """Tests for DimensionType enum."""

    def test_all_types_defined(self):
        """Test all dimension types are defined."""
        assert DimensionType.CATEGORICAL.value == "categorical"
        assert DimensionType.TIME.value == "time"
        assert DimensionType.GEOGRAPHIC.value == "geographic"
        assert DimensionType.NUMERIC.value == "numeric"


class TestHierarchyType:
    """Tests for HierarchyType enum."""

    def test_all_types_defined(self):
        """Test all hierarchy types are defined."""
        assert HierarchyType.TIME.value == "time"
        assert HierarchyType.GEOGRAPHIC.value == "geographic"
        assert HierarchyType.PRODUCT.value == "product"
        assert HierarchyType.ORGANIZATIONAL.value == "organizational"
        assert HierarchyType.CUSTOM.value == "custom"


class TestTimeGrain:
    """Tests for TimeGrain enum."""

    def test_all_grains_defined(self):
        """Test all time grains are defined."""
        assert TimeGrain.HOUR.value == "hour"
        assert TimeGrain.DAY.value == "day"
        assert TimeGrain.WEEK.value == "week"
        assert TimeGrain.MONTH.value == "month"
        assert TimeGrain.QUARTER.value == "quarter"
        assert TimeGrain.YEAR.value == "year"


class TestGenerateId:
    """Tests for ID generation."""

    def test_generate_id_format(self):
        """Test ID format with prefix."""
        metric_id = generate_id("met")
        assert metric_id.startswith("met_")
        assert len(metric_id) == 16  # met_ + 12 chars

    def test_generate_id_uniqueness(self):
        """Test IDs are unique."""
        ids = [generate_id("test") for _ in range(100)]
        assert len(ids) == len(set(ids))


# =============================================================================
# METRIC TESTS
# =============================================================================


class TestMetric:
    """Tests for Metric dataclass."""

    def test_create_simple_metric(self):
        """Test creating a simple metric."""
        metric = Metric(
            metric_id="met_001",
            name="Total Revenue",
            description="Sum of revenue",
            metric_type=MetricType.SIMPLE,
            expression="SUM(revenue)",
            entity_id="fact_sales",
            aggregation=AggregationType.SUM,
        )

        assert metric.metric_id == "met_001"
        assert metric.name == "Total Revenue"
        assert metric.metric_type == MetricType.SIMPLE
        assert metric.aggregation == AggregationType.SUM
        assert metric.certified is False

    def test_create_derived_metric(self):
        """Test creating a derived metric."""
        metric = Metric(
            metric_id="met_002",
            name="AOV",
            description="Average Order Value",
            metric_type=MetricType.DERIVED,
            expression="total_revenue / order_count",
            entity_id="fact_sales",
        )

        assert metric.metric_type == MetricType.DERIVED
        assert "total_revenue" in metric.expression

    def test_metric_to_dict(self):
        """Test metric serialization to dict."""
        metric = Metric(
            metric_id="met_003",
            name="Test Metric",
            description="Test",
            metric_type=MetricType.SIMPLE,
            expression="SUM(amount)",
            entity_id="fact_test",
            aggregation=AggregationType.SUM,
            certified=True,
            tags=["test", "important"],
        )

        d = metric.to_dict()
        assert d["metric_id"] == "met_003"
        assert d["metric_type"] == "simple"
        assert d["aggregation"] == "sum"
        assert d["certified"] is True
        assert "test" in d["tags"]

    def test_metric_from_dict(self):
        """Test metric deserialization from dict."""
        data = {
            "metric_id": "met_004",
            "name": "From Dict",
            "description": "Created from dict",
            "metric_type": "derived",
            "expression": "a / b",
            "entity_id": "fact_test",
        }

        metric = Metric.from_dict(data)
        assert metric.metric_id == "met_004"
        assert metric.metric_type == MetricType.DERIVED

    def test_metric_with_filters(self):
        """Test metric with filters."""
        metric = Metric(
            metric_id="met_005",
            name="Active Revenue",
            description="Revenue from active customers",
            metric_type=MetricType.SIMPLE,
            expression="SUM(revenue)",
            entity_id="fact_sales",
            aggregation=AggregationType.SUM,
            filters=[
                MetricFilter("flt_001", "status = 'active'", "Active only"),
            ],
        )

        assert len(metric.filters) == 1
        assert metric.filters[0].expression == "status = 'active'"


# =============================================================================
# DIMENSION TESTS
# =============================================================================


class TestDimension:
    """Tests for Dimension dataclass."""

    def test_create_categorical_dimension(self):
        """Test creating a categorical dimension."""
        dim = Dimension(
            dimension_id="dim_001",
            name="Product Category",
            entity_id="dim_product",
            attribute_id="category",
            dimension_type=DimensionType.CATEGORICAL,
        )

        assert dim.dimension_type == DimensionType.CATEGORICAL
        assert dim.time_granularity is None

    def test_create_time_dimension(self):
        """Test creating a time dimension."""
        dim = Dimension(
            dimension_id="dim_002",
            name="Order Date",
            entity_id="dim_date",
            attribute_id="date_key",
            dimension_type=DimensionType.TIME,
            time_granularity=TimeGrain.DAY,
        )

        assert dim.dimension_type == DimensionType.TIME
        assert dim.time_granularity == TimeGrain.DAY

    def test_create_geographic_dimension(self):
        """Test creating a geographic dimension."""
        dim = Dimension(
            dimension_id="dim_003",
            name="Country",
            entity_id="dim_geography",
            attribute_id="country_code",
            dimension_type=DimensionType.GEOGRAPHIC,
            geo_type="country",
        )

        assert dim.dimension_type == DimensionType.GEOGRAPHIC
        assert dim.geo_type == "country"

    def test_dimension_with_allowed_values(self):
        """Test dimension with allowed values constraint."""
        dim = Dimension(
            dimension_id="dim_004",
            name="Status",
            entity_id="dim_status",
            attribute_id="status_code",
            dimension_type=DimensionType.CATEGORICAL,
            allowed_values=["Active", "Inactive", "Pending"],
        )

        assert dim.allowed_values == ["Active", "Inactive", "Pending"]


# =============================================================================
# HIERARCHY TESTS
# =============================================================================


class TestHierarchy:
    """Tests for Hierarchy dataclass."""

    def test_create_time_hierarchy(self):
        """Test creating a time hierarchy."""
        hierarchy = Hierarchy(
            hierarchy_id="hier_001",
            name="Calendar Hierarchy",
            hierarchy_type=HierarchyType.TIME,
            levels=[
                HierarchyLevel("lvl_001", "Year", "dim_year", 0),
                HierarchyLevel("lvl_002", "Quarter", "dim_quarter", 1),
                HierarchyLevel("lvl_003", "Month", "dim_month", 2),
                HierarchyLevel("lvl_004", "Day", "dim_day", 3),
            ],
        )

        assert len(hierarchy.levels) == 4
        assert hierarchy.levels[0].name == "Year"
        assert hierarchy.levels[3].name == "Day"

    def test_create_geographic_hierarchy(self):
        """Test creating a geographic hierarchy."""
        hierarchy = Hierarchy(
            hierarchy_id="hier_002",
            name="Location Hierarchy",
            hierarchy_type=HierarchyType.GEOGRAPHIC,
            levels=[
                HierarchyLevel("lvl_005", "Country", "dim_country", 0),
                HierarchyLevel("lvl_006", "Region", "dim_region", 1),
                HierarchyLevel("lvl_007", "City", "dim_city", 2),
            ],
        )

        assert hierarchy.hierarchy_type == HierarchyType.GEOGRAPHIC
        assert len(hierarchy.levels) == 3


# =============================================================================
# SEMANTIC MODEL TESTS
# =============================================================================


class TestSemanticModel:
    """Tests for SemanticModel dataclass."""

    def test_create_empty_model(self):
        """Test creating an empty semantic model."""
        model = SemanticModel(
            name="Test Model",
            description="A test semantic model",
            domain="testing",
        )

        assert model.name == "Test Model"
        assert model.model_id.startswith("sem_")
        assert len(model.metrics) == 0
        assert len(model.dimensions) == 0
        assert len(model.hierarchies) == 0

    def test_add_metric_to_model(self):
        """Test adding a metric to a model."""
        model = SemanticModel(name="Sales Model")

        metric = Metric(
            metric_id="met_001",
            name="Revenue",
            description="Total revenue",
            metric_type=MetricType.SIMPLE,
            expression="SUM(amount)",
            entity_id="fact_sales",
            aggregation=AggregationType.SUM,
        )

        model.add_metric(metric)

        assert len(model.metrics) == 1
        assert model.metrics[0].semantic_model_id == model.model_id

    def test_get_metric_by_id(self):
        """Test getting metric by ID."""
        model = SemanticModel(name="Test Model")

        metric = Metric(
            metric_id="met_001",
            name="Test Metric",
            description="Test",
            metric_type=MetricType.SIMPLE,
            expression="SUM(x)",
            entity_id="fact_test",
        )
        model.add_metric(metric)

        found = model.get_metric("met_001")
        assert found is not None
        assert found.name == "Test Metric"

        not_found = model.get_metric("met_999")
        assert not_found is None

    def test_get_metric_by_name(self):
        """Test getting metric by name."""
        model = SemanticModel(name="Test Model")

        metric = Metric(
            metric_id="met_001",
            name="Revenue",
            description="Total revenue",
            metric_type=MetricType.SIMPLE,
            expression="SUM(amount)",
            entity_id="fact_sales",
        )
        model.add_metric(metric)

        found = model.get_metric_by_name("Revenue")
        assert found is not None
        assert found.metric_id == "met_001"

    def test_remove_metric(self):
        """Test removing a metric from model."""
        model = SemanticModel(name="Test Model")

        metric = Metric(
            metric_id="met_001",
            name="Test",
            description="Test",
            metric_type=MetricType.SIMPLE,
            expression="x",
            entity_id="test",
        )
        model.add_metric(metric)

        assert len(model.metrics) == 1
        result = model.remove_metric("met_001")
        assert result is True
        assert len(model.metrics) == 0

    def test_model_statistics(self):
        """Test getting model statistics."""
        model = SemanticModel(name="Sales Model")

        # Add certified metric
        model.add_metric(Metric(
            metric_id="met_001",
            name="Revenue",
            description="Revenue",
            metric_type=MetricType.SIMPLE,
            expression="x",
            entity_id="fact",
            certified=True,
            tags=["finance"],
        ))

        # Add uncertified metric
        model.add_metric(Metric(
            metric_id="met_002",
            name="Cost",
            description="Cost",
            metric_type=MetricType.SIMPLE,
            expression="y",
            entity_id="fact",
            tags=["finance", "ops"],
        ))

        # Add time dimension
        model.add_dimension(Dimension(
            dimension_id="dim_001",
            name="Date",
            entity_id="dim_date",
            attribute_id="date",
            dimension_type=DimensionType.TIME,
        ))

        stats = model.get_statistics()
        assert stats["total_metrics"] == 2
        assert stats["certified_metrics"] == 1
        assert stats["total_dimensions"] == 1
        assert stats["time_dimensions"] == 1
        assert "finance" in stats["tags"]

    def test_model_to_dict(self):
        """Test model serialization."""
        model = SemanticModel(
            name="Test Model",
            description="Description",
            domain="test",
            owner="test_team",
        )

        d = model.to_dict()
        assert d["name"] == "Test Model"
        assert d["domain"] == "test"
        assert "created_at" in d

    def test_model_to_yaml(self):
        """Test model YAML export."""
        model = SemanticModel(
            name="Test Model",
            domain="test",
        )

        yaml_str = model.to_yaml()
        assert "name: Test Model" in yaml_str
        assert "domain: test" in yaml_str

    def test_model_to_json(self):
        """Test model JSON export."""
        model = SemanticModel(
            name="Test Model",
            domain="test",
        )

        json_str = model.to_json()
        assert '"name": "Test Model"' in json_str

    def test_model_from_dict(self):
        """Test model deserialization."""
        model = SemanticModel(name="Original", domain="test")
        model.add_metric(Metric(
            metric_id="met_001",
            name="Metric",
            description="Desc",
            metric_type=MetricType.SIMPLE,
            expression="x",
            entity_id="fact",
        ))

        d = model.to_dict()
        restored = SemanticModel.from_dict(d)

        assert restored.name == "Original"
        assert len(restored.metrics) == 1

    def test_model_get_summary(self):
        """Test human-readable summary."""
        model = SemanticModel(
            name="Sales Analytics",
            domain="sales",
            owner="analytics_team",
        )
        model.add_metric(Metric(
            metric_id="met_001",
            name="Revenue",
            description="Revenue",
            metric_type=MetricType.SIMPLE,
            expression="x",
            entity_id="fact",
            certified=True,
        ))

        summary = model.get_summary()
        assert "Sales Analytics" in summary
        assert "sales" in summary
        assert "Metrics: 1" in summary


# =============================================================================
# MANAGER TESTS
# =============================================================================


class TestSemanticLayerManager:
    """Tests for SemanticLayerManager."""

    def test_create_model(self, manager):
        """Test creating a semantic model."""
        model = manager.create_model(
            name="Test Model",
            description="A test model",
            domain="testing",
            owner="test_team",
        )

        assert model.model_id.startswith("sem_")
        assert model.name == "Test Model"

    def test_get_model(self, manager):
        """Test retrieving a model."""
        created = manager.create_model(name="Retrieval Test")
        retrieved = manager.get_model(created.model_id)

        assert retrieved is not None
        assert retrieved.name == "Retrieval Test"

    def test_get_nonexistent_model(self, manager):
        """Test retrieving non-existent model."""
        result = manager.get_model("nonexistent_id")
        assert result is None

    def test_list_models(self, manager):
        """Test listing models."""
        manager.create_model(name="Model 1", domain="sales")
        manager.create_model(name="Model 2", domain="finance")
        manager.create_model(name="Model 3", domain="sales")

        all_models = manager.list_models()
        assert len(all_models) == 3

        sales_models = manager.list_models(domain="sales")
        assert len(sales_models) == 2

    def test_update_model(self, manager):
        """Test updating a model."""
        model = manager.create_model(name="Original Name")
        manager.update_model(model.model_id, name="Updated Name")

        updated = manager.get_model(model.model_id)
        assert updated.name == "Updated Name"

    def test_delete_model(self, manager):
        """Test deleting a model."""
        model = manager.create_model(name="To Delete")
        manager.delete_model(model.model_id)

        result = manager.get_model(model.model_id)
        assert result is None

    def test_add_metric(self, manager):
        """Test adding a metric to a model."""
        model = manager.create_model(name="Metric Test")

        metric = manager.add_metric(
            model_id=model.model_id,
            name="Test Metric",
            description="A test metric",
            metric_type="simple",
            expression="SUM(amount)",
            entity_id="fact_test",
            aggregation="sum",
        )

        assert metric.metric_id.startswith("met_")

        # Verify persisted
        loaded = manager.get_model(model.model_id)
        assert len(loaded.metrics) == 1
        assert loaded.metrics[0].name == "Test Metric"

    def test_add_dimension(self, manager):
        """Test adding a dimension to a model."""
        model = manager.create_model(name="Dimension Test")

        dimension = manager.add_dimension(
            model_id=model.model_id,
            name="Test Dimension",
            entity_id="dim_test",
            attribute_id="test_attr",
            dimension_type="categorical",
        )

        assert dimension.dimension_id.startswith("dim_")

        loaded = manager.get_model(model.model_id)
        assert len(loaded.dimensions) == 1

    def test_add_hierarchy(self, manager):
        """Test adding a hierarchy to a model."""
        model = manager.create_model(name="Hierarchy Test")

        hierarchy = manager.add_hierarchy(
            model_id=model.model_id,
            name="Test Hierarchy",
            hierarchy_type="custom",
            levels=[
                {"name": "Level 1", "dimension_id": "dim_1"},
                {"name": "Level 2", "dimension_id": "dim_2"},
            ],
        )

        assert hierarchy.hierarchy_id.startswith("hier_")
        assert len(hierarchy.levels) == 2

        loaded = manager.get_model(model.model_id)
        assert len(loaded.hierarchies) == 1
        assert len(loaded.hierarchies[0].levels) == 2

    def test_get_metrics_by_entity(self, manager):
        """Test finding metrics by entity."""
        model = manager.create_model(name="Entity Search Test")

        manager.add_metric(
            model_id=model.model_id,
            name="Metric A",
            description="Metric A",
            metric_type="simple",
            expression="x",
            entity_id="fact_sales",
        )
        manager.add_metric(
            model_id=model.model_id,
            name="Metric B",
            description="Metric B",
            metric_type="simple",
            expression="y",
            entity_id="fact_orders",
        )
        manager.add_metric(
            model_id=model.model_id,
            name="Metric C",
            description="Metric C",
            metric_type="simple",
            expression="z",
            entity_id="fact_sales",
        )

        sales_metrics = manager.get_metrics_by_entity("fact_sales")
        assert len(sales_metrics) == 2

    def test_get_certified_metrics(self, manager):
        """Test finding certified metrics."""
        model = manager.create_model(name="Cert Test")

        manager.add_metric(
            model_id=model.model_id,
            name="Certified",
            description="Certified metric",
            metric_type="simple",
            expression="x",
            entity_id="fact",
            certified=True,
        )
        manager.add_metric(
            model_id=model.model_id,
            name="Not Certified",
            description="Not certified",
            metric_type="simple",
            expression="y",
            entity_id="fact",
            certified=False,
        )

        certified = manager.get_certified_metrics()
        assert len(certified) == 1
        assert certified[0]["name"] == "Certified"


class TestFullSemanticModel:
    """Integration tests with full sample model."""

    def test_sample_model_creation(self, sample_model):
        """Test sample model is created correctly."""
        assert sample_model.name == "Sales Analytics"
        assert len(sample_model.metrics) == 3
        assert len(sample_model.dimensions) == 3
        assert len(sample_model.hierarchies) == 1

    def test_sample_model_metrics(self, sample_model):
        """Test sample model metrics."""
        revenue = sample_model.get_metric_by_name("Total Revenue")
        assert revenue is not None
        assert revenue.certified is True
        assert revenue.aggregation == AggregationType.SUM

        aov = sample_model.get_metric_by_name("Average Order Value")
        assert aov is not None
        assert aov.metric_type == MetricType.DERIVED

    def test_sample_model_dimensions(self, sample_model):
        """Test sample model dimensions."""
        time_dims = [d for d in sample_model.dimensions if d.dimension_type == DimensionType.TIME]
        assert len(time_dims) == 1

        geo_dims = [d for d in sample_model.dimensions if d.dimension_type == DimensionType.GEOGRAPHIC]
        assert len(geo_dims) == 1

    def test_sample_model_hierarchy(self, sample_model):
        """Test sample model hierarchy."""
        assert len(sample_model.hierarchies) == 1
        time_hier = sample_model.hierarchies[0]
        assert time_hier.name == "Time Hierarchy"
        assert len(time_hier.levels) == 4


# =============================================================================
# EXPORTER TESTS
# =============================================================================


class TestDbtSemanticExporter:
    """Tests for dbt semantic layer exporter."""

    def test_export_model_to_yaml(self, conn, sample_model):
        """Test exporting model to dbt YAML."""
        exporter = DbtSemanticExporter(conn)
        yaml_content = exporter.export_model(sample_model.model_id)

        assert "semantic_models:" in yaml_content
        assert "sales_analytics" in yaml_content
        assert "measures:" in yaml_content

    def test_export_metrics_to_yaml(self, conn, sample_model):
        """Test exporting metrics to dbt YAML."""
        exporter = DbtSemanticExporter(conn)
        yaml_content = exporter.export_metrics(sample_model.model_id)

        assert "metrics:" in yaml_content
        assert "total_revenue" in yaml_content

    def test_export_all(self, conn, sample_model):
        """Test exporting both model and metrics."""
        exporter = DbtSemanticExporter(conn)
        result = exporter.export_all(sample_model.model_id)

        assert "semantic_model" in result
        assert "metrics" in result

    def test_snake_case_conversion(self, conn):
        """Test snake_case conversion."""
        exporter = DbtSemanticExporter(conn)
        assert exporter._to_snake_case("Total Revenue") == "total_revenue"
        assert exporter._to_snake_case("Average Order Value") == "average_order_value"
        assert exporter._to_snake_case("AOV-Test") == "aov_test"


class TestPowerBISemanticExporter:
    """Tests for Power BI semantic exporter."""

    def test_export_model_to_tmdl(self, conn, sample_model):
        """Test exporting model to TMDL."""
        exporter = PowerBISemanticExporter(conn)
        tmdl_content = exporter.export_model(sample_model.model_id)

        assert "model Model" in tmdl_content
        assert "Sales Analytics" in tmdl_content

    def test_export_dax_measures(self, conn, sample_model):
        """Test exporting DAX measures."""
        exporter = PowerBISemanticExporter(conn)
        dax_content = exporter.export_dax_measures(sample_model.model_id)

        assert "// DAX Measures" in dax_content
        assert "Total_Revenue" in dax_content or "Total Revenue" in dax_content
        assert "SUM" in dax_content

    def test_export_dataset_json(self, conn, sample_model):
        """Test exporting Power BI dataset JSON."""
        exporter = PowerBISemanticExporter(conn)
        dataset = exporter.export_pbix_dataset(sample_model.model_id)

        assert dataset["name"] == "Sales Analytics"
        assert "tables" in dataset
        assert len(dataset["tables"]) > 0

    def test_aggregation_mapping(self, conn):
        """Test aggregation type mapping to DAX."""
        exporter = PowerBISemanticExporter(conn)
        assert exporter._map_aggregation_to_dax(AggregationType.SUM) == "SUM"
        assert exporter._map_aggregation_to_dax(AggregationType.COUNT_DISTINCT) == "DISTINCTCOUNT"
        assert exporter._map_aggregation_to_dax(AggregationType.AVG) == "AVERAGE"


class TestLookerSemanticExporter:
    """Tests for Looker LookML exporter."""

    def test_export_model_to_lookml(self, conn, sample_model):
        """Test exporting model to LookML."""
        exporter = LookerSemanticExporter(conn)
        lookml_content = exporter.export_model(sample_model.model_id)

        assert "explore:" in lookml_content
        assert "sales_analytics" in lookml_content

    def test_export_explore(self, conn, sample_model):
        """Test exporting explore definition."""
        exporter = LookerSemanticExporter(conn)
        explore = exporter.export_explore(sample_model.model_id)

        assert "explore: sales_analytics" in explore
        assert "label:" in explore

    def test_export_views(self, conn, sample_model):
        """Test exporting view definitions."""
        exporter = LookerSemanticExporter(conn)
        views = exporter.export_views(sample_model.model_id)

        assert len(views) > 0
        # Check for view content
        for view_name, view_content in views.items():
            assert "view:" in view_content
            assert "sql_table_name:" in view_content

    def test_dimension_type_mapping(self, conn):
        """Test dimension type mapping."""
        exporter = LookerSemanticExporter(conn)
        assert exporter._map_dimension_type(DimensionType.CATEGORICAL) == "string"
        assert exporter._map_dimension_type(DimensionType.TIME) == "time"
        assert exporter._map_dimension_type(DimensionType.NUMERIC) == "number"

    def test_aggregation_mapping(self, conn):
        """Test aggregation type mapping."""
        exporter = LookerSemanticExporter(conn)
        assert exporter._map_aggregation_to_lookml(AggregationType.SUM) == "sum"
        assert exporter._map_aggregation_to_lookml(AggregationType.COUNT_DISTINCT) == "count_distinct"
        assert exporter._map_aggregation_to_lookml(AggregationType.AVG) == "average"
