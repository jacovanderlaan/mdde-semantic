# ADR-301: Semantic Layer Module

**Status**: Proposed
**Date**: 2026-02-24
**Author**: MDDE Team
**Inspired By**: Patrick Okare's "Five Must-Have Layers" (Analytics & Consumption Layer)

## Context

Patrick Okare's article on [Modern Analytics Platform Layers](https://karetech.medium.com/data-stack-five-must-have-layers-of-a-modern-analytics-platform-126b797d638e) identifies the **Semantic Layer** as crucial for the Analytics & Consumption Layer:

> "The Semantic Layer provides a consistent, unified view of key business metrics across the organization. Think of it as the 'translator' between raw data and meaningful business insights. Without it, different departments or teams might calculate the same metric in varying ways, leading to confusion, misalignment, and errors in decision-making."

### Current MDDE State

MDDE excels at the Modeling & Transformation layer (Layer 3) but has limited support for the Analytics & Consumption layer (Layer 4):

| Feature | Status |
|---------|--------|
| Entity/attribute modeling | ✅ Complete |
| Transformation mappings | ✅ Complete |
| Data quality rules | ✅ Complete |
| **Metric definitions** | ❌ Missing |
| **Dimension hierarchies** | ❌ Missing |
| **Calculated measures** | ❌ Missing |
| **Semantic model export** | ❌ Missing |

### The Problem

Without a semantic layer, organizations face:
1. **Metric inconsistency**: Revenue calculated differently in sales vs. finance reports
2. **Duplicated logic**: Same calculations hardcoded in multiple dashboards
3. **No single source of truth**: Business users don't know which metric to trust
4. **Difficult governance**: Can't track where metrics are used

### Industry Context

The semantic layer is gaining momentum:
- **dbt Semantic Layer** (MetricFlow) - Metrics as code
- **Looker LookML** - Explores, views, measures
- **Power BI Semantic Models** - TMDL/DAX measures
- **AtScale, Cube.dev** - Universal semantic layers
- [Coalesce: Semantic Layers 2025 Playbook](https://coalesce.io/data-insights/semantic-layers-2025-catalog-owner-data-leader-playbook/)

## Decision

Implement a Semantic Layer Module in MDDE that:
1. Defines metrics, dimensions, and hierarchies as first-class metadata objects
2. Links to underlying entities and attributes
3. Exports to multiple BI tools (dbt, Power BI, Looker)
4. Integrates with impact analysis

### Core Concepts

```
┌─────────────────────────────────────────────────────────┐
│                   Semantic Model                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   Metrics    │  │  Dimensions  │  │ Hierarchies  │  │
│  │              │  │              │  │              │  │
│  │ total_revenue│  │ customer     │  │ time_hier    │  │
│  │ order_count  │  │ product      │  │ geo_hier     │  │
│  │ avg_order    │  │ date         │  │ product_hier │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
└─────────┼─────────────────┼─────────────────┼──────────┘
          │                 │                 │
          ▼                 ▼                 ▼
    ┌─────────────────────────────────────────────┐
    │           Physical Entities                  │
    │  fact_orders  dim_customer  dim_product     │
    └─────────────────────────────────────────────┘
```

### Data Model

#### Semantic Model

```python
@dataclass
class SemanticModel:
    """A semantic layer definition (like a dbt semantic model or Looker explore)."""

    model_id: str
    name: str
    description: Optional[str]

    # Ownership
    owner: Optional[str]
    domain: Optional[str]  # e.g., "sales", "finance", "marketing"

    # Components
    metrics: List[Metric]
    dimensions: List[Dimension]
    hierarchies: List[Hierarchy]

    # Configuration
    default_time_dimension: Optional[str]
    default_granularity: Optional[str]  # day, week, month

    # Metadata
    created_at: datetime
    updated_at: datetime
    version: str
```

#### Metric

```python
@dataclass
class Metric:
    """A business metric definition."""

    metric_id: str
    name: str
    description: str

    # Type and expression
    metric_type: MetricType  # SIMPLE, DERIVED, CUMULATIVE, RATIO
    expression: str  # SQL expression or formula

    # Source entity
    entity_id: str
    attribute_id: Optional[str]  # For simple metrics

    # Aggregation
    aggregation: AggregationType  # SUM, COUNT, AVG, MIN, MAX, COUNT_DISTINCT

    # Filters
    filters: List[MetricFilter]

    # Time grain support
    time_grains: List[str]  # ["day", "week", "month", "quarter", "year"]

    # Business metadata
    unit: Optional[str]  # "$", "%", "orders"
    format: Optional[str]  # "#,##0.00"

    # Governance
    owner: Optional[str]
    certified: bool = False
    tags: List[str] = field(default_factory=list)


class MetricType(Enum):
    SIMPLE = "simple"        # Direct aggregation: SUM(revenue)
    DERIVED = "derived"      # Calculated from other metrics: revenue / orders
    CUMULATIVE = "cumulative"  # Running total
    RATIO = "ratio"          # Ratio of two metrics
    CONVERSION = "conversion"  # Funnel conversion rate


class AggregationType(Enum):
    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    PERCENTILE = "percentile"
```

#### Dimension

```python
@dataclass
class Dimension:
    """A dimension for slicing metrics."""

    dimension_id: str
    name: str
    description: Optional[str]

    # Source
    entity_id: str
    attribute_id: str

    # Type
    dimension_type: DimensionType  # CATEGORICAL, TIME, GEOGRAPHIC

    # For time dimensions
    time_granularity: Optional[str]  # day, week, month

    # For geographic dimensions
    geo_type: Optional[str]  # country, region, city

    # Allowed values (optional constraint)
    allowed_values: Optional[List[str]]

    # Display
    label: Optional[str]
    format: Optional[str]


class DimensionType(Enum):
    CATEGORICAL = "categorical"
    TIME = "time"
    GEOGRAPHIC = "geographic"
    NUMERIC = "numeric"  # For numeric ranges/bins
```

#### Hierarchy

```python
@dataclass
class Hierarchy:
    """A drill-down hierarchy for dimensions."""

    hierarchy_id: str
    name: str
    description: Optional[str]

    # Levels (ordered from top to bottom)
    levels: List[HierarchyLevel]

    # Type
    hierarchy_type: HierarchyType  # TIME, GEOGRAPHIC, PRODUCT, ORGANIZATIONAL


@dataclass
class HierarchyLevel:
    """A level in a hierarchy."""

    level_id: str
    name: str
    dimension_id: str  # Links to a dimension
    order: int
```

### Database Schema

```sql
-- Semantic models
CREATE TABLE semantic_model (
    model_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description VARCHAR,
    owner VARCHAR,
    domain VARCHAR,
    default_time_dimension VARCHAR,
    default_granularity VARCHAR,
    version VARCHAR DEFAULT '1.0.0',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mdde_model_id VARCHAR REFERENCES model(model_id)
);

-- Metrics
CREATE TABLE semantic_metric (
    metric_id VARCHAR PRIMARY KEY,
    semantic_model_id VARCHAR REFERENCES semantic_model(model_id),
    name VARCHAR NOT NULL,
    description VARCHAR,
    metric_type VARCHAR NOT NULL,  -- simple, derived, cumulative, ratio
    expression VARCHAR NOT NULL,
    entity_id VARCHAR REFERENCES entity(entity_id),
    attribute_id VARCHAR REFERENCES attribute(attribute_id),
    aggregation VARCHAR,  -- sum, count, avg, etc.
    unit VARCHAR,
    format VARCHAR,
    owner VARCHAR,
    certified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Metric filters
CREATE TABLE semantic_metric_filter (
    filter_id VARCHAR PRIMARY KEY,
    metric_id VARCHAR REFERENCES semantic_metric(metric_id),
    filter_expression VARCHAR NOT NULL,
    description VARCHAR
);

-- Metric time grains
CREATE TABLE semantic_metric_time_grain (
    metric_id VARCHAR REFERENCES semantic_metric(metric_id),
    time_grain VARCHAR NOT NULL,  -- day, week, month, quarter, year
    PRIMARY KEY (metric_id, time_grain)
);

-- Metric tags
CREATE TABLE semantic_metric_tag (
    metric_id VARCHAR REFERENCES semantic_metric(metric_id),
    tag VARCHAR NOT NULL,
    PRIMARY KEY (metric_id, tag)
);

-- Dimensions
CREATE TABLE semantic_dimension (
    dimension_id VARCHAR PRIMARY KEY,
    semantic_model_id VARCHAR REFERENCES semantic_model(model_id),
    name VARCHAR NOT NULL,
    description VARCHAR,
    entity_id VARCHAR REFERENCES entity(entity_id),
    attribute_id VARCHAR REFERENCES attribute(attribute_id),
    dimension_type VARCHAR NOT NULL,  -- categorical, time, geographic
    time_granularity VARCHAR,
    geo_type VARCHAR,
    label VARCHAR,
    format VARCHAR
);

-- Hierarchies
CREATE TABLE semantic_hierarchy (
    hierarchy_id VARCHAR PRIMARY KEY,
    semantic_model_id VARCHAR REFERENCES semantic_model(model_id),
    name VARCHAR NOT NULL,
    description VARCHAR,
    hierarchy_type VARCHAR  -- time, geographic, product, organizational
);

-- Hierarchy levels
CREATE TABLE semantic_hierarchy_level (
    level_id VARCHAR PRIMARY KEY,
    hierarchy_id VARCHAR REFERENCES semantic_hierarchy(hierarchy_id),
    name VARCHAR NOT NULL,
    dimension_id VARCHAR REFERENCES semantic_dimension(dimension_id),
    level_order INTEGER NOT NULL
);

-- Metric dependencies (for derived metrics)
CREATE TABLE semantic_metric_dependency (
    parent_metric_id VARCHAR REFERENCES semantic_metric(metric_id),
    child_metric_id VARCHAR REFERENCES semantic_metric(metric_id),
    PRIMARY KEY (parent_metric_id, child_metric_id)
);
```

### Module Structure

```
src/mdde/semantic/
├── __init__.py
├── types.py           # Enums and dataclasses
├── model.py           # SemanticModel, Metric, Dimension, Hierarchy
├── manager.py         # SemanticLayerManager (CRUD)
├── validator.py       # Validation rules
├── exporter/
│   ├── __init__.py
│   ├── dbt.py         # Export to dbt semantic layer YAML
│   ├── powerbi.py     # Export to Power BI TMDL/DAX
│   ├── looker.py      # Export to LookML
│   └── cube.py        # Export to Cube.js schema
└── importer/
    ├── __init__.py
    ├── dbt.py         # Import from dbt metrics
    └── powerbi.py     # Import from Power BI model
```

### Usage Examples

#### Define a Semantic Model

```python
from mdde.semantic import SemanticLayerManager, Metric, Dimension, Hierarchy

manager = SemanticLayerManager(conn)

# Create semantic model
model = manager.create_model(
    name="Sales Analytics",
    description="Core sales metrics and dimensions",
    domain="sales",
    owner="sales_analytics_team",
)

# Add metrics
manager.add_metric(
    model_id=model.model_id,
    name="Total Revenue",
    description="Sum of all order totals",
    metric_type="simple",
    expression="SUM(order_total)",
    entity_id="ent_fact_orders",
    attribute_id="attr_order_total",
    aggregation="sum",
    unit="$",
    format="#,##0.00",
    time_grains=["day", "week", "month", "quarter", "year"],
    certified=True,
)

manager.add_metric(
    model_id=model.model_id,
    name="Average Order Value",
    description="Average revenue per order",
    metric_type="derived",
    expression="total_revenue / order_count",  # References other metrics
    entity_id="ent_fact_orders",
    aggregation="avg",
    unit="$",
)

# Add dimensions
manager.add_dimension(
    model_id=model.model_id,
    name="Order Date",
    entity_id="ent_dim_date",
    attribute_id="attr_date_key",
    dimension_type="time",
    time_granularity="day",
)

manager.add_dimension(
    model_id=model.model_id,
    name="Customer Segment",
    entity_id="ent_dim_customer",
    attribute_id="attr_segment",
    dimension_type="categorical",
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
```

#### Export to dbt Semantic Layer

```python
from mdde.semantic.exporter import DbtSemanticExporter

exporter = DbtSemanticExporter(conn)
yaml_content = exporter.export_model(model.model_id)

# Output:
# semantic_models:
#   - name: sales_analytics
#     description: Core sales metrics and dimensions
#     defaults:
#       agg_time_dimension: order_date
#
#     entities:
#       - name: orders
#         type: primary
#         expr: fact_orders
#
#     measures:
#       - name: total_revenue
#         description: Sum of all order totals
#         agg: sum
#         expr: order_total
#         create_metric: true
#
#     dimensions:
#       - name: order_date
#         type: time
#         type_params:
#           time_granularity: day
```

#### Export to Power BI

```python
from mdde.semantic.exporter import PowerBISemanticExporter

exporter = PowerBISemanticExporter(conn)
tmdl_content = exporter.export_model(model.model_id)

# Generates TMDL with DAX measures
```

#### Impact Analysis Integration

```python
from mdde.analyzer.impact import ImpactAnalysisManager

# Analyze impact on semantic layer when entity changes
manager = ImpactAnalysisManager(conn)
iam = manager.analyze_entity(
    entity_id="ent_fact_orders",
    direction=Direction.DOWNSTREAM,
    include_semantic=True,  # Include semantic layer objects
)

# Shows affected metrics:
# ent_fact_orders
# ├── fact_orders_summary (entity)
# └── [SEMANTIC] Total Revenue (metric)  ← Now visible!
#     └── [SEMANTIC] Average Order Value (derived metric)
```

### Validation Rules

```python
class SemanticValidator:
    """Validate semantic layer definitions."""

    def validate_metric(self, metric: Metric) -> List[ValidationError]:
        errors = []

        # SM001: Metric must reference valid entity
        if not self._entity_exists(metric.entity_id):
            errors.append(ValidationError("SM001", f"Entity not found: {metric.entity_id}"))

        # SM002: Derived metrics must reference existing metrics
        if metric.metric_type == MetricType.DERIVED:
            for ref in self._extract_metric_references(metric.expression):
                if not self._metric_exists(ref):
                    errors.append(ValidationError("SM002", f"Referenced metric not found: {ref}"))

        # SM003: Aggregation required for simple metrics
        if metric.metric_type == MetricType.SIMPLE and not metric.aggregation:
            errors.append(ValidationError("SM003", "Simple metrics require aggregation type"))

        return errors
```

## Implementation Plan

### Phase 1: Core Module (Week 1)
- [ ] Create `src/mdde/semantic/` module structure
- [ ] Implement types.py with enums and dataclasses
- [ ] Implement model.py with SemanticModel, Metric, Dimension, Hierarchy
- [ ] Add database schema migration
- [ ] Basic unit tests

### Phase 2: Manager & Validation (Week 2)
- [ ] Implement SemanticLayerManager (CRUD operations)
- [ ] Implement SemanticValidator
- [ ] Link to existing entities/attributes
- [ ] More comprehensive tests

### Phase 3: Export (Week 3)
- [ ] dbt Semantic Layer exporter
- [ ] Power BI TMDL exporter
- [ ] Looker LookML exporter
- [ ] Integration tests

### Phase 4: Integration (Week 4)
- [ ] Impact analysis integration (semantic objects in tree)
- [ ] MCP tools for semantic layer
- [ ] Documentation and brochure
- [ ] Demo

## Consequences

### Positive
- **Single source of truth** for metric definitions
- **Consistency** across BI tools and reports
- **Governance** - track metric ownership, certification, usage
- **Impact analysis** extended to semantic layer
- **Export** to multiple BI platforms

### Negative
- Additional database tables
- Learning curve for semantic layer concepts
- Maintenance of exporters as BI tools evolve

## References

- [Patrick Okare: Five Must-Have Layers](https://karetech.medium.com/data-stack-five-must-have-layers-of-a-modern-analytics-platform-126b797d638e)
- [dbt Semantic Layer Documentation](https://docs.getdbt.com/docs/build/semantic-models)
- [Coalesce: Semantic Layers 2025 Playbook](https://coalesce.io/data-insights/semantic-layers-2025-catalog-owner-data-leader-playbook/)
- [Semantic Layer in Data Governance](https://datalakehousehub.com/blog/2026-02-semantic-layer-06-data-governance/)
- [Power BI Semantic Model Best Practices](https://learn.microsoft.com/en-us/power-bi/guidance/star-schema)
