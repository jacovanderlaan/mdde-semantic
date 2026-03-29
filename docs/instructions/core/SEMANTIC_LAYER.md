# Semantic Layer Module (ADR-301)

The Semantic Layer module provides a consistent, unified view of business metrics across the organization. It acts as the "translator" between raw data and meaningful business insights.

**Inspired by**: Patrick Okare's "Five Must-Have Layers of a Modern Analytics Platform" - specifically the Analytics & Consumption Layer.

## Why a Semantic Layer?

Without a semantic layer, organizations face:
1. **Metric inconsistency**: Revenue calculated differently in sales vs. finance reports
2. **Duplicated logic**: Same calculations hardcoded in multiple dashboards
3. **No single source of truth**: Business users don't know which metric to trust
4. **Difficult governance**: Can't track where metrics are used

## Core Concepts

```
+-------------------------------------------------------+
|                   Semantic Model                       |
|  +-------------+  +-------------+  +----------------+ |
|  |   Metrics   |  | Dimensions  |  |  Hierarchies   | |
|  |             |  |             |  |                | |
|  | total_rev   |  | customer    |  | time_hier      | |
|  | order_count |  | product     |  | geo_hier       | |
|  | avg_order   |  | date        |  | product_hier   | |
|  +------+------+  +------+------+  +-------+--------+ |
+---------|--------------|--------------------|----------+
          |              |                    |
          v              v                    v
    +-------------------------------------------+
    |           Physical Entities                |
    |  fact_orders  dim_customer  dim_product   |
    +-------------------------------------------+
```

## Module Structure

```
src/mdde/semantic/
+-- __init__.py          # Module exports
+-- types.py             # Enums and dataclasses
+-- model.py             # SemanticModel class
+-- manager.py           # SemanticLayerManager (CRUD)
+-- exporter/
    +-- __init__.py
    +-- dbt.py           # Export to dbt Semantic Layer
    +-- powerbi.py       # Export to Power BI TMDL/DAX
    +-- looker.py        # Export to LookML
```

## Key Types

### MetricType
- `SIMPLE`: Direct aggregation (e.g., `SUM(revenue)`)
- `DERIVED`: Calculated from other metrics (e.g., `revenue / orders`)
- `CUMULATIVE`: Running total
- `RATIO`: Ratio of two metrics
- `CONVERSION`: Funnel conversion rate

### AggregationType
- `SUM`, `COUNT`, `COUNT_DISTINCT`
- `AVG`, `MIN`, `MAX`
- `MEDIAN`, `PERCENTILE`

### DimensionType
- `CATEGORICAL`: String dimensions (product category, region)
- `TIME`: Date/time dimensions
- `GEOGRAPHIC`: Location-based dimensions
- `NUMERIC`: Numeric range dimensions

### HierarchyType
- `TIME`: Year > Quarter > Month > Day
- `GEOGRAPHIC`: Country > Region > City
- `PRODUCT`: Category > Subcategory > Product
- `ORGANIZATIONAL`: Company > Department > Team
- `CUSTOM`: User-defined hierarchies

## Usage Examples

### Create a Semantic Model

```python
from mdde.semantic import SemanticLayerManager

manager = SemanticLayerManager(conn)

# Create model
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
    expression="SUM(order_total)",
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
)

manager.add_dimension(
    model_id=model.model_id,
    name="Customer Segment",
    entity_id="dim_customer",
    attribute_id="segment",
    dimension_type="categorical",
    allowed_values=["Enterprise", "SMB", "Consumer"],
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

### Export to dbt Semantic Layer

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
#
#     measures:
#       - name: total_revenue_measure
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

### Export to Power BI

```python
from mdde.semantic.exporter import PowerBISemanticExporter

exporter = PowerBISemanticExporter(conn)

# Export TMDL
tmdl = exporter.export_model(model.model_id)

# Export DAX measures
dax = exporter.export_dax_measures(model.model_id)
# Output:
# Total_Revenue = SUM('fact_orders'[order_total])
# Order_Count = COUNT('fact_orders'[order_id])
# Average_Order_Value = DIVIDE(Total_Revenue, Order_Count, 0)

# Export dataset JSON
dataset = exporter.export_pbix_dataset(model.model_id)
```

### Export to Looker LookML

```python
from mdde.semantic.exporter import LookerSemanticExporter

exporter = LookerSemanticExporter(conn)

# Full export
lookml = exporter.export_model(model.model_id)

# Just explore
explore = exporter.export_explore(model.model_id)

# Individual views
views = exporter.export_views(model.model_id)
```

## Querying Semantic Models

### Find metrics by entity

```python
# Find all metrics that use a specific entity
metrics = manager.get_metrics_by_entity("fact_orders")
# Returns metrics that would be impacted by entity changes
```

### Find certified metrics

```python
# Get all certified (governance-approved) metrics
certified = manager.get_certified_metrics()
certified = manager.get_certified_metrics(model_id="sem_sales")  # Filter by model
```

### Get model statistics

```python
model = manager.get_model(model_id)
stats = model.get_statistics()
# {
#   "total_metrics": 5,
#   "certified_metrics": 3,
#   "total_dimensions": 8,
#   "time_dimensions": 2,
#   "total_hierarchies": 2,
#   "unique_entities": 4,
#   "tags": ["revenue", "certified", "growth"]
# }
```

## Database Schema

The semantic layer uses the following tables:

| Table | Description |
|-------|-------------|
| `semantic_model` | Semantic model definitions |
| `semantic_metric` | Metric definitions |
| `semantic_metric_filter` | Filters for metrics |
| `semantic_metric_time_grain` | Supported time grains |
| `semantic_metric_tag` | Metric tags |
| `semantic_dimension` | Dimension definitions |
| `semantic_dimension_value` | Allowed values for dimensions |
| `semantic_hierarchy` | Hierarchy definitions |
| `semantic_hierarchy_level` | Hierarchy levels |
| `semantic_metric_dependency` | Dependencies between derived metrics |

## Integration with MDDE

### Link to Entities

Metrics and dimensions reference MDDE entities via `entity_id` and `attribute_id`:

```python
metric = manager.add_metric(
    model_id=model.model_id,
    name="Revenue",
    entity_id="ent_fact_orders",      # Links to MDDE entity
    attribute_id="attr_order_total",  # Links to MDDE attribute
    ...
)
```

### Impact Analysis

When an entity changes, find affected semantic objects:

```python
# Find metrics impacted by entity change
metrics = manager.get_metrics_by_entity("ent_fact_orders")
for m in metrics:
    print(f"Metric {m['name']} in model {m['model_name']} is affected")
```

## Governance Features

### Metric Certification

Mark metrics as certified (governance-approved):

```python
manager.add_metric(
    ...,
    certified=True,
    owner="finance_team",
    tags=["certified", "audited"],
)
```

### Metric Filters

Add filters to constrain metric calculations:

```python
manager.add_metric(
    ...,
    filters=[
        {"expression": "status = 'completed'", "description": "Only completed orders"},
        {"expression": "region != 'test'", "description": "Exclude test region"},
    ],
)
```

### Time Grain Support

Specify which time grains a metric supports:

```python
manager.add_metric(
    ...,
    time_grains=["day", "week", "month", "quarter", "year"],
)
```

## Best Practices

1. **Certify key metrics**: Mark governance-approved metrics as certified
2. **Use descriptive names**: Metric names should be business-friendly
3. **Document expressions**: Explain complex calculations in descriptions
4. **Link to entities**: Always link metrics to underlying MDDE entities
5. **Define hierarchies**: Create drill-down paths for analysis
6. **Tag consistently**: Use tags for categorization and discovery

## References

- [Patrick Okare: Five Must-Have Layers](https://karetech.medium.com/data-stack-five-must-have-layers-of-a-modern-analytics-platform-126b797d638e)
- [dbt Semantic Layer Documentation](https://docs.getdbt.com/docs/build/semantic-models)
- [Power BI Semantic Model Best Practices](https://learn.microsoft.com/en-us/power-bi/guidance/star-schema)
- [Looker LookML Overview](https://cloud.google.com/looker/docs/lookml-overview)
