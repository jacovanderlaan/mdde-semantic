# Semantic Layer Demo

> ADR-301: Semantic Layer Module - Inspired by Patrick Okare's "Five Must-Have Layers"

---

## Overview

This demo showcases the **Semantic Layer Module** implemented in MDDE v3.59.0+. The semantic layer provides a business-friendly abstraction over your data model, enabling:

- **Consistent Metrics**: Define business metrics once, use everywhere
- **Self-Service Analytics**: Business users query without SQL knowledge
- **Governed Definitions**: Certified metrics with ownership and lineage
- **Multi-Tool Export**: Deploy to dbt, Power BI, Looker, and more

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Metrics** | Business measures with certified definitions |
| **Dimensions** | Categorical, time, geographic, and numeric axes |
| **Hierarchies** | Drill-down paths (Year > Quarter > Month > Day) |
| **Governance** | Certification, ownership, tagging |
| **Export** | dbt Semantic Layer, Power BI TMDL/DAX, Looker LookML |

---

## Quick Start

### 1. Define a Semantic Model

```python
from mdde.semantic import (
    SemanticModel,
    SemanticMetric,
    SemanticDimension,
    Hierarchy,
    HierarchyLevel,
    SemanticMetricType,
    SemanticAggregationType,
    DimensionType,
    HierarchyType,
    SemanticTimeGrain,
)

# Create a semantic model for sales analytics
model = SemanticModel(
    semantic_model_id="sales_analytics",
    name="Sales Analytics",
    description="Core sales metrics and dimensions",
)

# Add a simple metric
model.add_metric(SemanticMetric(
    metric_id="total_revenue",
    name="Total Revenue",
    description="Sum of all order revenue",
    metric_type=SemanticMetricType.SIMPLE,
    expression="SUM(order_amount)",
    entity_id="ent_orders",
    attribute_id="order_amount",
    aggregation=SemanticAggregationType.SUM,
    unit="$",
    format="#,##0.00",
    owner="finance_team",
    certified=True,
    tags=["revenue", "core-kpi"],
))

# Add a derived metric
model.add_metric(SemanticMetric(
    metric_id="avg_order_value",
    name="Average Order Value",
    description="Total revenue divided by order count",
    metric_type=SemanticMetricType.DERIVED,
    expression="total_revenue / order_count",
    entity_id="ent_orders",
    unit="$",
    tags=["revenue", "per-order"],
))

# Add dimensions
model.add_dimension(SemanticDimension(
    dimension_id="dim_order_date",
    name="Order Date",
    description="Date when order was placed",
    entity_id="ent_orders",
    attribute_id="order_date",
    dimension_type=DimensionType.TIME,
    time_granularity=SemanticTimeGrain.DAY,
))

model.add_dimension(SemanticDimension(
    dimension_id="dim_region",
    name="Region",
    description="Sales region",
    entity_id="ent_orders",
    attribute_id="region",
    dimension_type=DimensionType.GEOGRAPHIC,
    geo_type="region",
))

# Add a time hierarchy
model.add_hierarchy(Hierarchy(
    hierarchy_id="hier_time",
    name="Time Hierarchy",
    hierarchy_type=HierarchyType.TIME,
    levels=[
        HierarchyLevel(level_id="lvl_year", name="Year", dimension_id="dim_order_date", order=0),
        HierarchyLevel(level_id="lvl_quarter", name="Quarter", dimension_id="dim_order_date", order=1),
        HierarchyLevel(level_id="lvl_month", name="Month", dimension_id="dim_order_date", order=2),
        HierarchyLevel(level_id="lvl_day", name="Day", dimension_id="dim_order_date", order=3),
    ],
))

print(f"Model: {model.name}")
print(f"Metrics: {len(model.metrics)}")
print(f"Dimensions: {len(model.dimensions)}")
print(f"Hierarchies: {len(model.hierarchies)}")
```

### 2. Persist with SemanticLayerManager

```python
from mdde.semantic import SemanticLayerManager

manager = SemanticLayerManager(conn)

# Save the model
manager.save_model(model)

# Retrieve metrics
revenue = manager.get_metric("total_revenue")
print(f"Metric: {revenue.name} - {revenue.description}")

# List certified metrics
certified = manager.list_metrics(certified_only=True)
print(f"Certified metrics: {len(certified)}")

# Get metrics by tag
revenue_metrics = manager.get_metrics_by_tag("revenue")
```

### 3. Export to BI Tools

```python
from mdde.semantic.exporter import dbt, powerbi, looker

# Export to dbt Semantic Layer
dbt_yaml = dbt.export_semantic_model(model)

# Export to Power BI (TMDL format)
tmdl = powerbi.export_to_tmdl(model)

# Export to Looker (LookML)
lookml = looker.export_to_lookml(model)
```

---

## Metric Types

| Type | Description | Example |
|------|-------------|---------|
| `SIMPLE` | Direct aggregation | `SUM(revenue)` |
| `DERIVED` | Calculated from other metrics | `revenue / orders` |
| `CUMULATIVE` | Running total | Running sum of daily revenue |
| `RATIO` | Ratio of two metrics | `new_customers / total_customers` |
| `CONVERSION` | Funnel conversion rate | `purchases / visits` |

---

## Dimension Types

| Type | Description | Example |
|------|-------------|---------|
| `CATEGORICAL` | Discrete values | Product Category, Status |
| `TIME` | Date/time with granularity | Order Date, Created At |
| `GEOGRAPHIC` | Location-based | Country, Region, City |
| `NUMERIC` | Numeric ranges/bins | Age Range, Price Tier |

---

## Hierarchy Types

| Type | Description | Example |
|------|-------------|---------|
| `TIME` | Temporal drill-down | Year > Quarter > Month > Day |
| `GEOGRAPHIC` | Location drill-down | Country > Region > City |
| `PRODUCT` | Product drill-down | Category > Subcategory > Product |
| `ORGANIZATIONAL` | Org structure | Division > Department > Team |
| `CUSTOM` | User-defined | Any custom hierarchy |

---

## Demo Scripts

| Script | Description |
|--------|-------------|
| [01_basic_model.py](scripts/01_basic_model.py) | Create a basic semantic model |
| [02_metrics_gallery.py](scripts/02_metrics_gallery.py) | Different metric types and formulas |
| [03_dimensions_hierarchies.py](scripts/03_dimensions_hierarchies.py) | Dimensions and drill-down hierarchies |
| [04_governance.py](scripts/04_governance.py) | Certification, ownership, tagging |
| [05_export_dbt.py](scripts/05_export_dbt.py) | Export to dbt Semantic Layer |
| [06_export_powerbi.py](scripts/06_export_powerbi.py) | Export to Power BI TMDL |

---

## Sample Models

| Model | Description |
|-------|-------------|
| [sales_analytics.yaml](models/sales_analytics.yaml) | E-commerce sales metrics |
| [customer_360.yaml](models/customer_360.yaml) | Customer analytics |
| [financial_kpis.yaml](models/financial_kpis.yaml) | Financial reporting |

---

## Architecture

```
+------------------+     +-------------------+     +------------------+
|   Entity Model   |     |   Semantic Layer  |     |   BI Tools       |
|   (Physical)     |---->|   (Logical)       |---->|   (Consumers)    |
+------------------+     +-------------------+     +------------------+
        |                        |                        |
        v                        v                        v
+------------------+     +-------------------+     +------------------+
| Tables, Columns  |     | Metrics,          |     | dbt Semantic     |
| Relationships    |     | Dimensions,       |     | Power BI         |
| Joins            |     | Hierarchies       |     | Looker           |
+------------------+     +-------------------+     +------------------+
```

The semantic layer bridges the gap between technical data models and business analytics needs.

---

## Governance Features

### Certification

```python
metric = SemanticMetric(
    metric_id="revenue",
    name="Total Revenue",
    certified=True,  # Mark as certified
    owner="finance_team",  # Assign owner
    tags=["core-kpi", "audited"],  # Add tags
    ...
)
```

### Ownership Tracking

```python
# Find metrics owned by a team
finance_metrics = manager.list_metrics(owner="finance_team")

# Update ownership
manager.update_metric_owner("revenue", "new_owner")
```

### Tagging

```python
# Find metrics by tag
revenue_metrics = manager.get_metrics_by_tag("revenue")
kpis = manager.get_metrics_by_tag("core-kpi")

# Add tags
manager.add_metric_tags("revenue", ["quarterly-report", "board-deck"])
```

---

## Export Formats

### dbt Semantic Layer

```yaml
semantic_models:
  - name: sales_analytics
    model: ref('fct_orders')

    entities:
      - name: order
        type: primary
        expr: order_id

    measures:
      - name: total_revenue
        expr: order_amount
        agg: sum

    dimensions:
      - name: order_date
        type: time
        expr: order_date
        time_grains: [day, week, month, quarter, year]
```

### Power BI TMDL

```json
{
  "model": {
    "name": "Sales Analytics",
    "tables": [...],
    "measures": [
      {
        "name": "Total Revenue",
        "expression": "SUM('Orders'[order_amount])",
        "formatString": "$#,##0.00"
      }
    ]
  }
}
```

### Looker LookML

```yaml
view: sales_analytics {
  measure: total_revenue {
    type: sum
    sql: ${order_amount} ;;
    value_format: "$#,##0.00"
    description: "Sum of all order revenue"
  }

  dimension: order_date {
    type: time
    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}.order_date ;;
  }
}
```

---

## Best Practices

### 1. Start with Core KPIs

Define the 5-10 most important business metrics first:

```python
core_kpis = [
    "total_revenue",
    "order_count",
    "avg_order_value",
    "customer_count",
    "conversion_rate",
]
```

### 2. Certify Before Deployment

Only deploy certified metrics to production BI tools:

```python
# Mark as certified after review
metric.certified = True
metric.owner = "data_team"
manager.save_metric(metric)
```

### 3. Use Consistent Naming

- Metrics: `total_*`, `avg_*`, `count_*`, `pct_*`
- Dimensions: `dim_*`
- Hierarchies: `hier_*`

### 4. Document Everything

```python
metric = SemanticMetric(
    name="Gross Margin",
    description="Revenue minus COGS, divided by revenue. Excludes operating expenses.",
    # ... detailed description helps self-service users
)
```

### 5. Test Derived Metrics

Ensure derived metrics match their component definitions:

```python
# Derived: avg_order_value = total_revenue / order_count
# Test: verify consistency across dimensions
```

---

## Database Schema

The semantic layer uses these tables:

```sql
-- Semantic models
CREATE TABLE semantic_model (
    semantic_model_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Metrics
CREATE TABLE semantic_metric (
    metric_id VARCHAR PRIMARY KEY,
    semantic_model_id VARCHAR,
    name VARCHAR NOT NULL,
    description TEXT,
    metric_type VARCHAR,
    expression TEXT,
    entity_id VARCHAR,
    attribute_id VARCHAR,
    aggregation VARCHAR,
    unit VARCHAR,
    format VARCHAR,
    owner VARCHAR,
    certified BOOLEAN,
    FOREIGN KEY (semantic_model_id) REFERENCES semantic_model(semantic_model_id)
);

-- Dimensions
CREATE TABLE semantic_dimension (
    dimension_id VARCHAR PRIMARY KEY,
    semantic_model_id VARCHAR,
    name VARCHAR NOT NULL,
    dimension_type VARCHAR,
    entity_id VARCHAR,
    attribute_id VARCHAR,
    time_granularity VARCHAR,
    FOREIGN KEY (semantic_model_id) REFERENCES semantic_model(semantic_model_id)
);

-- Hierarchies and levels
CREATE TABLE semantic_hierarchy (...);
CREATE TABLE semantic_hierarchy_level (...);

-- Tags
CREATE TABLE semantic_metric_tag (...);
```

---

## Further Reading

- [ADR-301: Semantic Layer Module](../../docs/adrs/ADR-301-semantic-layer.md)
- [Patrick Okare: Five Must-Have Layers](https://medium.com/@patrickokare/...)
- [Semantic Layer Documentation](../../docs/instructions/core/SEMANTIC_LAYER.md)

---

*Demo created for MDDE v3.59.0+ - Semantic Layer Module*
