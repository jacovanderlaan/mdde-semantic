# Building a BI-Agnostic Semantic Layer: The MDDE Approach

*How to centralize business metrics and serve them to any analytics tool*

---

## The Metric Drift Problem

Picture this scenario. Your company uses Power BI for executive dashboards, Tableau for marketing analytics, Python notebooks for data science, and custom APIs for customer-facing analytics. Each team calculates "revenue" slightly differently:

- **Power BI**: `SUM(orders.amount)`
- **Tableau**: `SUM(orders.amount) - SUM(refunds.amount)`
- **Python**: `SUM(CASE WHEN status != 'cancelled' THEN amount END)`
- **API**: Same as Python, but with a different date filter

The CFO opens two reports and sees two different numbers. Nobody is technically wrong—they just have different interpretations. This is **metric drift**, and it erodes trust in data faster than any data quality issue.

The solution isn't standardizing on one BI tool. That ship has sailed. Organizations use multiple tools because each serves different needs. The solution is a **semantic layer** that sits between the data warehouse and all consuming tools.

---

## What Makes a Good Semantic Layer?

After analyzing tools like Cube, dbt Semantic Layer (MetricFlow), Malloy, and others, several architectural patterns emerge:

### 1. Tool-Agnostic by Design

The semantic layer shouldn't live inside any BI tool. It should be an independent service or specification that any tool can consume. Define revenue once, use it everywhere.

### 2. Metrics as Code

Metric definitions should be version-controlled, reviewed, and tested like any other code. When the finance team proposes a change to the margin formula, it goes through a pull request, not a ticket to "update the dashboard."

### 3. SQL Generation, Not Replacement

The semantic layer translates business concepts into SQL. It doesn't replace SQL—it generates optimized, consistent SQL that runs in your existing warehouse. No new execution engine to manage.

### 4. AI-Ready Interface

As AI agents increasingly interact with data, they need a structured interface that explains what metrics mean and how to query them. The semantic layer becomes the "API" between AI and data.

---

## The MDDE Semantic Layer

MDDE (Metadata-Driven Data Engineering) implements the semantic layer as a core architectural component, not an afterthought. Here's how it works.

### Metric Definitions

Metrics in MDDE are first-class objects with rich metadata:

```python
from mdde.semantic import MetricsManager, MetricDefinition, MetricType

manager = MetricsManager(conn)

# Define a metric
manager.define_metric(MetricDefinition(
    metric_id="MET_revenue",
    metric_name="revenue",
    display_name="Revenue",
    description="Total revenue after refunds and cancellations",
    metric_type=MetricType.DERIVED,
    entity_id="ENT_order",
    expression="SUM(order_amount) - SUM(refund_amount)",
    filter_expression="order_status != 'cancelled'",
    time_grain="day",
    time_attribute_id="ATT_order_date",
    business_owner="finance-team",
    domain="sales",
    unit="USD",
    format_string="${:,.2f}",
    tags=["kpi", "finance", "certified"],
))
```

This single definition captures everything: the calculation, the filters, the owner, the formatting. Any tool querying this metric gets the same answer.

### Dimensions and Hierarchies

Metrics live in a dimensional context. MDDE supports full hierarchy definitions:

```python
from mdde.semantic import SemanticLayerManager, Dimension, Hierarchy

semantic = SemanticLayerManager(conn)

# Define a dimension
semantic.register_dimension(Dimension(
    dimension_id="DIM_geography",
    name="geography",
    description="Geographic hierarchy for sales analysis",
    entity_id="ENT_store",
    dimension_type="categorical",
))

# Define hierarchy (drill path)
semantic.register_hierarchy(Hierarchy(
    hierarchy_id="HIE_geo",
    name="Geographic Hierarchy",
    dimension_id="DIM_geography",
    levels=[
        HierarchyLevel(level_id="L1", name="region", attribute_id="ATT_region"),
        HierarchyLevel(level_id="L2", name="country", attribute_id="ATT_country"),
        HierarchyLevel(level_id="L3", name="city", attribute_id="ATT_city"),
    ],
))
```

Now when a user drills from Region → Country → City, every tool follows the same path.

### Semantic Models

A semantic model combines metrics, dimensions, and hierarchies into a consumable unit:

```python
from mdde.semantic import SemanticModel

model = SemanticModel(
    model_id="SEM_sales",
    name="Sales Analytics",
    description="Sales metrics for revenue and order analysis",
    owner="analytics-team",
    domain="sales",
    default_time_dimension="DIM_date",
    default_granularity="month",
    mdde_model_id="MDL_sales",  # Links to data model
)

semantic.register_model(model)
semantic.add_metric_to_model("SEM_sales", "MET_revenue")
semantic.add_dimension_to_model("SEM_sales", "DIM_geography")
semantic.add_hierarchy_to_model("SEM_sales", "HIE_geo")
```

---

## Querying the Semantic Layer

Once defined, metrics can be queried through a semantic interface:

```python
from mdde.semantic.query import SemanticQueryEngine

engine = SemanticQueryEngine(conn, "SEM_sales")

# Business-level query
result = engine.query(
    metrics=["revenue", "order_count", "average_order_value"],
    dimensions=["region", "product_category"],
    filters=[
        {"dimension": "date", "operator": ">=", "value": "2026-01-01"},
        {"dimension": "customer_segment", "operator": "=", "value": "enterprise"},
    ],
    time_grain="month",
)
```

The engine generates optimized SQL:

```sql
SELECT
    DATE_TRUNC('month', o.order_date) AS date_month,
    o.region,
    o.product_category,
    SUM(o.order_amount) - SUM(COALESCE(r.refund_amount, 0)) AS revenue,
    COUNT(*) AS order_count,
    (SUM(o.order_amount) - SUM(COALESCE(r.refund_amount, 0))) / NULLIF(COUNT(*), 0) AS average_order_value
FROM orders o
LEFT JOIN refunds r ON o.order_id = r.order_id
WHERE o.order_status != 'cancelled'
    AND o.order_date >= '2026-01-01'
    AND o.customer_segment = 'enterprise'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
```

Every tool—BI dashboard, Python notebook, API service—generates identical SQL from the same semantic query.

---

## Metrics as Code

MDDE supports defining metrics in YAML for version control:

```yaml
# metrics/revenue.yaml
metric:
  id: MET_revenue
  name: revenue
  display_name: Revenue
  description: "{{ doc('revenue') }}"
  type: derived

  expression: "SUM(order_amount) - SUM(refund_amount)"
  entity_id: ENT_order

  filters:
    - id: exclude_cancelled
      expression: "order_status != 'cancelled'"
      is_default: true

  dimensions:
    - id: DIM_date
      role: time
      time_grains: [day, week, month, quarter, year]

    - id: DIM_geography
      role: group_by
      hierarchy_id: HIE_geo

  goals:
    - id: GOAL_monthly
      period: month
      target: 1000000
      alert_threshold: 0.9

  certified: true
  owner: finance-team
  domain: sales
  tags: [kpi, finance, monthly-review]
```

This YAML lives in Git. Changes require pull requests. CI validates that metrics still compile. Auditors can trace who changed what and when.

```python
from mdde.semantic.metrics_code import MetricCodeSync

# Sync YAML definitions to database
sync = MetricCodeSync(conn)
result = sync.import_from_yaml("metrics/")

print(f"Imported: {result.created} new, {result.updated} updated")
print(f"Errors: {result.errors}")
```

---

## Integration Patterns

### Export to Cube.js

```python
from mdde.semantic.integrations.cube import CubeIntegration

cube = CubeIntegration()
cube.export_model(conn, "SEM_sales", output_dir="cube/schema")
```

**Generated**:
```javascript
cube(`Revenue`, {
  measures: {
    revenue: {
      type: `sum`,
      sql: `${order_amount} - ${refund_amount}`,
      filters: [{ sql: `${status} != 'cancelled'` }],
    },
  },
  dimensions: {
    region: { type: `string`, sql: `region` },
  },
});
```

### Export to dbt MetricFlow

```python
from mdde.semantic.integrations.metricflow import MetricFlowIntegration

mf = MetricFlowIntegration()
mf.export_metrics(conn, "SEM_sales", output_dir="semantic_models/")
```

**Generated**:
```yaml
metrics:
  - name: revenue
    type: simple
    type_params:
      measure: revenue
    filter: |
      {{ Dimension('order__status') }} != 'cancelled'
```

### Export to LookML

```python
from mdde.semantic.integrations.looker import LookMLIntegration

lookml = LookMLIntegration()
lookml.export_model(conn, "SEM_sales", output_dir="looker/views/")
```

**Generated**:
```lookml
measure: revenue {
  type: sum
  sql: ${order_amount} - ${refund_amount} ;;
  filters: [status: "-cancelled"]
}
```

### Export to Power BI

```python
from mdde.semantic.integrations.powerbi import PowerBISemanticIntegration

pbi = PowerBISemanticIntegration()
pbi.export_to_tmdl(conn, "SEM_sales", output_dir="powerbi/semantic/")
```

One semantic model, multiple target formats.

---

## Pre-Aggregation Optimization

Semantic queries can be expensive. MDDE includes a pre-aggregation advisor:

```python
from mdde.semantic.preagg import PreAggregationAdvisor

advisor = PreAggregationAdvisor(conn)

# Analyze last 30 days of query patterns
recommendations = advisor.analyze_query_log(days=30)

for rec in recommendations:
    print(f"Metric: {rec.metric_id}")
    print(f"  Dimensions: {rec.dimensions}")
    print(f"  Time grain: {rec.time_grain}")
    print(f"  Estimated speedup: {rec.estimated_speedup}x")
    print(f"  Storage cost: {rec.estimated_size_mb} MB")
```

Output:
```
Metric: MET_revenue
  Dimensions: ['region', 'product_category']
  Time grain: month
  Estimated speedup: 15x
  Storage cost: 12.5 MB

Metric: MET_order_count
  Dimensions: ['customer_segment']
  Time grain: week
  Estimated speedup: 8x
  Storage cost: 3.2 MB
```

Create the pre-aggregation:

```python
from mdde.semantic.preagg import PreAggregationManager

preagg = PreAggregationManager(conn)

preagg.define(
    preagg_id="PREAGG_revenue_monthly",
    metric_id="MET_revenue",
    dimensions=["region", "product_category"],
    time_grain="month",
    refresh_schedule="0 3 * * *",  # Daily at 3am
)

# Generate DDL for your warehouse
ddl = preagg.generate_ddl("PREAGG_revenue_monthly", dialect="snowflake")
print(ddl)
```

---

## AI Agent Interface

The semantic layer becomes the interface for AI agents:

```python
from mdde.semantic.agent import SemanticAgentInterface

interface = SemanticAgentInterface(conn, "SEM_sales")

# AI agent asks a question
response = interface.process_question(
    "What was revenue by region last quarter?"
)

# Structured interpretation
print(response.interpretation)
# {
#   "metrics": ["revenue"],
#   "dimensions": ["region"],
#   "time_period": "2026-Q1",
#   "time_grain": "quarter"
# }

# Generated SQL
print(response.sql)

# Natural language answer
print(response.answer)
# "Revenue by region for Q1 2026:
#  - EMEA: $2.3M (+12% vs Q4 2025)
#  - APAC: $1.8M (+8%)
#  - Americas: $3.1M (+15%)"
```

The AI doesn't guess what "revenue" means. It queries the semantic layer, which returns a precise definition and generates correct SQL.

---

## The Architecture

Here's how MDDE's semantic layer fits in the data platform:

```
               Data Platform Architecture with Semantic Layer
    ================================================================

    ┌─────────────────────────────────────────────────────────────┐
    │                    Data Sources                              │
    │   ERP    CRM    SaaS APIs    Files    Streaming             │
    └─────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    Data Warehouse                            │
    │   Raw Tables    Staging    Data Vault    Marts              │
    └─────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    MDDE Semantic Layer                       │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
    │  │   Metrics   │  │  Dimensions │  │ Hierarchies │         │
    │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘         │
    │         └────────────────┼────────────────┘                 │
    │                          │                                   │
    │  ┌────────────────┬──────┼──────┬────────────────┐          │
    │  │                │      │      │                │          │
    │  ▼                ▼      ▼      ▼                ▼          │
    │  Query        Tool    Pre-Agg   AI         Metrics-as-     │
    │  Engine      Export   Advisor  Interface     Code           │
    └──────────────────────────┬──────────────────────────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         │                     │                     │
         ▼                     ▼                     ▼
    ┌─────────┐         ┌─────────────┐        ┌─────────┐
    │BI Tools │         │  Notebooks  │        │AI Agents│
    │         │         │             │        │         │
    │Power BI │         │   Python    │        │ Claude  │
    │Tableau  │         │   R         │        │ GPT     │
    │Looker   │         │   Julia     │        │ Custom  │
    └─────────┘         └─────────────┘        └─────────┘
```

---

## Comparison with Other Tools

| Capability | Cube.js | MetricFlow | MDDE |
|------------|---------|------------|------|
| **Architecture** | Headless service | SQL generation | Hybrid |
| **Storage** | Separate service | dbt models | Metadata DB |
| **Pre-aggregations** | Built-in | Via dbt | Advisor + DDL |
| **BI integrations** | Via API | SQL | Export to native formats |
| **AI interface** | Limited | Limited | First-class |
| **Version control** | YAML/JS | YAML | YAML + DB sync |
| **Data modeling** | Separate | Via dbt | Integrated |

MDDE's approach is **hybrid**: it stores semantic definitions in a metadata database (enabling queries, lineage, governance) while supporting export to YAML for version control and to native formats for each BI tool.

---

## Getting Started

### Step 1: Define Your First Metric

```python
from mdde.semantic import MetricsManager

manager = MetricsManager(conn)

manager.define_metric(
    metric_id="MET_revenue",
    metric_name="revenue",
    expression="SUM(order_amount)",
    entity_id="ENT_order",
    aggregation="SUM",
)
```

### Step 2: Add Dimensions

```python
manager.add_dimension(
    metric_id="MET_revenue",
    dimension_id="DIM_region",
    attribute_id="ATT_region",
    role="group_by",
)
```

### Step 3: Query

```python
result = engine.query(
    metrics=["revenue"],
    dimensions=["region"],
    time_grain="month",
)
print(result.sql)
```

### Step 4: Export to Your BI Tool

```python
from mdde.semantic.integrations.powerbi import PowerBISemanticIntegration

pbi = PowerBISemanticIntegration()
pbi.export_to_tmdl(conn, "SEM_sales", "powerbi/")
```

---

## Conclusion

The semantic layer isn't a nice-to-have anymore. In a world of multiple BI tools, Python notebooks, AI agents, and embedded analytics, it's the only way to ensure everyone speaks the same data language.

MDDE's semantic layer provides:

1. **Single Source of Truth**: Define metrics once, use everywhere
2. **Version Control**: Metrics as code with proper governance
3. **Tool Flexibility**: Export to Cube, MetricFlow, LookML, Power BI
4. **AI Readiness**: Structured interface for automated reasoning
5. **Performance**: Pre-aggregation advisor for optimization

The future of analytics isn't choosing one BI tool. It's building a semantic layer that serves them all.

---

*This article is part of the MDDE documentation series. MDDE is a metadata-driven data engineering framework that brings software engineering practices to data warehouse development.*

**Related Reading:**
- [DRY Documentation: Doc Blocks for Data Warehouses](./2026-03-13_DRY-Documentation-Doc-Blocks-for-Data-Warehouses.md)
- [Source Delivery Properties: When Is My Data Ready?](./2026-03-10_Source-Delivery-Properties-When-Is-My-Data-Ready.md)
- [Best Open-Source Semantic Layer Tools in 2026](https://medium.com/@sergeygromov) - Sergey Gromov

**References:**
- [Cube Documentation](https://cube.dev/docs)
- [dbt Semantic Layer](https://docs.getdbt.com/docs/build/about-metricflow)
- [Patrick Okare: Five Must-Have Layers](https://medium.com/@patrickt.okare)
