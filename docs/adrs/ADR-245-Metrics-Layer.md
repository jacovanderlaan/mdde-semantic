# ADR-245: Metrics Layer

## Status
Accepted

## Date
2026-02-15

## Context

Business users need semantic access to metrics without writing SQL. A metrics layer provides business metric definitions that translate to SQL at query time, enabling self-service analytics and consistent metric calculations.

### Requirements
1. Define business metrics with semantic context
2. Support dimensions for slicing/grouping
3. Predefined filters for common use cases
4. Semantic query to SQL generation
5. Metric goals and alerting
6. Derived/calculated metrics

## Decision

### 1. Metrics Module

Location: `src/mdde/semantic/metrics/`

**Components**:
- `models.py` - Metric data models
- `manager.py` - MetricsManager for CRUD and SQL generation
- `__init__.py` - Module exports

### 2. Metric Types

```python
class MetricType(Enum):
    SIMPLE = "simple"           # Direct aggregation
    DERIVED = "derived"         # From other metrics
    RATIO = "ratio"             # Ratio of two metrics
    CUMULATIVE = "cumulative"   # Running total
    PERIOD_OVER_PERIOD = "period_over_period"
    WINDOW = "window"           # Window function
```

### 3. Aggregation Types

```python
class AggregationType(Enum):
    SUM = "SUM"
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    MEDIAN = "MEDIAN"
    PERCENTILE = "PERCENTILE"
    STDDEV = "STDDEV"
    VARIANCE = "VARIANCE"
```

### 4. Time Granularity

```python
class TimeGrain(Enum):
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"
```

### 5. Usage Examples

**Define Metrics**:
```python
from mdde.semantic import (
    MetricsManager, MetricDefinition, MetricType,
    AggregationType, MetricDimension, DimensionRole
)

manager = MetricsManager(conn)

# Simple metric
revenue_id = manager.create_metric(MetricDefinition(
    metric_id="metric_total_revenue",
    metric_name="total_revenue",
    display_name="Total Revenue",
    description="Sum of all order revenue",
    metric_type=MetricType.SIMPLE,
    entity_id="order_fact",
    attribute_id="order_amount",
    aggregation=AggregationType.SUM,
    unit="USD",
    format_string="$#,##0.00",
    domain="Sales",
))
```

**Add Dimensions**:
```python
manager.add_dimension(MetricDimension(
    dimension_id="dim_region",
    metric_id="metric_total_revenue",
    attribute_id="customer_region",
    dimension_name="region",
    display_name="Region",
    role=DimensionRole.GROUP_BY,
))
```

**Add Predefined Filters**:
```python
from mdde.semantic import MetricFilter

manager.add_filter(MetricFilter(
    filter_id="filter_ytd",
    metric_id="metric_total_revenue",
    filter_name="ytd",
    display_name="Year to Date",
    filter_expression="order_date >= DATE_TRUNC('year', CURRENT_DATE)",
))
```

**Semantic Query**:
```python
from mdde.semantic import MetricQuery, TimeGrain

query = MetricQuery(
    metric_ids=["metric_total_revenue"],
    dimensions=["region", "product_category"],
    filters={"country": "USA"},
    time_range={"start": "2026-01-01", "end": "2026-12-31"},
    time_grain=TimeGrain.MONTH,
    order_by=["total_revenue DESC"],
    limit=100,
)

# Generate SQL
sql = manager.generate_sql(query)
print(sql)
# SELECT
#     ord.region,
#     ord.product_category,
#     SUM(ord.order_amount) AS total_revenue
# FROM sales.order_fact ord
# WHERE country = 'USA'
#   AND order_date >= '2026-01-01'
#   AND order_date < '2026-12-31'
# GROUP BY 1, 2
# ORDER BY total_revenue DESC
# LIMIT 100
```

**Execute Query**:
```python
result = manager.execute_query(query)

print(f"Rows: {result.row_count}")
print(f"Execution: {result.execution_time_ms}ms")
for row in result.data:
    print(row)
```

### 6. Derived Metrics

```python
# Average order value = Revenue / Order Count
aov = MetricDefinition(
    metric_id="metric_aov",
    metric_name="average_order_value",
    metric_type=MetricType.DERIVED,
    expression="total_revenue / order_count",
    depends_on_metrics=["metric_total_revenue", "metric_order_count"],
)
```

### 7. Metric Goals

```python
from mdde.semantic import MetricGoal

manager.add_goal(MetricGoal(
    goal_id="goal_q1_revenue",
    metric_id="metric_total_revenue",
    goal_name="Q1 2026 Revenue Target",
    target_value=10000000,
    comparison="gte",
    time_period="2026-Q1",
))
```

### 8. Database Tables

| Table | Purpose |
|-------|---------|
| `metric_def` | Metric definitions |
| `metric_dimension` | Available dimensions |
| `metric_filter` | Predefined filters |
| `metric_goal` | Metric targets |

## Consequences

### Positive
- Self-service analytics
- Consistent metric calculations
- Semantic business language
- SQL generation from queries
- Metric governance

### Negative
- Learning curve for metric modeling
- SQL generation complexity
- Performance depends on underlying tables

### Risks
- Inconsistent dimension definitions
- Complex metric dependencies
- Query performance at scale

## Implementation

Files created:
1. `src/mdde/semantic/metrics/models.py` - Data models
2. `src/mdde/semantic/metrics/manager.py` - MetricsManager
3. `src/mdde/semantic/metrics/__init__.py` - Module exports

## References

- [dbt Metrics](https://docs.getdbt.com/docs/build/metrics)
- [Looker LookML](https://docs.looker.com/data-modeling/learning-lookml)
- [MetricFlow](https://docs.transform.co/docs/overview)
- [Knowledge Plane Analysis](../research/knowledge-plane-analysis.md)
