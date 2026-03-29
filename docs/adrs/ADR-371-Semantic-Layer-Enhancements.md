# ADR-371: Semantic Layer Enhancements - Headless Analytics & Metrics-as-Code

> **Inspired by**: "Best Open-Source Semantic Layer Tools in 2026" - Sergey Gromov

**Status**: Proposed
**Date**: 2026-03-13
**Extends**: ADR-245 (Metrics Layer), ADR-301 (Semantic Layer Module)

---

## Context

MDDE already has a robust semantic layer (ADR-245, ADR-301) with:
- MetricDefinition, MetricDimension, MetricFilter
- SemanticModel, SemanticMetric, SemanticDimension
- Hierarchy support with drill-down paths
- SQL generation from semantic queries

However, comparing MDDE to modern semantic layer tools (Cube, dbt Semantic Layer/MetricFlow, MetriQL, Malloy), several architectural patterns could strengthen MDDE's position:

### Current Gaps

| Capability | Cube | MetricFlow | MDDE Current |
|------------|------|------------|--------------|
| **Headless API** | REST/GraphQL | SQL generation | Limited |
| **Pre-aggregations** | Built-in | Via dbt | Not integrated |
| **Metrics-as-Code** | YAML/JS | YAML | Partial |
| **Multi-tool serving** | Yes | Via dbt | Metadata only |
| **AI/Agent interface** | Emerging | Emerging | ADR-246 basis |
| **Caching layer** | Yes | Via warehouse | Not integrated |

### Key Insights from Article

1. **Metric Drift Problem**: Same metric calculated differently across tools
2. **BI-Agnostic Requirement**: Semantic layer should serve any consumer
3. **Metrics-as-Code**: Version control, review process, testing
4. **Headless Architecture**: API-first for embedded analytics
5. **AI Agents**: Semantic layer as interface for automated reasoning

---

## Decision

Enhance MDDE's semantic layer with three architectural patterns:

### Pattern 1: Metrics-as-Code Export/Import

Enable round-trip between MDDE metadata and YAML metric definitions:

```yaml
# metrics/revenue.yaml
metric:
  id: MET_revenue
  name: revenue
  description: "{{ doc('revenue') }}"
  type: derived
  expression: "SUM(order_amount) - SUM(refund_amount)"
  entity_id: ENT_order

  filters:
    - id: cancelled_excluded
      expression: "order_status != 'cancelled'"
      is_default: true

  dimensions:
    - id: DIM_time
      attribute_id: ATT_order_date
      role: time
      time_grains: [day, week, month, quarter, year]

    - id: DIM_region
      attribute_id: ATT_region
      role: group_by
      hierarchy_id: HIE_geography

  goals:
    - id: GOAL_monthly
      period: month
      target: 1000000

  certified: true
  owner: finance-team
  tags: [finance, kpi, certified]
```

### Pattern 2: Semantic Query API

Expose metrics through a query interface that generates optimized SQL:

```python
from mdde.semantic.query import SemanticQueryEngine

engine = SemanticQueryEngine(conn)

# Semantic query (BI-agnostic)
result = engine.query(
    metrics=["revenue", "order_count"],
    dimensions=["region", "product_category"],
    filters=[
        {"dimension": "date", "operator": ">=", "value": "2026-01-01"},
        {"dimension": "region", "operator": "in", "value": ["EMEA", "APAC"]},
    ],
    time_grain="month",
)

# Returns SQL and optionally executes
print(result.sql)
# SELECT
#   DATE_TRUNC('month', o.order_date) AS date,
#   o.region,
#   o.product_category,
#   SUM(o.order_amount) - SUM(o.refund_amount) AS revenue,
#   COUNT(*) AS order_count
# FROM orders o
# WHERE o.order_status != 'cancelled'
#   AND o.order_date >= '2026-01-01'
#   AND o.region IN ('EMEA', 'APAC')
# GROUP BY 1, 2, 3
```

### Pattern 3: Pre-Aggregation Definitions

Define materialized aggregations for common query patterns:

```python
from mdde.semantic.preagg import PreAggregationManager

preagg = PreAggregationManager(conn)

preagg.define(
    preagg_id="PREAGG_revenue_monthly",
    metric_id="MET_revenue",
    dimensions=["region", "product_category"],
    time_grain="month",
    refresh_schedule="0 2 * * *",  # Daily at 2am
    partition_by="date_month",
    indexes=["region", "product_category"],
)

# Generate DDL
ddl = preagg.generate_ddl("PREAGG_revenue_monthly")
```

---

## Architecture

```
                    MDDE Semantic Layer Architecture
    ================================================================

                         ┌─────────────────────────┐
                         │   Metrics-as-Code       │
                         │   (YAML Definitions)    │
                         └───────────┬─────────────┘
                                     │
                                     ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    Semantic Registry                         │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
    │  │   Metrics   │  │  Dimensions │  │ Hierarchies │         │
    │  │  (ADR-245)  │  │  (ADR-301)  │  │  (ADR-301)  │         │
    │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘         │
    │         └────────────────┼────────────────┘                 │
    │                          │                                   │
    │         ┌────────────────┼────────────────┐                 │
    │         ▼                ▼                ▼                 │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
    │  │    Query    │  │  Pre-Agg    │  │   AI Agent  │         │
    │  │   Engine    │  │  Advisor    │  │  Interface  │         │
    │  │  (NEW)      │  │  (NEW)      │  │  (ADR-246)  │         │
    │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘         │
    └─────────┼────────────────┼────────────────┼─────────────────┘
              │                │                │
              ▼                ▼                ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    Consumers                                 │
    │                                                              │
    │   BI Tools    Notebooks    APIs    AI Agents    dbt/SQL     │
    │                                                              │
    └─────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: Metrics-as-Code (40 tests)

**New module**: `mdde.semantic.metrics_code`

```python
# Types
@dataclass
class MetricYamlConfig:
    """Complete metric definition in YAML format."""
    id: str
    name: str
    description: str
    type: MetricType
    expression: str
    entity_id: Optional[str]
    attribute_id: Optional[str]
    aggregation: Optional[AggregationType]
    filters: List[MetricFilterConfig]
    dimensions: List[DimensionRefConfig]
    goals: List[MetricGoalConfig]
    certified: bool = False
    owner: Optional[str] = None
    tags: List[str] = field(default_factory=list)

# Parser
class MetricYamlParser:
    """Parse metric definitions from YAML."""

    def parse_file(self, path: str) -> List[MetricYamlConfig]: ...
    def parse_directory(self, directory: str) -> List[MetricYamlConfig]: ...
    def validate(self, config: MetricYamlConfig) -> List[ValidationError]: ...

# Exporter
class MetricYamlExporter:
    """Export MDDE metrics to YAML format."""

    def export_metric(self, metric_id: str) -> str: ...
    def export_model(self, model_id: str, output_dir: str) -> List[str]: ...
    def export_all(self, output_dir: str) -> List[str]: ...

# Sync
class MetricCodeSync:
    """Bidirectional sync between YAML and database."""

    def import_from_yaml(self, path: str) -> SyncResult: ...
    def export_to_yaml(self, output_dir: str) -> SyncResult: ...
    def diff(self, yaml_path: str) -> List[MetricDiff]: ...
```

### Phase 2: Query Engine (35 tests)

**New module**: `mdde.semantic.query`

```python
@dataclass
class SemanticQuery:
    """A semantic query request."""
    metrics: List[str]
    dimensions: List[str] = field(default_factory=list)
    filters: List[QueryFilter] = field(default_factory=list)
    time_grain: Optional[TimeGrain] = None
    limit: Optional[int] = None
    order_by: List[OrderSpec] = field(default_factory=list)

@dataclass
class SemanticQueryResult:
    """Result of semantic query compilation."""
    sql: str
    columns: List[ColumnSpec]
    metrics_used: List[str]
    dimensions_used: List[str]
    compilation_time_ms: float
    warnings: List[str] = field(default_factory=list)

class SemanticQueryEngine:
    """Generate SQL from semantic queries."""

    def __init__(self, conn, semantic_model_id: str): ...

    def query(
        self,
        metrics: List[str],
        dimensions: Optional[List[str]] = None,
        filters: Optional[List[dict]] = None,
        time_grain: Optional[str] = None,
        **kwargs
    ) -> SemanticQueryResult: ...

    def execute(
        self,
        query: SemanticQuery,
        return_dataframe: bool = True
    ) -> Any: ...

    def validate_query(self, query: SemanticQuery) -> List[ValidationError]: ...

    def explain(self, query: SemanticQuery) -> QueryExplanation: ...
```

### Phase 3: Pre-Aggregation Advisor (25 tests)

**New module**: `mdde.semantic.preagg`

```python
@dataclass
class PreAggregation:
    """Pre-aggregation definition."""
    preagg_id: str
    metric_id: str
    dimensions: List[str]
    time_grain: TimeGrain

    # Scheduling
    refresh_schedule: Optional[str] = None  # Cron expression
    refresh_window: Optional[str] = None    # e.g., "7 days"

    # Storage
    partition_by: Optional[str] = None
    indexes: List[str] = field(default_factory=list)

    # Metadata
    estimated_rows: Optional[int] = None
    estimated_size_mb: Optional[float] = None
    last_refresh: Optional[datetime] = None

class PreAggregationAdvisor:
    """Analyze query patterns and recommend pre-aggregations."""

    def analyze_query_log(
        self,
        days: int = 30
    ) -> List[PreAggRecommendation]: ...

    def estimate_storage(
        self,
        preagg: PreAggregation
    ) -> StorageEstimate: ...

    def estimate_speedup(
        self,
        preagg: PreAggregation,
        sample_queries: List[SemanticQuery]
    ) -> SpeedupEstimate: ...

class PreAggregationManager:
    """Manage pre-aggregation definitions and DDL generation."""

    def define(self, **kwargs) -> PreAggregation: ...
    def generate_ddl(self, preagg_id: str, dialect: str = "duckdb") -> str: ...
    def generate_refresh_sql(self, preagg_id: str) -> str: ...
    def list_preaggs(self, metric_id: Optional[str] = None) -> List[PreAggregation]: ...
```

### Phase 4: Tool Export Integrations (20 tests)

**New module**: `mdde.semantic.export`

```python
class CubeExporter:
    """Export semantic model to Cube.js format."""

    def export_model(self, model_id: str) -> dict: ...
    def export_to_file(self, model_id: str, output_path: str) -> None: ...

class MetricFlowExporter:
    """Export semantic model to dbt MetricFlow YAML."""

    def export_model(self, model_id: str) -> str: ...
    def export_to_directory(self, model_id: str, output_dir: str) -> List[str]: ...

class LookMLExporter:
    """Export semantic model to LookML format."""

    def export_model(self, model_id: str) -> str: ...

class PowerBISemanticExporter:
    """Export semantic model to Power BI semantic model format."""

    def export_model(self, model_id: str) -> dict: ...
```

---

## Use Cases

### Use Case 1: Metrics as Code Workflow

```python
from mdde.semantic.metrics_code import MetricYamlParser, MetricCodeSync

# Load metrics from YAML
parser = MetricYamlParser()
metrics = parser.parse_directory("semantic/metrics")

# Validate
for metric in metrics:
    errors = parser.validate(metric)
    if errors:
        print(f"Validation errors in {metric.id}: {errors}")

# Sync to database
sync = MetricCodeSync(conn)
result = sync.import_from_yaml("semantic/metrics")
print(f"Imported {result.created} new, updated {result.updated}")
```

### Use Case 2: BI Tool Query Generation

```python
from mdde.semantic.query import SemanticQueryEngine

engine = SemanticQueryEngine(conn, "SEM_sales")

# Business user query
result = engine.query(
    metrics=["revenue", "margin_percent"],
    dimensions=["region", "product_line"],
    filters=[
        {"dimension": "date", "operator": "between",
         "value": ["2026-01-01", "2026-03-31"]},
    ],
    time_grain="month",
)

# Execute in any warehouse
df = engine.execute(result, return_dataframe=True)
```

### Use Case 3: Pre-Aggregation Optimization

```python
from mdde.semantic.preagg import PreAggregationAdvisor, PreAggregationManager

# Analyze query patterns
advisor = PreAggregationAdvisor(conn)
recommendations = advisor.analyze_query_log(days=30)

for rec in recommendations[:5]:
    print(f"Recommended: {rec.metric_id} by {rec.dimensions}")
    print(f"  Estimated speedup: {rec.estimated_speedup}x")
    print(f"  Storage cost: {rec.estimated_size_mb} MB")

# Create recommended pre-aggregation
manager = PreAggregationManager(conn)
preagg = manager.define(
    preagg_id="PREAGG_revenue_region_monthly",
    metric_id="MET_revenue",
    dimensions=["region", "product_category"],
    time_grain="month",
    refresh_schedule="0 3 * * *",
)

# Generate DDL for target warehouse
ddl = manager.generate_ddl(preagg.preagg_id, dialect="snowflake")
print(ddl)
```

### Use Case 4: AI Agent Interface

```python
from mdde.semantic.agent import SemanticAgentInterface

interface = SemanticAgentInterface(conn, "SEM_sales")

# AI agent asks semantic question
response = interface.process_question(
    "What was the revenue by region last quarter?"
)

# Returns structured response
print(response.interpretation)
# Metric: revenue, Dimensions: region, Time: 2026-Q1

print(response.sql)
# SELECT region, SUM(revenue) ...

print(response.answer)
# EMEA: $2.3M, APAC: $1.8M, Americas: $3.1M
```

---

## Consequences

### Positive

- **Tool Agnostic**: Same metrics work across BI tools, notebooks, APIs
- **Version Control**: Metrics stored as code, proper review process
- **Query Optimization**: Pre-aggregations reduce warehouse load
- **AI Ready**: Structured interface for automated reasoning
- **Export Flexibility**: Generate configs for Cube, MetricFlow, LookML

### Negative

- **Complexity**: Additional layers to maintain
- **Learning Curve**: New concepts (pre-agg, semantic queries)
- **Storage Overhead**: Pre-aggregations consume space

### Mitigations

| Risk | Mitigation |
|------|------------|
| Complexity | Start with metrics-as-code, add query engine later |
| Learning curve | Comprehensive examples, wizard UI |
| Storage overhead | Advisor recommends cost-effective pre-aggs only |

---

## Test Plan

| Phase | Test File | Expected Tests |
|-------|-----------|----------------|
| 1 | `test_metrics_code.py` | 40 |
| 2 | `test_query_engine.py` | 35 |
| 3 | `test_preagg.py` | 25 |
| 4 | `test_tool_export.py` | 20 |
| **Total** | | **120** |

---

## Related ADRs

| ADR | Relationship |
|-----|--------------|
| ADR-245 | Extends (Metrics Layer) |
| ADR-301 | Extends (Semantic Layer Module) |
| ADR-246 | Integrates (AI Agent Context) |
| ADR-370 | Uses (Doc Blocks for metric descriptions) |
| ADR-369 | Related (Source Delivery for data freshness) |

---

## References

- [Best Open-Source Semantic Layer Tools in 2026](https://medium.com/@sergeygromov) - Sergey Gromov
- [Cube Documentation](https://cube.dev/docs)
- [dbt Semantic Layer / MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow)
- [Patrick Okare's Five Must-Have Layers](https://medium.com/@patrickt.okare)
- MDDE Semantic Layer (ADR-245, ADR-301)

---

## Integration Patterns

### Integration 1: Cube.js Integration

Bi-directional sync with Cube.js semantic layer:

```python
from mdde.semantic.integrations.cube import CubeIntegration

cube = CubeIntegration(
    cube_api_url="http://localhost:4000",
    cube_api_secret="your-secret",
)

# Export MDDE semantic model to Cube
cube.export_model(conn, "SEM_sales", output_dir="cube/schema")

# Import Cube schema into MDDE
cube.import_schema(conn, "cube/schema/Orders.js", target_model="SEM_sales")

# Query through Cube API
result = cube.query(
    measures=["Orders.revenue"],
    dimensions=["Orders.region"],
    time_dimension={"dimension": "Orders.createdAt", "granularity": "month"},
)
```

**Generated Cube Schema**:
```javascript
// cube/schema/Revenue.js
cube(`Revenue`, {
  sql: `SELECT * FROM ${orders}`,

  measures: {
    revenue: {
      type: `sum`,
      sql: `order_amount - refund_amount`,
      description: "Total revenue after refunds",
      meta: { mdde_metric_id: "MET_revenue" },
    },
  },

  dimensions: {
    region: {
      type: `string`,
      sql: `region`,
    },
  },

  preAggregations: {
    revenueByRegionMonthly: {
      measures: [revenue],
      dimensions: [region],
      timeDimension: createdAt,
      granularity: `month`,
      refreshKey: { every: `1 day` },
    },
  },
});
```

### Integration 2: dbt MetricFlow Integration

Sync metrics with dbt Semantic Layer:

```python
from mdde.semantic.integrations.metricflow import MetricFlowIntegration

mf = MetricFlowIntegration(dbt_project_dir="/path/to/dbt")

# Export MDDE metrics to MetricFlow YAML
mf.export_metrics(conn, "SEM_sales", output_dir="semantic_models")

# Import MetricFlow definitions into MDDE
mf.import_semantic_manifest(conn, "target/semantic_manifest.json")

# Validate consistency
diff = mf.diff(conn, "SEM_sales")
for change in diff:
    print(f"{change.type}: {change.metric_id} - {change.description}")
```

**Generated MetricFlow YAML**:
```yaml
# semantic_models/sem_sales.yml
semantic_models:
  - name: orders
    model: ref('fct_orders')
    defaults:
      agg_time_dimension: order_date

    entities:
      - name: order
        type: primary
        expr: order_id

    measures:
      - name: revenue
        agg: sum
        expr: order_amount - refund_amount
        description: "{{ doc('revenue') }}"

    dimensions:
      - name: region
        type: categorical
        expr: region

metrics:
  - name: revenue
    type: simple
    type_params:
      measure: revenue
    filter: |
      {{ Dimension('order__status') }} != 'cancelled'
```

### Integration 3: Apache Superset Integration

Connect MDDE semantic layer to Superset:

```python
from mdde.semantic.integrations.superset import SupersetIntegration

superset = SupersetIntegration(
    superset_url="http://localhost:8088",
    username="admin",
    password="admin",
)

# Create Superset dataset from MDDE semantic model
superset.create_dataset(
    conn=conn,
    semantic_model_id="SEM_sales",
    database_id=1,  # Superset database ID
)

# Sync metric definitions to Superset saved metrics
superset.sync_metrics(conn, "SEM_sales")
```

### Integration 4: Looker/LookML Integration

Export to LookML for Looker:

```python
from mdde.semantic.integrations.looker import LookMLIntegration

looker = LookMLIntegration()

# Export MDDE model to LookML
looker.export_model(
    conn=conn,
    model_id="SEM_sales",
    output_dir="looker/views",
)
```

**Generated LookML**:
```lookml
# looker/views/orders.view.lkml
view: orders {
  sql_table_name: public.orders ;;

  measure: revenue {
    type: sum
    sql: ${order_amount} - ${refund_amount} ;;
    description: "Total revenue after refunds"
    filters: [status: "-cancelled"]
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension_group: order_date {
    type: time
    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}.order_date ;;
  }
}
```

### Integration 5: Power BI Semantic Model

Export to Power BI TMDL format:

```python
from mdde.semantic.integrations.powerbi import PowerBISemanticIntegration

pbi = PowerBISemanticIntegration()

# Export to TMDL (Tabular Model Definition Language)
pbi.export_to_tmdl(
    conn=conn,
    model_id="SEM_sales",
    output_dir="powerbi/semantic",
)

# Export to PBIP (Power BI Project)
pbi.export_to_pbip(
    conn=conn,
    model_id="SEM_sales",
    output_path="powerbi/sales.pbip",
)
```

### Integration 6: Tableau Pulse / Tableau Semantics

Export to Tableau semantic layer:

```python
from mdde.semantic.integrations.tableau import TableauSemanticIntegration

tableau = TableauSemanticIntegration(
    server_url="https://tableau.company.com",
    api_token="your-token",
)

# Create Tableau data source from MDDE model
tableau.publish_data_source(
    conn=conn,
    model_id="SEM_sales",
    project_name="Analytics",
)

# Sync metrics to Tableau Pulse
tableau.sync_pulse_metrics(conn, "SEM_sales")
```

### Integration 7: OpenLineage / DataHub

Export metric lineage for data governance:

```python
from mdde.semantic.integrations.openlineage import OpenLineageIntegration

ol = OpenLineageIntegration(
    datahub_url="http://datahub:8080",
)

# Emit metric lineage events
ol.emit_metric_lineage(
    conn=conn,
    metric_id="MET_revenue",
)

# Sync all semantic model lineage
ol.sync_model_lineage(conn, "SEM_sales")
```

### Integration 8: MCP (Model Context Protocol) for AI

Expose semantic layer through MCP for AI agents:

```python
from mdde.semantic.integrations.mcp import SemanticMCPServer

# Create MCP server exposing semantic layer
server = SemanticMCPServer(
    conn=conn,
    semantic_models=["SEM_sales", "SEM_finance"],
)

# Tools exposed to AI:
# - list_metrics: List available metrics
# - get_metric_definition: Get metric SQL and description
# - query_metric: Execute semantic query
# - explain_metric: Explain what a metric measures
```

**MCP Tool Definition**:
```json
{
  "name": "query_metric",
  "description": "Query a business metric with dimensions and filters",
  "inputSchema": {
    "type": "object",
    "properties": {
      "metrics": {
        "type": "array",
        "items": {"type": "string"},
        "description": "Metric names to query (e.g., ['revenue', 'order_count'])"
      },
      "dimensions": {
        "type": "array",
        "items": {"type": "string"},
        "description": "Dimensions to group by (e.g., ['region', 'product'])"
      },
      "filters": {
        "type": "array",
        "description": "Filter conditions"
      },
      "time_grain": {
        "type": "string",
        "enum": ["day", "week", "month", "quarter", "year"]
      }
    },
    "required": ["metrics"]
  }
}
```

---

## Integration Architecture

```
                    MDDE Semantic Layer Integrations
    ================================================================

                         ┌─────────────────────────┐
                         │   MDDE Semantic Layer   │
                         │   (ADR-245, ADR-301)    │
                         └───────────┬─────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
              ▼                      ▼                      ▼
    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
    │  BI Tool Export │    │  Semantic Layer │    │  Data Catalog   │
    │  Integrations   │    │  Integrations   │    │  Integrations   │
    ├─────────────────┤    ├─────────────────┤    ├─────────────────┤
    │ • Looker/LookML │    │ • Cube.js       │    │ • DataHub       │
    │ • Power BI TMDL │    │ • MetricFlow    │    │ • OpenLineage   │
    │ • Tableau       │    │ • Superset      │    │ • Atlan         │
    │ • Metabase      │    │ • Lightdash     │    │ • Collibra      │
    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                     │
                                     ▼
                         ┌─────────────────────────┐
                         │    AI/Agent Interface   │
                         │    (MCP, LangChain)     │
                         └─────────────────────────┘
```

---

## Implementation Checklist

### Before Starting
- [ ] Review ADR-245 (Metrics Layer)
- [ ] Review ADR-301 (Semantic Layer Module)
- [ ] Analyze current SemanticLayerManager capabilities

### During Implementation
- [ ] Phase 1: Metrics-as-Code export/import
- [ ] Phase 2: Query engine with SQL generation
- [ ] Phase 3: Pre-aggregation advisor
- [ ] Phase 4: Tool export integrations

### After Completion
- [ ] All 120+ tests passing
- [ ] Documentation with examples
- [ ] Integration with existing MetricsManager
- [ ] ADR status → Implemented

---

*This ADR follows MDDE's spec-driven development methodology.*
