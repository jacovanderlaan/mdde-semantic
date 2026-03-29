# ADR-364: SQL-Executable Ontology

## Status

Implemented

## Date

2026-02-27

## Context

Traditional semantic layers and ontologies are **descriptive** - they define what metrics mean and how concepts relate, but require separate systems to execute queries. This creates a gap:

- Semantic layer defines "revenue = SUM(order_total)"
- But execution happens in a separate BI tool or SQL engine
- AI systems can read metadata but can't execute it directly
- No unified interface for semantic query + execution

Research from Timbr.ai (Tzvi Weitzner, Feb 2026) identifies the key insight:

> "The real shift isn't from semantic layer to ontology. It's from **description to execution**."

MDDE already has:
- **ADR-301 Semantic Layer**: Metrics, dimensions, hierarchies (descriptive)
- **ADR-359 Business Ontology**: Causal relationships, context-aware interpretation
- **GenAI Module**: SQL generation from natural language

The gap: metrics and relationships are defined but not directly executable.

## Decision

Implement SQL-Executable Ontology that combines semantic meaning with SQL execution:

1. **ExecutableMetric**: Metrics with both definition AND SQL expression
2. **SemanticRelationship**: Relationships with JOIN clauses
3. **ExecutableOntology**: Ontology with path finding and query execution
4. **OntologyQuery**: Semantic query interface

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SQL-Executable Ontology                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   │
│  │ ExecutableMetric│   │ Semantic        │   │ Executable      │   │
│  │                 │   │ Relationship    │   │ Ontology        │   │
│  │ - definition    │   │                 │   │                 │   │
│  │ - sql_expression│   │ - rel_type      │   │ - metrics       │   │
│  │ - dimensions    │   │ - join_sql      │   │ - relationships │   │
│  │ - filters       │   │ - cardinality   │   │ - find_path()   │   │
│  │ - to_sql()      │   │ - to_join()     │   │ - execute()     │   │
│  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘   │
│           │                     │                     │             │
│           └─────────────────────┴─────────────────────┘             │
│                                 │                                    │
│                                 ▼                                    │
│                    ┌─────────────────────┐                          │
│                    │   OntologyQuery     │                          │
│                    │                     │                          │
│                    │ - query()           │                          │
│                    │ - nl_to_sql()       │                          │
│                    └─────────────────────┘                          │
│                                 │                                    │
│                                 ▼                                    │
│                    ┌─────────────────────┐                          │
│                    │  ExecutableQuery    │                          │
│                    │                     │                          │
│                    │  SQL + metadata     │                          │
│                    └─────────────────────┘                          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Implementation

### Core Types

```python
from mdde.semantic.ontology import (
    # Enums
    RelationshipType,  # HAS, BELONGS_TO, REFERENCES, etc.
    JoinType,          # INNER, LEFT, RIGHT, FULL
    Cardinality,       # 1:1, 1:N, N:1, N:M

    # Models
    QueryContext,
    ExecutableMetric,
    SemanticRelationship,
    ExecutableQuery,
    ExecutableOntology,
    OntologyQuery,
)
```

### ExecutableMetric

Metrics that are both defined AND executable:

```python
revenue_metric = ExecutableMetric(
    metric_id="monthly_revenue",
    name="Monthly Revenue",
    definition="Total revenue from orders per month",  # Human description

    # Execution
    sql_expression="SUM(order_total)",  # The actual SQL
    grain_entity_id="fact_orders",      # Aggregation level

    # Semantic context
    compatible_dimensions=["customer_segment", "region", "product_category"],
    named_filters={
        "enterprise_only": "customer_segment = 'enterprise'",
        "last_12_months": "order_date >= DATEADD(month, -12, CURRENT_DATE)",
    },

    # Governance
    owner="data-analytics@company.com",
    certified=True,
)

# Generate SQL
sql = revenue_metric.to_sql(alias="total_revenue")
# → "SUM(order_total) AS total_revenue"
```

### SemanticRelationship

Relationships with both meaning AND SQL:

```python
customer_orders = SemanticRelationship(
    relationship_id="customer_has_orders",
    source_entity_id="dim_customer",
    target_entity_id="fact_orders",

    # Semantic
    relationship_type=RelationshipType.HAS,
    cardinality=Cardinality.ONE_TO_MANY,
    name="Customer Orders",
    description="A customer has zero or more orders",

    # Execution
    join_type=JoinType.LEFT,
    join_condition="dim_customer.customer_id = fact_orders.customer_id",
    traversal_cost=1.0,  # For query optimization
)

# Generate JOIN
join_sql = customer_orders.to_join_clause()
# → "LEFT JOIN fact_orders ON dim_customer.customer_id = fact_orders.customer_id"
```

### ExecutableOntology

Combines metrics and relationships with execution capability:

```python
ontology = ExecutableOntology()

# Register entities
ontology.register_entity(
    "dim_customer",
    table_name="dim_customer",
    primary_key="customer_id",
    schema="gold",
)

# Add metrics
ontology.add_metric(revenue_metric)

# Add relationships
ontology.add_relationship(customer_orders)

# Register dimensions
ontology.register_dimension("customer_segment", "dim_customer", "segment")

# Find path between entities (for auto-joining)
path = ontology.find_path("dim_customer", "dim_product")
# Returns list of relationships to traverse

# Execute metric with dimensions
result = ontology.execute_metric(
    "monthly_revenue",
    dimensions=["customer_segment"],
    filters={"enterprise_only": True},
)
# Returns ExecutableQuery with SQL + metadata
```

### OntologyQuery

High-level query interface:

```python
query_engine = OntologyQuery(ontology)

# Semantic query
result = query_engine.query(
    metrics=["revenue", "order_count"],
    dimensions=["customer_segment", "region"],
    filters={"status": "active"},
)

# Natural language (basic)
result = query_engine.natural_language_to_sql(
    "Show me revenue by customer segment"
)
```

## Integration with Existing MDDE

### With ADR-301 Semantic Layer

```python
from mdde.semantic import SemanticLayerManager
from mdde.semantic.ontology import ExecutableOntology

# Current: Descriptive
semantic = SemanticLayerManager(conn)
metric = semantic.get_metric("monthly_revenue")
print(metric.definition)  # "Total revenue per month"

# Enhanced: Executable
ontology = ExecutableOntology()
# Import from semantic layer
for metric in semantic.get_all_metrics():
    ontology.add_metric(ExecutableMetric(
        metric_id=metric.metric_id,
        name=metric.name,
        definition=metric.definition,
        sql_expression=metric.expression,  # Now executable!
        grain_entity_id=metric.entity_id,
    ))
```

### With ADR-359 Business Ontology

```python
from mdde.semantic.ontology import BusinessOntology, ExecutableOntology

# Business ontology provides context
business = BusinessOntology()
business.add_concept(BusinessConcept(
    concept_id="customer_health",
    name="Customer Health",
    definition="Overall health of customer relationship",
    business_model="enterprise_white_glove",
))

# Executable ontology provides SQL
executable = ExecutableOntology()
executable.add_metric(ExecutableMetric(
    metric_id="health_score",
    name="Health Score",
    definition="Customer health (0-100)",
    sql_expression="(recency * 0.4 + frequency * 0.3 + monetary * 0.3) * 100",
    grain_entity_id="dim_customer",
))

# Combined: context-aware execution
```

### With GenAI Transpiler

```python
from mdde.genai import OntologyAwareTranspiler

transpiler = OntologyAwareTranspiler(conn, ontology)

# NL → SQL with ontology guidance
query = transpiler.natural_language_to_sql(
    "What's our revenue trend by enterprise customers?"
)
# Ontology ensures:
# - "revenue" resolves to correct metric SQL
# - "enterprise customers" resolves to correct filter
# - Query uses proper JOINs from relationships
```

## Key Benefits

1. **Unified Interface**: Define once, execute anywhere
2. **AI-Ready**: AI systems consume executable logic, not just descriptions
3. **Path Finding**: Auto-generate JOINs through relationship traversal
4. **Governed Execution**: Certified metrics execute consistently
5. **Context Preservation**: Business meaning travels with execution

## Database Schema

No new tables required. The ontology can be:
- Stored as YAML/JSON files
- Serialized to existing metadata tables
- Built dynamically from semantic layer

## Advanced Features

### Metric Inheritance

Metrics can inherit from parent metrics, enabling reuse and specialization:

```python
# Base metric
revenue = ExecutableMetric(
    metric_id="revenue",
    name="Revenue",
    definition="Total revenue",
    sql_expression="SUM(order_total)",
    grain_entity_id="fact_orders",
    compatible_dimensions=["region", "product"],
    named_filters={"last_30_days": "order_date >= CURRENT_DATE - 30"},
)

# Child metric inherits dimensions and filters
enterprise_revenue = ExecutableMetric(
    metric_id="enterprise_revenue",
    name="Enterprise Revenue",
    definition="Revenue from enterprise customers",
    sql_expression="SUM(order_total)",
    grain_entity_id="fact_orders",
    parent_metric_id="revenue",  # Inherit from revenue
    named_filters={"enterprise_only": "segment = 'enterprise'"},
    inherit_dimensions=True,  # Inherit parent's dimensions
    inherit_filters=True,  # Inherit parent's filters
)

# With transformation
revenue_growth_pct = ExecutableMetric(
    metric_id="revenue_growth_pct",
    name="Revenue Growth %",
    definition="Revenue growth as percentage",
    sql_expression="",  # Uses transformation
    grain_entity_id="fact_orders",
    parent_metric_id="revenue",
    transformation="* 100 / LAG(SUM(order_total))",  # Applied to parent SQL
)

# Resolve inheritance
ontology.add_metric(revenue)
ontology.add_metric(enterprise_revenue)
resolved = ontology.get_resolved_metric("enterprise_revenue")
# resolved now has both parent and child dimensions/filters
```

### Filter Composition

Combine named filters with AND/OR logic:

```python
from mdde.semantic.ontology import ComposedFilter, FilterCompositionMode

# Create composed filter
active_filter = ComposedFilter(
    name="active",
    sql_expression="status = 'active'",
)
enterprise_filter = ComposedFilter(
    name="enterprise",
    sql_expression="segment = 'enterprise'",
)

# Compose with AND
combined = active_filter.compose_with(
    enterprise_filter,
    FilterCompositionMode.AND,
)
# → "(status = 'active') AND (segment = 'enterprise')"

# Negate
not_active = active_filter.negate()
# → "NOT (status = 'active')"

# From ontology
composed = ontology.create_composed_filter(
    ["active", "enterprise"],
    FilterCompositionMode.AND,
)
```

### Optimization Hints

Guide query optimization with dialect-specific hints:

```python
from mdde.semantic.ontology import OptimizationHint

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
        OptimizationHint(
            hint_type="repartition",
            target="fact_orders",
            parameters={"columns": ["customer_id"]},
        ),
    ],
    preferred_aggregation_strategy="hash",
    estimated_cardinality=1000000,
)

# Get dialect-specific hints
hints = ontology.get_optimization_hints("revenue", dialect="databricks")
# → ["/*+ BROADCAST(dim_customer) */", "/*+ REPARTITION(customer_id) */"]
```

### OntologyAwareTranspiler

AI-powered NL→SQL with ontology grounding:

```python
from mdde.genai import OntologyAwareTranspiler

transpiler = OntologyAwareTranspiler(ontology)

# Transpile with ontology context
result = transpiler.transpile("Show me revenue by customer segment")

# Result includes:
result.sql          # The generated SQL
result.metrics_used       # ["revenue"]
result.dimensions_used    # ["customer_segment"]
result.grounded     # True - grounded in ontology
result.confidence   # 0.9 - high confidence
```

## Testing

66 tests covering:
- ExecutableMetric creation and SQL generation
- SemanticRelationship JOIN clause generation
- ExecutableOntology path finding and query execution
- OntologyQuery semantic and NL queries
- Metric inheritance and resolution
- Filter composition (AND/OR/NOT)
- Optimization hints and dialect-specific SQL
- OntologyAwareTranspiler integration
- Serialization roundtrips

## Related ADRs

- **ADR-301**: Semantic Layer (base metrics/dimensions)
- **ADR-359**: Business Ontology (causal relationships)
- **ADR-261**: GenAI Transpiler (NL→SQL integration point)
- **ADR-283**: Discovery (infer relationships from schema)

## References

- Tzvi Weitzner, Timbr.ai (Feb 2026): "SQL Ontology" concept
- Jessica Talisman, "Metadata Weekly": Ontologies, Context Graphs, and Semantic Layers
- [Timbr.ai](https://timbr.ai) - SQL Knowledge Graph
