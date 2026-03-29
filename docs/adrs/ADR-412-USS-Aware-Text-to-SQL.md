# ADR-412: USS-Aware Text-to-SQL Generation

**Status:** Proposed
**Date:** 2026-03-23
**Author:** MDDE Team

## Context

Text-to-SQL systems struggle with join path selection. When multiple facts and dimensions exist, the LLM must infer:
- Which tables to join
- Which join keys to use
- How to avoid fan traps and chasm traps
- How to handle M:N relationships

Even with rich semantic models, accuracy plateaus around 70-80% because join inference is fundamentally ambiguous.

### The Puppini Bridge Solution

The **Universal Star Schema (USS)**, developed by Bill Inmon and Francesco Puppini, solves this by introducing a **bridge table** as the single integration point:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    UNIVERSAL STAR SCHEMA                             │
│                                                                      │
│     ┌──────────┐     ┌───────────┐     ┌──────────┐                 │
│     │Dim Date  │     │Dim Customer│    │Dim Product│                │
│     └────┬─────┘     └─────┬─────┘     └────┬─────┘                 │
│          │                 │                 │                       │
│          └────────────┬────┴────────────────┘                        │
│                       │                                              │
│                       ▼                                              │
│              ┌────────────────┐                                      │
│              │   BRIDGE       │  ◄── All joins go through here      │
│              │   TABLE        │                                      │
│              └────────┬───────┘                                      │
│                       │                                              │
│          ┌────────────┼────────────┐                                 │
│          │            │            │                                 │
│          ▼            ▼            ▼                                 │
│     ┌─────────┐  ┌─────────┐  ┌─────────┐                           │
│     │  Fact   │  │  Fact   │  │  Fact   │                           │
│     │ Orders  │  │Shipments│  │Invoices │                           │
│     └─────────┘  └─────────┘  └─────────┘                           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

Key insight: **Every entity has exactly ONE relationship to the bridge**. This eliminates join ambiguity.

### Why This Matters for GenAI

| Without USS | With USS |
|-------------|----------|
| LLM must choose among N possible join paths | Single canonical join path |
| Fan traps cause row explosion | Traps impossible by design |
| Complex prompt engineering needed | Simple "bridge-first" rule |
| M:N relationships require special handling | Bridge handles M:N naturally |
| Join accuracy ~70-80% | Join accuracy ~95%+ |

## Decision

Create a **USS-aware text-to-SQL module** that:

1. **Detects USS models** - Identifies semantic models using the bridge pattern
2. **Generates bridge-first SQL** - All queries start from the bridge table
3. **Provides deterministic join paths** - No join inference needed
4. **Integrates with verified queries** - USS examples improve few-shot learning

## Architecture

### 1. USS Model Detection

```yaml
# semantic_model.yaml
semantic_model:
  name: sales_analytics
  pattern: universal_star_schema  # Signals USS-aware generation

  bridge:
    table: gold.bridge_sales
    description: Central integration point for sales analytics

    dimension_keys:
      - name: date_sk
        dimension: dim_date
        role: transaction_date

      - name: customer_sk
        dimension: dim_customer

      - name: product_sk
        dimension: dim_product

    fact_keys:
      - name: order_sk
        fact: fact_orders
        nullable: true  # Not all bridge rows have orders

      - name: shipment_sk
        fact: fact_shipments
        nullable: true

      - name: invoice_sk
        fact: fact_invoices
        nullable: true

  # Standard semantic model elements
  entities:
    - dim_date
    - dim_customer
    - dim_product
    - fact_orders
    - fact_shipments
    - fact_invoices

  metrics:
    - total_revenue
    - order_count
    - shipment_count
```

### 2. USS Query Builder

```python
class USSQueryBuilder:
    """
    USS-aware text-to-SQL generator.

    Key principle: All joins flow through the bridge table.

    Usage:
        builder = USSQueryBuilder(conn)
        result = builder.generate(
            question="Top 5 customers by revenue with their shipment counts",
            semantic_model_id="sales_analytics"
        )
        print(result.sql)
    """

    def generate(
        self,
        question: str,
        semantic_model_id: str,
    ) -> USSQueryResult:
        # 1. Load USS model
        model = self._load_uss_model(semantic_model_id)

        # 2. Extract required entities from question
        entities = self._identify_entities(question, model)

        # 3. Build deterministic join structure
        join_structure = self._build_bridge_joins(model.bridge, entities)

        # 4. Generate SQL with LLM (joins pre-determined)
        sql = self._generate_with_llm(question, join_structure, model)

        return USSQueryResult(
            sql=sql,
            join_path=join_structure,
            entities_used=entities,
            bridge_table=model.bridge.table,
        )
```

### 3. Bridge-First Join Generation

The critical insight: **joins are deterministic once we know the required entities**.

```python
def _build_bridge_joins(
    self,
    bridge: BridgeDefinition,
    entities: List[str],
) -> JoinStructure:
    """
    Build deterministic join structure through bridge.

    All joins follow the pattern:
      FROM bridge b
      JOIN dim_x ON b.x_sk = dim_x.x_sk
      JOIN fact_y ON b.y_sk = fact_y.y_sk
    """
    joins = [f"FROM {bridge.table} b"]

    for entity in entities:
        if entity in bridge.dimension_keys:
            key = bridge.dimension_keys[entity]
            joins.append(
                f"JOIN {entity} ON b.{key.name} = {entity}.{key.name}"
            )
        elif entity in bridge.fact_keys:
            key = bridge.fact_keys[entity]
            # Facts are LEFT JOIN (not all bridge rows have all facts)
            joins.append(
                f"LEFT JOIN {entity} ON b.{key.name} = {entity}.{key.name}"
            )

    return JoinStructure(joins=joins, bridge=bridge.table)
```

### 4. LLM Prompt with Pre-Built Joins

```python
USS_GENERATE_PROMPT = """You are generating SQL for a Universal Star Schema model.

## IMPORTANT: Join Structure (Pre-Determined)
The following join structure is MANDATORY. Do not modify it:

```sql
{join_structure}
```

## Bridge Table
All queries flow through: {bridge_table}
- Dimension keys: {dimension_keys}
- Fact keys: {fact_keys}

## Available Metrics
{metrics}

## User Question
{question}

## Instructions
1. Use the EXACT join structure provided above
2. Add SELECT columns based on the question
3. Add WHERE/GROUP BY/ORDER BY as needed
4. Use proper aggregations for fact measures
5. NULL-safe handling for fact columns (they may be NULL in bridge)

## Response
Return only the SQL query:
```sql
"""
```

### 5. Integration with Verified Queries (ADR-375)

USS models should have verified queries that demonstrate the bridge pattern:

```yaml
# verified_queries/sales_uss.yaml
verified_queries:
  - id: vq_uss_001
    question: "Top 5 customers by revenue"
    sql: |
      SELECT
        c.customer_name,
        SUM(fo.order_amount) as total_revenue
      FROM gold.bridge_sales b
      JOIN dim_customer c ON b.customer_sk = c.customer_sk
      LEFT JOIN fact_orders fo ON b.order_sk = fo.order_sk
      WHERE fo.order_sk IS NOT NULL  -- Only rows with orders
      GROUP BY c.customer_name
      ORDER BY total_revenue DESC
      LIMIT 5
    intent: ranking
    pattern: universal_star_schema
    bridge_table: gold.bridge_sales
    entities: [dim_customer, fact_orders]

  - id: vq_uss_002
    question: "Orders vs shipments by month"
    sql: |
      SELECT
        d.month_name,
        COUNT(DISTINCT fo.order_sk) as order_count,
        COUNT(DISTINCT fs.shipment_sk) as shipment_count
      FROM gold.bridge_sales b
      JOIN dim_date d ON b.date_sk = d.date_sk
      LEFT JOIN fact_orders fo ON b.order_sk = fo.order_sk
      LEFT JOIN fact_shipments fs ON b.shipment_sk = fs.shipment_sk
      GROUP BY d.month_name, d.month_num
      ORDER BY d.month_num
    intent: comparison
    pattern: universal_star_schema
    bridge_table: gold.bridge_sales
    entities: [dim_date, fact_orders, fact_shipments]
    notes: |
      Cross-fact query demonstrating safe drill-across via bridge.
      No fan trap because both facts join to bridge, not to each other.
```

## Implementation

### Module Structure

```
src/mdde/genai/uss_assistant/
├── __init__.py
├── types.py              # USSModel, BridgeDefinition, JoinStructure
├── builder.py            # USSQueryBuilder
├── prompts.py            # USS-specific prompt templates
├── entity_extractor.py   # Extract entities from questions
├── join_generator.py     # Deterministic join generation
└── validator.py          # Validate generated SQL uses bridge
```

### Key Classes

```python
@dataclass
class BridgeDefinition:
    """Definition of a USS bridge table."""
    table: str
    schema: str
    description: str
    dimension_keys: Dict[str, BridgeKey]  # entity -> key info
    fact_keys: Dict[str, BridgeKey]       # entity -> key info


@dataclass
class BridgeKey:
    """A key column in the bridge table."""
    name: str           # Column name (e.g., customer_sk)
    entity: str         # Target entity (e.g., dim_customer)
    role: Optional[str] # Role for role-playing dims (e.g., ship_date)
    nullable: bool      # True for fact keys


@dataclass
class USSModel:
    """A Universal Star Schema semantic model."""
    model_id: str
    name: str
    bridge: BridgeDefinition
    dimensions: List[str]
    facts: List[str]
    metrics: List[MetricDefinition]


@dataclass
class JoinStructure:
    """Pre-built join structure for USS query."""
    bridge: str
    joins: List[str]
    dimension_aliases: Dict[str, str]
    fact_aliases: Dict[str, str]

    def to_sql(self) -> str:
        return "\n".join(self.joins)


@dataclass
class USSQueryResult:
    """Result of USS query generation."""
    success: bool
    sql: str
    join_path: JoinStructure
    entities_used: List[str]
    bridge_table: str
    metrics_used: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    tokens_used: int = 0
```

### Validation

```python
class USSQueryValidator:
    """Validate that generated SQL follows USS pattern."""

    def validate(self, sql: str, model: USSModel) -> ValidationResult:
        issues = []

        # Check 1: Query must start from bridge
        if model.bridge.table not in sql:
            issues.append(
                f"Query does not use bridge table {model.bridge.table}"
            )

        # Check 2: No direct fact-to-dimension joins
        for fact in model.facts:
            for dim in model.dimensions:
                if self._has_direct_join(sql, fact, dim):
                    issues.append(
                        f"Direct join between {fact} and {dim}. "
                        f"Use bridge table instead."
                    )

        # Check 3: Facts should be LEFT JOIN (nullable in bridge)
        for fact in model.facts:
            if f"JOIN {fact}" in sql and f"LEFT JOIN {fact}" not in sql:
                issues.append(
                    f"Fact {fact} should use LEFT JOIN (nullable in bridge)"
                )

        return ValidationResult(
            valid=len(issues) == 0,
            issues=issues
        )
```

## Benefits

### 1. Deterministic Join Paths
No more guessing which tables to join. The bridge provides a single canonical path.

### 2. Impossible Fan Traps
Traditional star schemas can explode rows when joining multiple facts. USS prevents this by design.

### 3. Simpler Prompts
Instead of explaining complex join rules, we simply say "use the bridge."

### 4. Better Few-Shot Learning
Verified queries all follow the same pattern, making examples more effective.

### 5. Cross-Fact Analysis
Drill-across queries (Orders + Shipments) become trivial and safe.

## Example Queries

### Question: "Top customers by revenue who also have shipments"

**Without USS (Ambiguous):**
```sql
-- Which join path? Multiple options, risk of fan trap
SELECT c.name, SUM(o.amount), COUNT(s.id)
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN shipments s ON o.id = s.order_id  -- Or c.id = s.customer_id?
GROUP BY c.name
-- Fan trap: orders × shipments explosion
```

**With USS (Deterministic):**
```sql
SELECT
  c.customer_name,
  SUM(fo.order_amount) as revenue,
  COUNT(DISTINCT fs.shipment_sk) as shipments
FROM gold.bridge_sales b
JOIN dim_customer c ON b.customer_sk = c.customer_sk
LEFT JOIN fact_orders fo ON b.order_sk = fo.order_sk
LEFT JOIN fact_shipments fs ON b.shipment_sk = fs.shipment_sk
WHERE fo.order_sk IS NOT NULL  -- Has orders
  AND fs.shipment_sk IS NOT NULL  -- Has shipments
GROUP BY c.customer_name
ORDER BY revenue DESC
-- No fan trap: each fact joins to bridge independently
```

## Integration Points

| Component | Integration |
|-----------|-------------|
| SQL Assistant | Add USS mode detection |
| Verified Queries (ADR-375) | Tag USS examples |
| Semantic Layer (ADR-301) | Add USS model type |
| Copilot | USS-aware chat |
| Intelligent Layer (ADR-372) | USS inference rules |

## Implementation Plan

### Phase 1: Core Builder
- [ ] `USSModel` and `BridgeDefinition` types
- [ ] `USSQueryBuilder.generate()` method
- [ ] Bridge join generator
- [ ] Unit tests

### Phase 2: LLM Integration
- [ ] USS-specific prompts
- [ ] Entity extraction from questions
- [ ] Integration with GenAI providers

### Phase 3: Validation & Safety
- [ ] `USSQueryValidator`
- [ ] Fan trap detection
- [ ] Bridge usage enforcement

### Phase 4: Verified Query Integration
- [ ] USS pattern tag for verified queries
- [ ] Retrieval filtering by pattern
- [ ] Example formatting for prompts

## Consequences

### Positive
- **Higher accuracy**: Join paths are deterministic
- **Safer queries**: Fan traps impossible
- **Simpler prompts**: "Use the bridge" is easy to understand
- **Better examples**: Consistent pattern improves few-shot learning

### Negative
- **Requires USS modeling**: Teams must adopt the pattern
- **Bridge overhead**: Additional table to maintain
- **Learning curve**: New pattern for modelers

### Neutral
- **Optional adoption**: Works alongside traditional models
- **Gradual migration**: Start with high-value analytical domains

## References

- [The Unified Star Schema - Bill Inmon & Francesco Puppini (2020)](https://www.amazon.com/Unified-Star-Schema-Agile-Resilient/dp/1634628160)
- [Universal Star Schema Skill](.claude/skills/modeling/universal-star-schema/SKILL.md)
- ADR-301: Semantic Layer Module
- ADR-372: Intelligent Semantic Layer
- ADR-375: Verified Query Repository
- [Francesco Puppini: Designing OBT for AI](https://metadatamatters.com/2026/02/04/designing-one-big-table-obt-for-ai/)
