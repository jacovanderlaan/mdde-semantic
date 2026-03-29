# ADR-375: Verified Query Repository

**Status:** Accepted
**Date:** 2026-03-21
**Author:** MDDE Team

## Context

Text-to-SQL systems face a fundamental accuracy challenge: they must infer correct SQL from natural language questions, often guessing at table relationships, column semantics, and business logic. Even with rich semantic models, accuracy plateaus around 70-80%.

A pattern emerging in the industry pushes accuracy past this baseline: **Verified Questions and SQL attached to the semantic model**. The idea is simple:

1. Take a question users actually ask
2. Write (or validate) the correct SQL for it
3. Attach that pair to the semantic model
4. When similar questions come in, retrieve relevant examples for the model to reason from

Snowflake calls this the **Verified Query Repository** in Cortex Analyst. Other vendors implement similar concepts but no open standard has made this a first-class concept yet.

### Current State in MDDE

MDDE already has building blocks:
- `verified_query` entity in the metadata schema
- `golden_questions.py` in the trust module for AI evaluation
- Semantic layer with metrics, dimensions, and entities

However, these are disconnected. Verified queries aren't attached to semantic models, and there's no retrieval mechanism for similar questions.

## Decision

Make **Verified Queries** a first-class concept in MDDE by:

1. **Attaching verified queries to semantic models** - Each semantic model has an associated collection of verified question/SQL pairs

2. **Similarity-based retrieval** - When answering a new question, retrieve relevant verified examples to inject as context

3. **Feedback loop for auto-promotion** - Automatically identify high-quality queries from usage patterns and promote them to verified status

4. **YAML-native format** - Store verified queries in YAML alongside the semantic model, not just in the database

## Architecture

### 1. Verified Query Structure

```yaml
# semantic_model/verified_queries/customer_metrics.yaml
verified_queries:
  - id: vq_001
    question: "Top 5 customers by revenue last quarter"
    sql: |
      SELECT
        c.customer_name,
        SUM(o.amount) as total_revenue
      FROM dim_customer c
      JOIN fact_orders o ON c.customer_key = o.customer_key
      WHERE o.order_date >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months'
        AND o.order_date < DATE_TRUNC('quarter', CURRENT_DATE)
      GROUP BY c.customer_name
      ORDER BY total_revenue DESC
      LIMIT 5
    intent: ranking
    entities: [dim_customer, fact_orders]
    metrics: [total_revenue]
    verified_by: "data_team"
    verified_at: "2026-03-15"
    tags: [customer, revenue, ranking]

  - id: vq_002
    question: "Monthly order trend for California"
    sql: |
      SELECT
        DATE_TRUNC('month', o.order_date) as month,
        COUNT(*) as order_count,
        SUM(o.amount) as revenue
      FROM fact_orders o
      JOIN dim_customer c ON o.customer_key = c.customer_key
      WHERE c.state = 'California'
      GROUP BY 1
      ORDER BY 1
    intent: trend
    entities: [fact_orders, dim_customer]
    filters: [state_filter]
    verified_by: "analytics_lead"
    verified_at: "2026-03-10"
```

### 2. Semantic Model Integration

```yaml
# semantic_model.yaml
semantic_model:
  name: sales_analytics
  version: "1.2.0"

  # Entities, metrics, dimensions...
  entities:
    - dim_customer
    - fact_orders

  metrics:
    - total_revenue
    - order_count

  # First-class verified queries reference
  verified_queries:
    source: verified_queries/  # Folder with YAML files
    count: 47                   # Auto-updated
    coverage:
      entities: 85%            # % of entities with examples
      metrics: 92%             # % of metrics with examples
      intents: [ranking, trend, comparison, aggregation]
```

### 3. Question Intent Categories

```python
class QuestionIntent(Enum):
    """Standard question intent categories for verified queries."""
    RANKING = "ranking"           # Top N, bottom N
    TREND = "trend"               # Over time patterns
    COMPARISON = "comparison"     # A vs B
    AGGREGATION = "aggregation"   # Sum, count, avg
    FILTERING = "filtering"       # Where conditions
    LOOKUP = "lookup"             # Single record retrieval
    DISTRIBUTION = "distribution" # Breakdown by category
    CORRELATION = "correlation"   # Relationship between metrics
```

### 4. Similarity-Based Retrieval

When a new question arrives:

```python
class VerifiedQueryRetriever:
    def retrieve(
        self,
        question: str,
        semantic_model_id: str,
        top_k: int = 3,
    ) -> List[VerifiedQuery]:
        """
        Retrieve most relevant verified queries for a question.

        Uses:
        1. Embedding similarity (semantic match)
        2. Entity overlap (same tables referenced)
        3. Intent classification (same question type)
        4. Keyword matching (fallback)
        """
```

### 5. Auto-Promotion Pipeline

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Query History  │────►│  Candidate      │────►│   Verified      │
│  (all queries)  │     │  Selection      │     │   Repository    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                              │
                              ▼
                        Criteria:
                        • Frequency > N
                        • Satisfaction > 80%
                        • SQL validates
                        • Results stable
```

## Implementation

### Module Structure

```
src/mdde/semantic/verified/
├── __init__.py
├── types.py              # VerifiedQuery, QuestionIntent, etc.
├── repository.py         # VerifiedQueryRepository manager
├── retriever.py          # Similarity-based retrieval
├── promoter.py           # Auto-promotion from history
├── yaml_loader.py        # Load from YAML files
└── exporter.py           # Export to various formats
```

### Key Classes

```python
@dataclass
class VerifiedQuery:
    """A verified question-SQL pair."""
    query_id: str
    question: str
    sql: str
    intent: QuestionIntent
    semantic_model_id: str
    entities: List[str]
    metrics: List[str] = field(default_factory=list)
    filters: List[str] = field(default_factory=list)
    complexity: str = "medium"  # simple, medium, complex
    verified_by: Optional[str] = None
    verified_at: Optional[datetime] = None
    source: str = "manual"  # manual, auto-promoted, imported
    usage_count: int = 0
    satisfaction_rate: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    embedding: Optional[List[float]] = None  # For similarity search


class VerifiedQueryRepository:
    """
    Manages verified queries for semantic models.

    Usage:
        repo = VerifiedQueryRepository(conn)

        # Add verified query
        repo.add_query(VerifiedQuery(
            question="Top customers by revenue",
            sql="SELECT ...",
            intent=QuestionIntent.RANKING,
            semantic_model_id="sales_model",
            entities=["dim_customer", "fact_orders"],
        ))

        # Retrieve for generation
        examples = repo.retrieve_similar(
            question="Best selling products last month",
            semantic_model_id="sales_model",
            top_k=3
        )

        # Auto-promote from history
        promoted = repo.auto_promote(semantic_model_id="sales_model")
    """
```

### Integration with Text-to-SQL

```python
def generate_sql(question: str, semantic_model_id: str) -> str:
    # 1. Retrieve relevant verified queries
    examples = repository.retrieve_similar(question, semantic_model_id, top_k=3)

    # 2. Build prompt with examples
    prompt = f"""
    Given the semantic model and these verified examples:

    {format_examples(examples)}

    Generate SQL for: {question}
    """

    # 3. Generate with examples as context
    return llm.generate(prompt)
```

## YAML Schema

```yaml
# JSON Schema for verified_queries.yaml
$schema: http://json-schema.org/draft-07/schema#
type: object
properties:
  verified_queries:
    type: array
    items:
      type: object
      required: [id, question, sql, intent, entities]
      properties:
        id:
          type: string
          pattern: "^vq_[a-z0-9]+$"
        question:
          type: string
          minLength: 10
        sql:
          type: string
          minLength: 10
        intent:
          type: string
          enum: [ranking, trend, comparison, aggregation,
                 filtering, lookup, distribution, correlation]
        entities:
          type: array
          items: { type: string }
          minItems: 1
        metrics:
          type: array
          items: { type: string }
        filters:
          type: array
          items: { type: string }
        complexity:
          type: string
          enum: [simple, medium, complex]
        verified_by:
          type: string
        verified_at:
          type: string
          format: date
        tags:
          type: array
          items: { type: string }
```

## Migration

Existing `verified_query` entities will be migrated to the new structure:

```sql
-- Add new columns to verified_query
ALTER TABLE metadata.verified_query ADD COLUMN semantic_model_id VARCHAR;
ALTER TABLE metadata.verified_query ADD COLUMN entities JSON;
ALTER TABLE metadata.verified_query ADD COLUMN metrics JSON;
ALTER TABLE metadata.verified_query ADD COLUMN filters JSON;
ALTER TABLE metadata.verified_query ADD COLUMN embedding JSON;
ALTER TABLE metadata.verified_query ADD COLUMN usage_count INTEGER DEFAULT 0;
ALTER TABLE metadata.verified_query ADD COLUMN satisfaction_rate DOUBLE;
ALTER TABLE metadata.verified_query ADD COLUMN source VARCHAR DEFAULT 'manual';
```

## Benefits

1. **Improved Accuracy** - Examples provide concrete patterns for the LLM to follow
2. **Faster Generation** - Similar queries can be adapted rather than generated from scratch
3. **Knowledge Capture** - Tribal knowledge encoded in verified queries
4. **Continuous Improvement** - Auto-promotion creates a feedback loop
5. **Standard Format** - YAML-native, version-controlled, portable

## Related ADRs

- ADR-301: Semantic Layer Module
- ADR-374: AI Trust Layer (Golden Questions)
- ADR-246: AI Agent Context
- ADR-372: Intelligent Semantic Layer

## References

- [Snowflake Verified Query Examples](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst#verified-query-examples)
- [Open Semantic Interchange](https://github.com/open-semantic-interchange/osi-spec)
