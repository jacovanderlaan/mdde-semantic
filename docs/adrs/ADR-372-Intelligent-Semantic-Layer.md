# ADR-372: Intelligent Semantic Layer

## Status
Proposed

## Context

Inspired by Simon Späti's article "Beyond Semantic Layers: Building an Intelligent Data Architecture That Explains 'Why'", this ADR proposes unifying MDDE's existing capabilities into a cohesive **Intelligent Semantic Layer** that combines three architectural layers:

1. **Technical Layer** (metadata catalog) - ADR-244 Ontology
2. **Semantic Layer** (metric definitions) - ADR-301 Semantic Layer, ADR-245 Metrics
3. **Ontology Layer** (business knowledge) - ADR-359 Business Ontology, ADR-364 Executable Ontology

### The Problem: "What" vs "Why"

Traditional semantic layers answer questions like:
- "What is revenue?" → `SUM(order_amount) - SUM(refund_amount)`
- "What was revenue last month?" → `$2.3M`

But they cannot answer:
- "Why did revenue drop 20%?"
- "What factors typically cause revenue changes?"
- "Is this revenue trend good or bad given our business context?"

### Simon Späti's Insight

> "The future of analytics isn't just semantic layers. It's intelligent architectures that combine technical metadata, business semantics, and formal ontologies with inference rules."

The key innovation is **inference rules** encoded in OWL/RDF format that enable AI agents to:
1. Trace causal relationships between metrics
2. Explain patterns using business knowledge
3. Suggest actions based on encoded business rules

### MDDE's Current State

| Capability | Module | Status |
|------------|--------|--------|
| Technical metadata | `mdde.internal` | Complete |
| Semantic metrics | `mdde.semantic.metrics` | Complete |
| Business ontology | `mdde.semantic.ontology.business_ontology` | Complete |
| Executable ontology | `mdde.semantic.ontology.executable` | Complete |
| Causal relationships | `CausalRelationship` | Complete |
| Metric interpretation | `MetricInterpretation` | Complete |

What's missing is the **integration layer** that combines these into a unified "Intelligent Semantic Layer" with:
1. Inference rules for automated reasoning
2. AI agent tools that leverage all three layers
3. Natural language query interface with "why" explanations

## Decision

### 1. Inference Rules

Add formal inference rules that enable automated reasoning:

```python
@dataclass
class InferenceRule:
    """
    A formal inference rule for automated reasoning.

    Encodes business knowledge as IF-THEN rules that can be
    evaluated programmatically or by AI agents.
    """
    rule_id: str
    name: str
    description: str

    # Rule structure
    condition: str  # SPARQL-like condition or Python expression
    conclusion: str  # What to infer when condition is true

    # Context
    domain: str = ""  # Business domain this applies to
    priority: int = 0  # Higher = evaluated first
    confidence: float = 1.0  # Rule reliability

    # Evidence
    supporting_metrics: List[str] = field(default_factory=list)
    supporting_relationships: List[str] = field(default_factory=list)
    evidence_sources: List[str] = field(default_factory=list)


class InferenceEngine:
    """
    Execute inference rules against semantic context.

    Bridges the gap between "what" and "why".
    """

    def infer(
        self,
        observation: Dict[str, Any],
        context: str,
    ) -> List[InferenceResult]:
        """
        Apply inference rules to an observation.

        Args:
            observation: Data observation (e.g., {"revenue": -20, "tickets": -45})
            context: Business context identifier

        Returns:
            List of inferences with explanations
        """
```

### 2. Unified Query Interface

Single entry point that combines all three layers:

```python
class IntelligentSemanticLayer:
    """
    Unified interface to the Intelligent Semantic Layer.

    Combines:
    - Technical metadata (entities, attributes, lineage)
    - Semantic definitions (metrics, dimensions, hierarchies)
    - Business ontology (concepts, causality, interpretations)
    - Inference rules (automated reasoning)
    """

    def __init__(
        self,
        conn,
        semantic_model_id: Optional[str] = None,
        business_ontology_id: Optional[str] = None,
    ):
        self.metadata = MetadataManager(conn)
        self.semantic = SemanticLayerManager(conn)
        self.ontology = BusinessOntologyManager(conn)
        self.inference = InferenceEngine()

    def query(
        self,
        metrics: List[str],
        dimensions: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        include_explanation: bool = True,
    ) -> IntelligentQueryResult:
        """
        Execute a semantic query with optional explanation.

        Returns data + context + "why" explanation.
        """

    def explain(
        self,
        observation: Dict[str, Any],
        context: str,
    ) -> Explanation:
        """
        Explain an observation using business ontology.

        This is the key "why" function.
        """

    def suggest_actions(
        self,
        observation: Dict[str, Any],
        context: str,
    ) -> List[SuggestedAction]:
        """
        Suggest actions based on observation and business rules.
        """
```

### 3. AI Agent Integration

MCP tools that expose the intelligent layer to AI agents:

```python
# MCP tool for AI agents
@mcp_tool("semantic_query")
def semantic_query(
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    explain: bool = True,
) -> Dict[str, Any]:
    """
    Query the semantic layer with optional explanation.

    Args:
        metrics: Metrics to query (e.g., ["revenue", "order_count"])
        dimensions: Dimensions to group by (e.g., ["region", "month"])
        explain: Whether to include "why" explanation

    Returns:
        {
            "data": [...],
            "explanation": {
                "metrics_used": [...],
                "business_context": "...",
                "causal_factors": [...],
                "recommended_interpretation": "..."
            }
        }
    """

@mcp_tool("explain_metric_change")
def explain_metric_change(
    metric: str,
    change_percent: float,
    context: str,
) -> Dict[str, Any]:
    """
    Explain why a metric changed.

    Uses business ontology to provide contextual interpretation.

    Returns:
        {
            "sentiment": "warning",
            "explanation": "In white-glove enterprise model, reduced
                           support tickets indicates customer disengagement",
            "causal_chain": ["tickets → engagement → churn_risk"],
            "suggested_actions": ["Proactive outreach", "Health score review"],
            "confidence": 0.85
        }
    """
```

### 4. Example: NYC Taxi Use Case

Following Simon Späti's NYC Taxi example:

```yaml
# ontology/taxi_analytics.ontology.yaml
ontology:
  name: NYC Taxi Analytics
  domain: transportation

  concepts:
    - id: trip
      name: Taxi Trip
      definition: A completed taxi journey from pickup to dropoff

    - id: fare
      name: Trip Fare
      definition: Total charge for a taxi trip
      related_concepts: [trip, tip, surcharge]

    - id: tip
      name: Driver Tip
      definition: Gratuity paid to driver

    - id: demand
      name: Demand
      definition: Volume of ride requests in an area

  # Causal relationships
  relationships:
    - source: weather
      target: demand
      type: causes
      direction: conditional
      rule: "rainy_weather → higher_demand"

    - source: surge_pricing
      target: fare
      type: causes
      direction: positive
      rule: "high_demand → surge → higher_fare"

    - source: fare
      target: tip
      type: correlates
      direction: positive
      rule: "higher_fare → higher_tip_amount"

  # Inference rules
  inference_rules:
    - id: rain_demand_rule
      name: Weather Impact on Demand
      condition: |
        weather.precipitation > 0.1 AND hour IN (7,8,9,17,18,19)
      conclusion: |
        demand INCREASE 20-40%
      explanation: |
        During rush hours, rain causes people to prefer taxis over
        walking or public transit, increasing demand by 20-40%.

    - id: surge_explanation
      name: Surge Pricing Trigger
      condition: |
        demand.current > demand.average * 1.5
      conclusion: |
        surge_multiplier = 1.0 + ((demand.current / demand.average) - 1) * 0.5
      explanation: |
        When demand exceeds 150% of average, surge pricing activates
        to balance supply and demand.

  # Metric interpretations
  interpretations:
    - metric: average_fare
      context: rush_hour
      increase_means: |
        Higher fares during rush hour typically indicate surge pricing
        due to demand exceeding supply. This is normal market behavior.
      decrease_means: |
        Lower fares during rush hour are unusual. May indicate
        increased competition, promotions, or data quality issues.
```

**Query with explanation:**

```python
from mdde.semantic import IntelligentSemanticLayer

isl = IntelligentSemanticLayer(conn)

# Simple query
result = isl.query(
    metrics=["average_fare", "trip_count"],
    dimensions=["hour", "weather_condition"],
    filters={"date": "2026-03-13"},
    include_explanation=True,
)

# Result includes "why"
print(result.explanation)
# {
#   "pattern_detected": "Fare spike at 5-6 PM during rain",
#   "causal_chain": [
#     "Rain during rush hour → Increased demand",
#     "Demand > 1.5x average → Surge pricing activated",
#     "Surge pricing → Higher fares"
#   ],
#   "business_context": "This is expected behavior per surge_pricing_rule",
#   "confidence": 0.92
# }
```

### 5. Architecture

```
         Intelligent Semantic Layer Architecture
    ========================================================

    ┌────────────────────────────────────────────────────────┐
    │                    User / AI Agent                      │
    │    "Why did taxi fares spike yesterday evening?"       │
    └─────────────────────────┬──────────────────────────────┘
                              │
                              ▼
    ┌────────────────────────────────────────────────────────┐
    │              Intelligent Semantic Layer                 │
    │                 (IntelligentSemanticLayer)             │
    │  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐ │
    │  │  Query   │  │ Explain  │  │  Suggest Actions     │ │
    │  │  Engine  │  │  Engine  │  │      Engine          │ │
    │  └────┬─────┘  └────┬─────┘  └──────────┬───────────┘ │
    │       │             │                    │             │
    └───────┼─────────────┼────────────────────┼─────────────┘
            │             │                    │
    ┌───────▼─────────────▼────────────────────▼─────────────┐
    │                  Inference Engine                       │
    │                                                         │
    │  ┌─────────────────────────────────────────────────┐   │
    │  │              Inference Rules                     │   │
    │  │  IF condition THEN conclusion WITH confidence    │   │
    │  └─────────────────────────────────────────────────┘   │
    │                                                         │
    │  ┌─────────────────────────────────────────────────┐   │
    │  │           Causal Chain Traversal                 │   │
    │  │  weather → demand → surge → fare → tip           │   │
    │  └─────────────────────────────────────────────────┘   │
    └───────────────────────────┬─────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       ▼
    ┌────────────┐      ┌──────────────┐      ┌──────────────┐
    │ Technical  │      │   Semantic   │      │   Business   │
    │   Layer    │      │    Layer     │      │   Ontology   │
    │            │      │              │      │              │
    │ - Entities │      │ - Metrics    │      │ - Concepts   │
    │ - Attrs    │      │ - Dimensions │      │ - Causality  │
    │ - Lineage  │      │ - Hierarchies│      │ - Interpret. │
    │ - Quality  │      │ - Goals      │      │ - Rules      │
    └──────┬─────┘      └──────┬───────┘      └──────┬───────┘
           │                   │                     │
           │   ADR-244/246     │   ADR-301/245       │   ADR-359/364
           │                   │                     │
           └───────────────────┴─────────────────────┘
                               │
                               ▼
                    ┌──────────────────┐
                    │   DuckDB / Data  │
                    │    Warehouse     │
                    └──────────────────┘
```

## Integration with Existing ADRs

| ADR | Integration Point |
|-----|-------------------|
| ADR-244 | Ontology concepts as foundation |
| ADR-245 | Metrics manager provides semantic definitions |
| ADR-246 | Agent context provides knowledge planes |
| ADR-301 | Semantic layer model and query engine |
| ADR-359 | Business ontology provides causal relationships |
| ADR-364 | Executable ontology provides SQL generation |
| ADR-371 | Semantic layer integrations (Cube, MetricFlow) |

## Implementation Plan

### Phase 1: Inference Engine
- [ ] `InferenceRule` dataclass
- [ ] `InferenceEngine` class with rule evaluation
- [ ] `InferenceResult` with confidence and chain
- [ ] Tests for rule evaluation

### Phase 2: Unified Interface
- [ ] `IntelligentSemanticLayer` class
- [ ] `query()` method with explanation
- [ ] `explain()` method for observations
- [ ] `suggest_actions()` method

### Phase 3: AI Agent Tools
- [ ] MCP tool `semantic_query`
- [ ] MCP tool `explain_metric_change`
- [ ] MCP tool `list_causal_factors`
- [ ] Integration with Claude Agent SDK

### Phase 4: YAML Ontology Format
- [ ] Ontology YAML schema
- [ ] Inference rule YAML syntax
- [ ] Loader and validator
- [ ] Examples (taxi, retail, SaaS)

## Consequences

### Positive

- **Unified Interface**: Single entry point to all semantic capabilities
- **AI-Ready**: AI agents can explain "why", not just "what"
- **Business Context**: Formal encoding of business knowledge
- **Reduced Hallucination**: AI answers grounded in encoded rules
- **Audit Trail**: Reasoning chains are traceable and explainable

### Negative

- **Complexity**: Another abstraction layer
- **Maintenance**: Inference rules must be kept current
- **Expertise Required**: Creating good ontologies requires domain knowledge

### Neutral

- **Optional**: Teams can use basic semantic layer without inference
- **Gradual Adoption**: Start with key metrics, expand over time

## References

- [Simon Späti: Beyond Semantic Layers](https://www.ssp.sh/blog/intelligent-semantic-layer/)
- [dltHub: Ontology driven Dimensional Modeling](https://dlthub.com/blog/ontology)
- [Patrick Okare: Five Must-Have Layers](https://medium.com/@patrickt.okare)
- MDDE ADR-244: Ontology Support
- MDDE ADR-245: Metrics Layer
- MDDE ADR-301: Semantic Layer Module
- MDDE ADR-359: Business Ontology Layer
- MDDE ADR-364: SQL-Executable Ontology
- MDDE ADR-371: Semantic Layer Enhancements
