# ADR-359: Business Ontology Layer

## Status
Proposed

## Context

Based on dltHub's article ["Ontology driven Dimensional Modeling"](https://dlthub.com/blog/ontology):

> "Data Models tell you **What**; Ontologies tell you **Why**."

MDDE already has strong ontology support (ADR-244) with OWL/RDF/SKOS concepts, but this is primarily **structural** - it captures what entities are and how they relate. However, for AI-driven analytics, we need to capture:

1. **Causal Relationships** - Why things connect, not just that they connect
2. **Contextual Interpretation** - How to interpret metrics in different business contexts
3. **Reasoning Rules** - Business logic that transcends data structure

### The Semantic Gap Problem

| Layer | Domain | Purpose |
|-------|--------|---------|
| **Ontology** | Real world | Blueprint of truth - why things exist |
| **Knowledge Graph** | Unstructured data | Node/edge representation |
| **Canonical Model** | Structured data | Unified data view |
| **Dimensional Model** | Analytics | Aggregate metrics |

Questions address the real world, but data uses a different model. Without ontological context, AI agents remain limited to reporting functions.

### The "Mundane Misdirection" Test

Support tickets drop 45%, response times halve, CSAT improves.

- **AI without ontology**: Celebrates success, recommends scaling back support
- **AI with ontology** (knowing "white-glove enterprise model"): Detects customer disengagement, flags critical issue

The same metrics mean opposite things depending on business context.

## Decision

Add a Business Ontology Layer that extends ADR-244's structural ontology with:

1. **Business Concepts** with causal relationships
2. **Context-Aware Metric Interpretation**
3. **Reasoning Rules** for AI guidance
4. **Ontology Questionnaire** for domain capture

### Implementation

#### 1. Business Concept Model

```python
@dataclass
class BusinessConcept:
    """A business concept with causal context."""
    concept_id: str
    name: str
    definition: str
    business_model: str  # e.g., "white_glove_enterprise", "self_service_retail"
    causal_relationships: List[CausalRelationship]
    success_indicators: List[str]
    failure_indicators: List[str]

@dataclass
class CausalRelationship:
    """A causal relationship between concepts."""
    source_concept: str
    target_concept: str
    relationship_type: CausalType  # CAUSES, INDICATES, PREDICTS, CORRELATES
    direction: str  # positive, negative, conditional
    context: str  # When this relationship holds
```

#### 2. Metric Interpretation

```python
@dataclass
class MetricInterpretation:
    """Context-aware metric interpretation."""
    metric_id: str
    context: str  # Business context identifier
    increase_means: str  # What an increase indicates
    decrease_means: str  # What a decrease indicates
    thresholds: Dict[str, Threshold]  # Named thresholds
    recommended_actions: Dict[str, str]  # Situation -> action
```

#### 3. Business Ontology Manager

```python
class BusinessOntologyManager:
    """Manage business concepts and interpretations."""

    def interpret_metric_change(
        self,
        metric_id: str,
        change_percent: float,
        context: str,
    ) -> MetricInterpretationResult:
        """
        Interpret a metric change in business context.

        Returns interpretation with:
        - Sentiment (positive/negative/warning)
        - Explanation based on business model
        - Recommended actions
        - Confidence score
        """
```

### Use Cases

#### 1. AI-Guided Analytics

```python
from mdde.semantic.ontology import BusinessOntologyManager

manager = BusinessOntologyManager(conn)

# Load business ontology
manager.load_ontology("enterprise_crm.ontology.yaml")

# Interpret metric change
result = manager.interpret_metric_change(
    metric_id="customer_support_tickets",
    change_percent=-45,
    context="enterprise_white_glove",
)

# Returns:
# {
#   "sentiment": "WARNING",
#   "explanation": "In white-glove model, reduced tickets signals disengagement",
#   "recommended_action": "Proactive outreach to check satisfaction",
#   "confidence": 0.85
# }
```

#### 2. Ontology Generation

```python
from mdde.semantic.ontology import OntologyQuestionnaire

questionnaire = OntologyQuestionnaire()

# Generate business-specific questions
questions = questionnaire.generate_questions("retail_ecommerce")

# Q1: What is your primary business model?
# Q2: Who are your customers?
# Q3: What does success look like?
# Q4: How do you measure customer health?
# ...

# Process answers into ontology
ontology = questionnaire.process_answers(answers)
```

#### 3. GenAI Integration

```python
from mdde.genai import OntologyAwareTranspiler

transpiler = OntologyAwareTranspiler(
    conn,
    ontology_path="business.ontology.yaml",
)

# Generate transformation with ontology context
result = transpiler.generate_transformation(
    source="raw_support_tickets",
    target="fact_customer_engagement",
    prompt="Calculate customer engagement score",
)

# Ontology ensures:
# - Correct interpretation of "engagement" for business model
# - Appropriate aggregation granularity
# - Causal relationships in derived metrics
```

### Integration Points

| Existing Feature | Enhancement |
|-----------------|-------------|
| ADR-244 Ontology | Add causal layer above structural |
| ADR-301 Semantic Layer | Business-aware metric definitions |
| ADR-302 Confidence | Context-based confidence scoring |
| ADR-357 Business Context | Extend with causal relationships |
| GenAI Module | Ontology-guided generation |

## Consequences

### Positive

- **AI-Ready Analytics**: Enables LLMs to reason about business context
- **Reduced Hallucination**: Ontology provides ground truth for interpretation
- **Standardized Context**: Business rules captured once, applied everywhere
- **Audit Trail**: Reasoning chain documented for compliance

### Negative

- **Setup Effort**: Requires business user input to create ontology
- **Maintenance**: Ontology must evolve with business model changes
- **Complexity**: Another layer in the semantic stack

### Neutral

- **Optional Feature**: Teams can use MDDE without business ontology
- **Gradual Adoption**: Can start with key metrics, expand over time

## Implementation Plan

### Phase 1: Core Models
- [x] BusinessConcept, CausalRelationship models
- [x] MetricInterpretation model
- [x] BusinessOntologyManager class

### Phase 2: Interpretation Engine
- [x] interpret_metric_change() method
- [x] Context matching logic
- [x] Recommendation generation

### Phase 3: Questionnaire
- [x] OntologyQuestionnaire class
- [x] Industry-specific question templates
- [x] Answer-to-ontology processor

### Phase 4: Integration
- [ ] GenAI transpiler integration (ADR-363)
- [ ] Semantic layer hooks
- [ ] Deepnote dashboard export

## References

- [dltHub: Ontology driven Dimensional Modeling](https://dlthub.com/blog/ontology)
- [dltHub: Building Semantic Models with LLMs](https://dlthub.com/blog/building-semantic-models-with-llms-and-dlt)
- [Ontologies, Context Graphs, and Semantic Layers](https://metadataweekly.substack.com/p/ontologies-context-graphs-and-semantic)
- MDDE ADR-244: Ontology Support
- MDDE ADR-301: Semantic Layer
- MDDE ADR-357: Business Context Templates
