# ADR-247: Aboutness Layer - Semantic Intent and Purpose

**Status**: Implemented
**Date**: 2026-02-16
**Author**: MDDE Team
**Category**: Semantic
**Inspired By**: [Juha Korpela on Aboutness](https://www.linkedin.com/posts/jkorpela_aboutness-is-a-cool-term-that-i-stumbled-activity-7429058205768556544-OCxr), Juan Sequeda's "Mappings are where the semantics live"

## Context

Data modeling traditionally focuses on **what data is** (schema, types, constraints) but often neglects **what data means** (semantic intent, purpose, real-world representation). This gap creates challenges:

1. **Lost context**: Developers inherit data models without understanding why entities exist
2. **Semantic drift**: Over time, the meaning of attributes diverges from original intent
3. **Integration failures**: Systems with same names but different meanings create mapping errors
4. **AI misinterpretation**: LLMs struggle to reason about data without explicit semantic context

"Aboutness" is the semantic link between the **Data Plane** (raw information) and the **Knowledge Plane** (conceptual understanding). As Juan Sequeda notes: "Mappings are where the semantics live."

### MDDE's Current Semantic Capabilities

MDDE already has strong semantic foundations:
- **Ontology Manager** (ADR-244): OWL concepts, properties, hierarchies
- **Glossary System**: Business terms with definitions and synonyms
- **Semantic Types Domain**: 38 classification categories
- **Metrics Layer** (ADR-245): Business metrics with semantic abstraction
- **AI Agent Context** (ADR-246): LLM-friendly knowledge representation
- **KDE Management**: Key Data Elements with criticality tracking

**Gap**: These are disconnected semantic islands. We lack:
- Explicit **intent** and **purpose** for entities/attributes
- **Aboutness dimensions** that classify what data represents
- **Semantic dependency** tracking separate from technical lineage
- **Aboutness validation** ensuring semantic consistency

## Decision

Implement an **Aboutness Layer** that explicitly captures the semantic intent, purpose, and meaning of data elements.

### 1. Core Concepts

| Concept | Definition | Example |
|---------|------------|---------|
| **Intent** | Why this data element exists | "Tracks customer purchase value for revenue analysis" |
| **Represents** | What real-world concept it models | "Customer.email represents ContactMethod.Electronic" |
| **Aboutness Dimension** | Category of semantic meaning | measure, identifier, classifier, temporal, relationship |
| **Purpose** | Business use cases | ["revenue_calculation", "customer_segmentation"] |
| **Semantic Role** | Function in business context | aggregatable, sliceable, filterable, groupable |

### 2. Aboutness Dimensions

```yaml
# Domain: aboutness_dimension
domain:
  domain_id: aboutness_dimension
  name: Aboutness Dimension
  description: Classification of what data represents semantically

  values:
    # What is being measured/quantified
    - value: measure
      description: Numeric value capturing magnitude, amount, or quantity
      examples: ["revenue", "quantity", "score", "rate"]

    # What uniquely identifies something
    - value: identifier
      description: Uniquely distinguishes one instance from another
      examples: ["customer_id", "order_number", "SSN"]

    # What category/classification
    - value: classifier
      description: Groups or categorizes entities
      examples: ["status", "type", "category", "segment"]

    # What point or span of time
    - value: temporal
      description: Captures when something occurred or applies
      examples: ["created_at", "effective_date", "period"]

    # What connection to other entities
    - value: relationship
      description: Establishes connection between entities
      examples: ["parent_id", "manager_id", "belongs_to"]

    # What quality/characteristic
    - value: quality
      description: Describes a characteristic or attribute
      examples: ["name", "description", "color", "size"]

    # What location/geography
    - value: spatial
      description: Represents physical or logical location
      examples: ["address", "region", "coordinates"]

    # What state/condition
    - value: state
      description: Current condition or lifecycle stage
      examples: ["is_active", "approval_status", "phase"]
```

### 3. Database Schema

```sql
-- Entity-level aboutness
CREATE TABLE metadata.entity_aboutness (
    aboutness_id        VARCHAR(100) PRIMARY KEY,
    entity_id           VARCHAR(100) NOT NULL,
    model_id            VARCHAR(100),

    -- Purpose & Intent
    purpose             TEXT,           -- Why this entity exists
    business_context    TEXT,           -- Business domain context
    real_world_object   VARCHAR(200),   -- What it represents (e.g., "Customer", "Transaction")

    -- Classification
    aboutness_dimension VARCHAR(50),    -- Primary dimension: classifier, measure, etc.
    semantic_category   VARCHAR(100),   -- Business category

    -- Usage
    business_use_cases  JSON,           -- ["revenue_analysis", "customer_segmentation"]
    stakeholder_groups  JSON,           -- ["finance", "marketing"]

    -- Ontology Links
    represents_concept  VARCHAR(200),   -- Link to ontology concept
    equivalent_to       JSON,           -- External concept URIs

    -- Metadata
    confidence_score    DECIMAL(3,2),   -- 0.00-1.00 confidence in semantic assignment
    source              VARCHAR(50),    -- manual, inferred, imported
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by          VARCHAR(100),

    UNIQUE(entity_id, model_id)
);

-- Attribute-level aboutness
CREATE TABLE metadata.attribute_aboutness (
    aboutness_id        VARCHAR(100) PRIMARY KEY,
    entity_id           VARCHAR(100) NOT NULL,
    attribute_id        VARCHAR(100) NOT NULL,
    model_id            VARCHAR(100),

    -- Purpose & Intent
    intent              TEXT,           -- Why this attribute exists
    measures_what       TEXT,           -- For measures: what is being quantified
    identifies_what     TEXT,           -- For identifiers: what is being identified
    classifies_what     TEXT,           -- For classifiers: what is being categorized

    -- Classification
    aboutness_dimension VARCHAR(50) NOT NULL,  -- measure, identifier, classifier, etc.
    semantic_type       VARCHAR(100),   -- From semantic_type domain
    semantic_role       VARCHAR(50),    -- aggregatable, sliceable, filterable

    -- Real-world mapping
    represents_property VARCHAR(200),   -- Link to ontology property
    canonical_name      VARCHAR(200),   -- Standardized name across systems

    -- Validation rules derived from aboutness
    expected_behavior   JSON,           -- {"aggregation": "SUM", "nullability": "required"}

    -- Lineage of meaning
    derived_from        VARCHAR(100),   -- Another attribute this meaning derives from
    semantic_transform  TEXT,           -- How meaning transforms from source

    -- Metadata
    confidence_score    DECIMAL(3,2),
    source              VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(entity_id, attribute_id, model_id)
);

-- Semantic dependencies (separate from technical lineage)
CREATE TABLE metadata.semantic_dependency (
    dependency_id       VARCHAR(100) PRIMARY KEY,
    source_concept      VARCHAR(200) NOT NULL,  -- Concept or attribute
    target_concept      VARCHAR(200) NOT NULL,  -- Concept or attribute
    dependency_type     VARCHAR(50) NOT NULL,   -- requires, implies, conflicts, refines
    strength            VARCHAR(20),            -- strong, moderate, weak
    description         TEXT,
    model_id            VARCHAR(100),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aboutness validation results
CREATE TABLE metadata.aboutness_validation (
    validation_id       VARCHAR(100) PRIMARY KEY,
    entity_id           VARCHAR(100),
    attribute_id        VARCHAR(100),
    model_id            VARCHAR(100),
    check_code          VARCHAR(20) NOT NULL,   -- A001, A002, etc.
    severity            VARCHAR(20) NOT NULL,   -- error, warning, info
    message             TEXT NOT NULL,
    recommendation      TEXT,
    validated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 4. AboutnessManager Class

```python
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum

class AboutnessDimension(Enum):
    """What data semantically represents."""
    MEASURE = "measure"           # Quantifies something
    IDENTIFIER = "identifier"     # Uniquely identifies
    CLASSIFIER = "classifier"     # Categorizes/groups
    TEMPORAL = "temporal"         # Time-related
    RELATIONSHIP = "relationship" # Connection to other entities
    QUALITY = "quality"           # Characteristic/attribute
    SPATIAL = "spatial"           # Location/geography
    STATE = "state"               # Condition/lifecycle

class SemanticRole(Enum):
    """How data can be used semantically."""
    AGGREGATABLE = "aggregatable"   # Can be summed/averaged
    SLICEABLE = "sliceable"         # Can filter data
    GROUPABLE = "groupable"         # Can group by
    SORTABLE = "sortable"           # Meaningful sort order
    JOINABLE = "joinable"           # Can join with other entities
    DERIVABLE = "derivable"         # Source for calculations

@dataclass
class EntityAboutness:
    """Semantic intent for an entity."""
    entity_id: str
    purpose: str                              # Why it exists
    real_world_object: str                    # What it represents
    aboutness_dimension: AboutnessDimension
    business_use_cases: List[str] = field(default_factory=list)
    represents_concept: Optional[str] = None  # Ontology concept
    confidence_score: float = 1.0

@dataclass
class AttributeAboutness:
    """Semantic intent for an attribute."""
    entity_id: str
    attribute_id: str
    intent: str                               # Why it exists
    aboutness_dimension: AboutnessDimension
    semantic_role: SemanticRole
    measures_what: Optional[str] = None       # For measures
    identifies_what: Optional[str] = None     # For identifiers
    classifies_what: Optional[str] = None     # For classifiers
    represents_property: Optional[str] = None # Ontology property
    canonical_name: Optional[str] = None      # Cross-system standard name
    confidence_score: float = 1.0

class AboutnessManager:
    """Manage semantic aboutness for entities and attributes."""

    def __init__(self, conn):
        self.conn = conn

    # CRUD Operations
    def set_entity_aboutness(self, aboutness: EntityAboutness) -> str:
        """Set aboutness for an entity."""
        pass

    def set_attribute_aboutness(self, aboutness: AttributeAboutness) -> str:
        """Set aboutness for an attribute."""
        pass

    def get_entity_aboutness(self, entity_id: str) -> Optional[EntityAboutness]:
        """Get aboutness for an entity."""
        pass

    def get_attribute_aboutness(
        self, entity_id: str, attribute_id: str
    ) -> Optional[AttributeAboutness]:
        """Get aboutness for an attribute."""
        pass

    # Discovery & Inference
    def infer_aboutness(self, entity_id: str) -> List[AttributeAboutness]:
        """Infer aboutness from naming patterns and semantic types."""
        pass

    def suggest_canonical_names(
        self, entity_id: str
    ) -> Dict[str, List[str]]:
        """Suggest canonical names for attributes based on patterns."""
        pass

    # Query & Analysis
    def find_by_dimension(
        self, dimension: AboutnessDimension
    ) -> List[AttributeAboutness]:
        """Find all attributes with a given aboutness dimension."""
        pass

    def find_by_concept(self, concept: str) -> List[EntityAboutness]:
        """Find entities representing a concept."""
        pass

    def get_semantic_dependencies(
        self, concept: str
    ) -> List[Dict[str, Any]]:
        """Get semantic dependencies for a concept."""
        pass

    # Validation
    def validate_aboutness(self, model_id: str) -> List[Dict[str, Any]]:
        """Validate semantic consistency of aboutness assignments."""
        pass

    def detect_conflicts(self, model_id: str) -> List[Dict[str, Any]]:
        """Detect semantic conflicts (e.g., identifier marked as aggregatable)."""
        pass
```

### 5. Aboutness YAML Schema

```yaml
# Entity with explicit aboutness
entity:
  entity_id: customer_order
  name: Customer Order
  description: Records of customer purchases

  aboutness:
    purpose: >
      Tracks individual customer purchase transactions to support
      revenue analysis, inventory management, and customer behavior insights.
    real_world_object: PurchaseTransaction
    dimension: classifier  # This entity classifies transactions
    business_use_cases:
      - revenue_reporting
      - customer_segmentation
      - inventory_forecasting
    stakeholder_groups:
      - finance
      - sales
      - operations
    represents_concept: schema:Order  # Link to schema.org

  attributes:
    - attribute_id: order_id
      name: Order ID
      data_type: VARCHAR(50)
      aboutness:
        intent: Uniquely identifies each order across all systems
        dimension: identifier
        identifies_what: PurchaseTransaction
        semantic_role: joinable
        canonical_name: transaction_identifier
        represents_property: schema:orderNumber

    - attribute_id: order_total
      name: Order Total
      data_type: DECIMAL(18,2)
      aboutness:
        intent: Captures monetary value of the complete order including tax and shipping
        dimension: measure
        measures_what: TransactionValue
        semantic_role: aggregatable
        canonical_name: transaction_amount
        expected_behavior:
          aggregation: SUM
          nullability: required
          positive_only: true
        represents_property: schema:totalPrice

    - attribute_id: order_status
      name: Order Status
      data_type: VARCHAR(20)
      aboutness:
        intent: Tracks current lifecycle stage of the order
        dimension: state
        classifies_what: OrderLifecycleStage
        semantic_role: sliceable
        canonical_name: order_state
        valid_transitions:
          - pending -> confirmed
          - confirmed -> shipped
          - shipped -> delivered

    - attribute_id: customer_id
      name: Customer ID
      data_type: VARCHAR(50)
      aboutness:
        intent: Links order to the purchasing customer
        dimension: relationship
        identifies_what: Customer
        semantic_role: joinable
        canonical_name: customer_identifier
        relationship_target: customer
```

### 6. Aboutness Validation Checks

| Check | Code | Severity | Description |
|-------|------|----------|-------------|
| Missing Purpose | A001 | Warning | Entity has no explicit purpose defined |
| Conflicting Dimension | A002 | Error | Attribute claims incompatible dimensions |
| Aggregation Mismatch | A003 | Error | Identifier marked as aggregatable |
| Missing Canonical Name | A004 | Info | No cross-system standard name defined |
| Orphaned Semantic | A005 | Warning | Aboutness references non-existent concept |
| Incomplete Coverage | A006 | Info | Entity has attributes without aboutness |
| Semantic Drift | A007 | Warning | Aboutness differs from glossary definition |
| Role Conflict | A008 | Error | Incompatible semantic roles assigned |

### 7. Integration Points

#### With Existing Semantic Infrastructure

```python
# Link aboutness to ontology concepts
def link_to_ontology(self, entity_id: str, concept_uri: str):
    """Link entity aboutness to formal ontology concept."""
    aboutness = self.get_entity_aboutness(entity_id)
    aboutness.represents_concept = concept_uri

    # Auto-infer aboutness dimension from concept type
    concept = self.ontology_manager.get_concept(concept_uri)
    if concept.is_subclass_of("schema:QuantitativeValue"):
        aboutness.aboutness_dimension = AboutnessDimension.MEASURE

# Enrich glossary with aboutness
def sync_with_glossary(self, model_id: str):
    """Synchronize aboutness with glossary terms."""
    for term in self.glossary_manager.list_terms(model_id):
        for link in term.attribute_links:
            aboutness = self.get_attribute_aboutness(link.entity_id, link.attribute_id)
            if aboutness and aboutness.intent != term.definition:
                self.report_semantic_drift(link, term)
```

#### With AI Agent Context

```python
# Provide aboutness context to LLM
def get_agent_context(self, entity_id: str) -> Dict[str, Any]:
    """Build LLM-friendly aboutness context."""
    aboutness = self.get_entity_aboutness(entity_id)

    return {
        "entity": entity_id,
        "purpose": aboutness.purpose,
        "represents": aboutness.real_world_object,
        "semantic_dimension": aboutness.aboutness_dimension.value,
        "business_uses": aboutness.business_use_cases,
        "attributes": [
            {
                "name": attr.attribute_id,
                "intent": attr.intent,
                "dimension": attr.aboutness_dimension.value,
                "measures": attr.measures_what,
                "identifies": attr.identifies_what,
            }
            for attr in self.get_all_attribute_aboutness(entity_id)
        ]
    }
```

### 8. CLI Commands

```bash
# Set entity aboutness
mdde aboutness set-entity customer_order \
  --purpose "Tracks customer purchase transactions" \
  --represents "PurchaseTransaction" \
  --dimension measure

# Set attribute aboutness
mdde aboutness set-attribute customer_order.order_total \
  --intent "Captures monetary value of complete order" \
  --dimension measure \
  --measures "TransactionValue" \
  --role aggregatable

# Infer aboutness from patterns
mdde aboutness infer --model sales_analytics

# Validate aboutness consistency
mdde aboutness validate --model sales_analytics

# Find all measures in model
mdde aboutness find --dimension measure --model sales_analytics

# Export aboutness to YAML
mdde aboutness export --model sales_analytics --output aboutness.yaml
```

## Implementation

### Phase 1: Core Infrastructure (Week 1)
- [ ] Add `aboutness_dimension` domain
- [ ] Create database tables: `entity_aboutness`, `attribute_aboutness`
- [ ] Implement `AboutnessManager` with CRUD operations
- [ ] Add aboutness properties to YAML schema

### Phase 2: Inference & Validation (Week 2)
- [ ] Implement pattern-based aboutness inference
- [ ] Create validation checks (A001-A008)
- [ ] Add semantic dependency tracking
- [ ] Integrate with existing glossary and KDE systems

### Phase 3: Integration (Week 3)
- [ ] Connect to OntologyManager for concept linking
- [ ] Extend AI Agent context with aboutness
- [ ] Add aboutness to knowledge graph export
- [ ] CLI commands

### Phase 4: Advanced Features (Week 4)
- [ ] SPARQL queries for semantic discovery
- [ ] Cross-model canonical name mapping
- [ ] Semantic change impact analysis
- [ ] Dashboard/visualization

## Consequences

### Positive
- **Explicit semantics**: Data meaning is documented, not assumed
- **Better AI reasoning**: LLMs have context for accurate interpretation
- **Cross-system alignment**: Canonical names enable data integration
- **Quality validation**: Semantic conflicts caught early
- **Knowledge preservation**: Intent survives personnel changes

### Negative
- **Additional metadata burden**: More fields to maintain
- **Inference complexity**: Auto-inference may produce incorrect results
- **Learning curve**: Teams must understand aboutness concepts

### Mitigations
- AI-assisted inference reduces manual burden
- Confidence scores flag uncertain assignments
- Gradual adoption - start with key entities only

## References

- [Aboutness (Wikipedia)](https://en.wikipedia.org/wiki/Aboutness)
- [Juan Sequeda on Semantic Mappings](https://www.linkedin.com/in/juansequeda/)
- [ADR-244: Ontology Support](./ADR-244-Ontology-Support.md)
- [ADR-245: Metrics Layer](./ADR-245-Metrics-Layer.md)
- [ADR-246: AI Agent Context](./ADR-246-AI-Agent-Context.md)
