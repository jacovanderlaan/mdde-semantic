# ADR-244: Ontology Support

## Status
Accepted

## Date
2026-02-15

## Context

Knowledge Plane architecture progresses from Glossary → Taxonomy → Ontology. MDDE already has business glossary and domain support. Formal ontology adds OWL-inspired concepts, properties, and reasoning capabilities for semantic interoperability.

### Requirements
1. Define formal concepts with OWL semantics
2. Support concept hierarchies (subclass relationships)
3. Define properties with domains and ranges
4. Link entities and attributes to concepts
5. Basic inference/reasoning support
6. RDF/OWL-compatible export (future)

## Decision

### 1. Ontology Module

Location: `src/mdde/semantic/ontology/`

**Components**:
- `models.py` - Data models for ontology elements
- `manager.py` - OntologyManager for CRUD and inference
- `__init__.py` - Module exports

### 2. Core Concepts

| Class | Description |
|-------|-------------|
| `Ontology` | Container for concepts and properties |
| `OntologyConcept` | Formal concept (class) with definition |
| `OntologyProperty` | Property with domain/range |
| `OntologyRestriction` | Property restrictions (cardinality, etc.) |
| `EntityConceptLink` | Links MDDE entities to concepts |
| `AttributeSemanticLink` | Links attributes to properties |

### 3. Concept Types

```python
class ConceptType(Enum):
    CLASS = "class"           # Standard class
    INDIVIDUAL = "individual" # Instance
    DATATYPE = "datatype"     # Data type
    ANNOTATION = "annotation" # Annotation concept
```

### 4. Property Types

```python
class PropertyType(Enum):
    OBJECT = "object"         # Links concepts
    DATA = "data"             # Links to literals
    ANNOTATION = "annotation" # Metadata
```

### 5. Property Characteristics

```python
class PropertyCharacteristic(Enum):
    FUNCTIONAL = "functional"           # At most one value
    INVERSE_FUNCTIONAL = "inverse_functional"
    SYMMETRIC = "symmetric"             # a→b implies b→a
    ASYMMETRIC = "asymmetric"
    TRANSITIVE = "transitive"           # a→b→c implies a→c
    REFLEXIVE = "reflexive"
    IRREFLEXIVE = "irreflexive"
```

### 6. Usage Examples

**Create Ontology**:
```python
from mdde.semantic import OntologyManager, OntologyConcept, ConceptType

manager = OntologyManager(conn)

# Create ontology
ontology_id = manager.create_ontology(
    Ontology(
        ontology_id="ont_finance",
        name="Finance Ontology",
        namespace="https://example.com/finance#",
        description="Financial domain concepts",
    )
)
```

**Define Concepts**:
```python
# Define concepts
customer_id = manager.create_concept(OntologyConcept(
    concept_id="concept_customer",
    ontology_id="ont_finance",
    concept_name="Customer",
    concept_type=ConceptType.CLASS,
    definition="A party that purchases goods or services",
))

# Create hierarchy
manager.set_concept_parent("concept_corporate", "concept_customer")
```

**Link to Entities**:
```python
from mdde.semantic import EntityConceptLink, LinkType

manager.link_entity_to_concept(EntityConceptLink(
    link_id="link_001",
    entity_id="customer_hub",
    concept_id="concept_customer",
    link_type=LinkType.INSTANCE_OF,
))
```

**Inference**:
```python
# Get all subtypes of Customer
subtypes = manager.get_concept_subtypes("concept_customer")

# Find common superclass
common = manager.find_common_superclass(
    ["concept_corporate", "concept_individual"]
)
```

### 7. Standard Namespaces

```python
STANDARD_NAMESPACES = {
    "owl": "http://www.w3.org/2002/07/owl#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
}
```

### 8. Database Tables

| Table | Purpose |
|-------|---------|
| `ontology` | Ontology containers |
| `ontology_concept` | Concepts/classes |
| `ontology_property` | Properties |
| `ontology_concept_hierarchy` | Parent-child relationships |
| `ontology_entity_link` | Entity-concept links |
| `ontology_attribute_link` | Attribute-property links |

## Consequences

### Positive
- Formal semantic modeling
- OWL-inspired standards
- Reasoning capabilities
- Semantic interoperability foundation
- AI agent context enrichment

### Negative
- Learning curve for OWL concepts
- Performance overhead for inference
- Requires ontology design expertise

### Risks
- Over-complex ontologies
- Inference performance at scale
- Semantic drift from source ontologies

## Implementation

Files created:
1. `src/mdde/semantic/ontology/models.py` - Data models
2. `src/mdde/semantic/ontology/manager.py` - OntologyManager
3. `src/mdde/semantic/ontology/__init__.py` - Module exports

## References

- [OWL Web Ontology Language](https://www.w3.org/OWL/)
- [RDF Primer](https://www.w3.org/TR/rdf11-primer/)
- [SKOS Simple Knowledge Organization System](https://www.w3.org/TR/skos-primer/)
- [Knowledge Plane Analysis](../research/knowledge-plane-analysis.md)
