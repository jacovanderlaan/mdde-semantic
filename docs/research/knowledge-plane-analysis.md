# Knowledge Plane Architecture Analysis & MDDE Inspiration

*Research Date: 2026-02-15*

## Executive Summary

The Knowledge Plane represents the evolution from data management to semantic knowledge management. This analysis, inspired by Juan Sequeda's "Separate Knowledge from Data" principle and Blindata's Knowledge Plane architecture, identifies features that could enhance MDDE's semantic capabilities.

---

## Core Concept: Knowledge vs Data Separation

### Juan Sequeda's Framework

| Plane | Definition | Examples |
|-------|------------|----------|
| **Knowledge Plane** | Metadata, semantics, context | Business glossaries, definitions, domain models, schemas, taxonomies, ontologies |
| **Data Plane** | The facts | Databases, data lakes, files - typically larger, distributed, performance-optimized |

**Key Principle**: "Keep data where it is" - don't force everything into one system. Use virtualization or materialization based on use cases.

### Connection Methods

| Method | Description | MDDE Relevance |
|--------|-------------|----------------|
| **Shared Identifiers** | Unique references (e.g., `qudt:DEG_C` for Degree Celsius) | Domain IDs, entity IDs |
| **Mappings** | Relationships enabling query translation | Entity mappings, attribute mappings |

---

## Knowledge Plane Architecture (Blindata)

### Three-Layer Semantic Progression

```
Level 1: GLOSSARY
├── Vocabulary definitions
├── What things are called
└── Consistency in terminology

Level 2: TAXONOMY
├── Hierarchical classification
├── Organizing terms into categories
└── Parent-child relationships

Level 3: ONTOLOGY
├── Relational structure
├── How concepts interact
└── Context and relationships (RDF/OWL/SHACL)
```

**MDDE Current State**:
- Level 1: ✅ Domains with descriptions
- Level 2: ✅ Subject areas, hierarchies
- Level 3: ⚠️ Partial (relationships, stereotypes, but no formal ontology)

### Semantic Linking vs Traditional Tagging

| Approach | Description | Example |
|----------|-------------|---------|
| **Tagging** | Ambiguous labeling | Tag `customer_address` as "Address" |
| **Semantic Linking** | Context-aware relationships | Define `hasShippingAddress`, `hasBillingAddress` |

**Implication for MDDE**: Move beyond simple attribute descriptions to formal semantic relationships.

---

## Knowledge Mesh: Federated Approach

### Federated Centralization Model

```
┌─────────────────────────────────────────────────────────┐
│                   CENTRAL TEAM                          │
│     Core Enterprise Concepts & Global Ontology          │
└────────────────────────┬────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ▼               ▼               ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  DOMAIN A   │  │  DOMAIN B   │  │  DOMAIN C   │
│ Sales Team  │  │ Finance Team│  │ Product Team│
│ Local Terms │  │ Local Terms │  │ Local Terms │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       ▼                ▼                ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│DATA PRODUCTS│  │DATA PRODUCTS│  │DATA PRODUCTS│
│ Entities    │  │ Entities    │  │ Entities    │
│ linked to   │  │ linked to   │  │ linked to   │
│ semantics   │  │ semantics   │  │ semantics   │
└─────────────┘  └─────────────┘  └─────────────┘
```

**Shift Left Responsibility**: Domain experts define semantic meaning during development, not post-hoc.

---

## Implementation Patterns (Juan Sequeda)

### Pattern 1: Tag-Based Linking
Connect column schemas to ontology concepts through identifiers.

**MDDE Implementation**:
```yaml
attribute:
  name: temperature_celsius
  semantic_concept: qudt:DEG_C
  ontology_uri: http://qudt.org/vocab/unit/DEG_C
```

### Pattern 2: Identifier Injection
Store governed identifiers alongside data values ("strings → things").

**MDDE Implementation**:
```yaml
attribute:
  name: country
  semantic_type: reference_data
  canonical_identifier: iso_3166_1_alpha_2
  domain: country_code
```

### Pattern 3: Knowledge Tables
Centrally govern ontologies driving physical schemas.

**MDDE Implementation**:
```yaml
ontology_concept:
  concept_id: ont:Customer
  label: Customer
  definition: A person or organization that purchases goods or services
  superclass: ont:Party
  properties:
    - has_contact_info
    - has_address
    - has_orders
```

---

## Features for MDDE

### Priority 1: Semantic Layer Enhancement (v3.39.0)

#### 1.1 Ontology Support (ADR-244) ✅ IMPLEMENTED

**What it does**: Formal representation of domain concepts and relationships.

**Status**: Implemented in v3.22.0 (2026-02-15)
- OWL-inspired concepts with inheritance
- Properties with domain/range constraints
- Entity-concept linking
- Basic inference/reasoning
- Location: `src/mdde/semantic/ontology/`

**Current MDDE Gap**: ~~Domains and stereotypes exist but no formal ontology.~~ RESOLVED

**Recommendation for MDDE**:
- Add `ontology_concept` table for formal concept definitions
- Add `ontology_property` for relationships between concepts
- Support RDF/OWL export for interoperability
- Link entities to ontology concepts

**New Tables**:
```sql
CREATE TABLE metadata.ontology_concept (
    concept_id VARCHAR PRIMARY KEY,
    ontology_id VARCHAR NOT NULL,
    concept_uri VARCHAR,              -- Full URI for linked data
    label VARCHAR NOT NULL,
    definition VARCHAR,
    superclass_id VARCHAR,            -- Parent concept
    equivalent_class VARCHAR,         -- OWL equivalentClass
    created_at TIMESTAMP,
    FOREIGN KEY (superclass_id) REFERENCES ontology_concept(concept_id)
);

CREATE TABLE metadata.ontology_property (
    property_id VARCHAR PRIMARY KEY,
    ontology_id VARCHAR NOT NULL,
    property_uri VARCHAR,
    label VARCHAR NOT NULL,
    definition VARCHAR,
    domain_concept_id VARCHAR,        -- Subject concept
    range_concept_id VARCHAR,         -- Object concept
    property_type VARCHAR,            -- object_property, data_property
    is_functional BOOLEAN,
    is_inverse_functional BOOLEAN,
    FOREIGN KEY (domain_concept_id) REFERENCES ontology_concept(concept_id),
    FOREIGN KEY (range_concept_id) REFERENCES ontology_concept(concept_id)
);

CREATE TABLE metadata.entity_concept_link (
    link_id VARCHAR PRIMARY KEY,
    entity_id VARCHAR NOT NULL,
    concept_id VARCHAR NOT NULL,
    link_type VARCHAR,                -- instance_of, represents, related_to
    confidence DECIMAL(3,2),
    FOREIGN KEY (entity_id) REFERENCES entity(entity_id),
    FOREIGN KEY (concept_id) REFERENCES ontology_concept(concept_id)
);
```

**YAML Definition**:
```yaml
ontology:
  id: enterprise_ontology
  namespace: https://example.org/ontology/
  concepts:
    - id: Customer
      label: Customer
      definition: A person or organization that purchases goods or services
      superclass: Party
      properties:
        - hasContactInfo
        - hasShippingAddress
        - hasBillingAddress
    - id: Order
      label: Order
      definition: A request to purchase products or services
      superclass: Transaction
      properties:
        - placedBy (range: Customer)
        - contains (range: OrderLine)
```

#### 1.2 Semantic Linking (ADR-199)

**What it does**: Context-aware relationships instead of simple tagging.

**Current MDDE Gap**: Attributes have types but no semantic context.

**Recommendation for MDDE**:
- Add semantic relationship types to attributes
- Support property paths for complex relationships
- Generate semantic queries

**Semantic Attribute Enhancement**:
```yaml
attribute:
  name: shipping_address_id
  data_type: VARCHAR
  semantic:
    concept: Address
    relationship: hasShippingAddress
    role: shipping

attribute:
  name: billing_address_id
  data_type: VARCHAR
  semantic:
    concept: Address
    relationship: hasBillingAddress
    role: billing
```

#### 1.3 Knowledge Graph Export (ADR-200)

**What it does**: Export MDDE models as knowledge graphs.

**Recommendation for MDDE**:
- RDF/Turtle export of entities and relationships
- OWL ontology export from stereotypes and domains
- SPARQL endpoint generation
- GraphQL schema generation

**Export Formats**:
| Format | Use Case | Output |
|--------|----------|--------|
| RDF/Turtle | Linked data | `.ttl` files |
| OWL | Ontology interchange | `.owl` files |
| JSON-LD | Web APIs | `.jsonld` files |
| GraphQL | Query APIs | `.graphql` schema |

### Priority 2: Semantic Layer for BI (v3.40.0)

#### 2.1 Metrics Layer (ADR-245) ✅ IMPLEMENTED

**What it does**: Define business metrics with semantic context.

**Status**: Implemented in v3.22.0 (2026-02-15)
- Metric definitions with aggregations
- Dimensions and filters
- Semantic query to SQL generation
- Metric goals and alerting
- Location: `src/mdde/semantic/metrics/`

**Distinction from Ontology**:
- **Ontology**: Formal specification of concepts (what things ARE)
- **Metrics Layer**: Query-time abstraction for measures (how to CALCULATE)

**New Tables**:
```sql
CREATE TABLE metadata.metric_def (
    metric_id VARCHAR PRIMARY KEY,
    metric_name VARCHAR NOT NULL,
    metric_type VARCHAR,              -- simple, derived, ratio, cumulative
    description VARCHAR,
    business_owner VARCHAR,
    calculation_expression VARCHAR,   -- SQL expression
    time_grain VARCHAR,               -- daily, weekly, monthly
    entity_id VARCHAR,                -- Primary entity
    FOREIGN KEY (entity_id) REFERENCES entity(entity_id)
);

CREATE TABLE metadata.metric_dimension (
    metric_dimension_id VARCHAR PRIMARY KEY,
    metric_id VARCHAR NOT NULL,
    attribute_id VARCHAR NOT NULL,
    dimension_role VARCHAR,           -- slice, filter, group_by
    FOREIGN KEY (metric_id) REFERENCES metric_def(metric_id),
    FOREIGN KEY (attribute_id) REFERENCES attribute(attribute_id)
);
```

**YAML Definition**:
```yaml
metrics:
  - name: total_revenue
    type: simple
    description: Sum of all order amounts
    entity: orders
    expression: SUM(order_amount)
    time_grain: daily
    dimensions:
      - customer_segment
      - product_category
      - region

  - name: average_order_value
    type: derived
    description: Average value per order
    expression: total_revenue / order_count
    depends_on:
      - total_revenue
      - order_count
```

#### 2.2 Semantic Query Generation

**What it does**: Generate SQL from semantic queries.

**Example**:
```
Semantic Query: "Show total_revenue by customer_segment for last_quarter"
                          ↓
Generated SQL:
SELECT
    d.customer_segment,
    SUM(f.order_amount) as total_revenue
FROM orders f
JOIN customer d ON f.customer_id = d.customer_id
WHERE f.order_date >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months')
  AND f.order_date < DATE_TRUNC('quarter', CURRENT_DATE)
GROUP BY d.customer_segment
```

### Priority 3: AI Agent Foundation (v3.41.0)

#### 3.1 Knowledge Context for AI (ADR-246) ✅ IMPLEMENTED

**What it does**: Provide semantic context for AI agents.

**Status**: Implemented in v3.22.0 (2026-02-15)
- KnowledgePlane aggregation of metadata
- Context request with type filtering
- Intent detection for queries
- Token-optimized output formats
- Location: `src/mdde/semantic/agent/`

**Recommendation for MDDE**: ~~IMPLEMENTED~~
- Export knowledge graph as context for LLMs
- Generate natural language descriptions of ontology
- Support question-answering over metadata

**Integration with GenAI**:
```python
# AI agent can query the knowledge plane
agent.ask("What entities contain customer information?")
# → Uses ontology to understand Customer concept
# → Returns: customer, customer_address, customer_contact, orders

agent.ask("How is revenue calculated?")
# → Uses metrics layer
# → Returns: SUM(order_amount) from orders entity
```

---

## Maturity Model

### Knowledge Plane Maturity Progression

| Level | Stage | Description | MDDE Features |
|-------|-------|-------------|---------------|
| 1 | **Crawl** | Business Glossary | Domains, descriptions |
| 2 | **Walk** | Taxonomy | Subject areas, hierarchies |
| 3 | **Run** | Core Ontology | Entity concepts, semantic links |
| 4 | **Fly** | Federated Ontologies | Domain-specific extensions, AI reasoning |

**Current MDDE Position**: Level 2 (Walk) - has taxonomies, needs formal ontology

---

## Architecture Integration

### Module Structure

```
src/mdde/semantic/
├── __init__.py
├── ontology/
│   ├── __init__.py
│   ├── ontology_manager.py      # Concept CRUD
│   ├── property_manager.py      # Property definitions
│   ├── inference.py             # Basic reasoning
│   └── validators.py            # Ontology consistency
├── metrics/
│   ├── __init__.py
│   ├── metric_manager.py        # Metric definitions
│   ├── calculation_engine.py    # Expression evaluation
│   └── sql_generator.py         # Metric SQL generation
├── export/
│   ├── __init__.py
│   ├── rdf_exporter.py          # RDF/Turtle export
│   ├── owl_exporter.py          # OWL export
│   ├── jsonld_exporter.py       # JSON-LD export
│   └── graphql_generator.py     # GraphQL schema
└── linking/
    ├── __init__.py
    ├── entity_linker.py         # Link entities to concepts
    └── attribute_linker.py      # Semantic attribute linking
```

---

## Comparison: Semantic vs Analytics

| Aspect | Ontology (Knowledge) | Semantic Layer (BI) |
|--------|---------------------|---------------------|
| **Purpose** | Define what things ARE | Define how to CALCULATE |
| **Format** | RDF/OWL/SHACL | SQL/dbt metrics/LookML |
| **Consumers** | AI agents, data catalog | BI tools, analysts |
| **Governance** | Domain teams | Analytics team |
| **MDDE Support** | Planned (v3.39.0) | Planned (v3.40.0) |

---

## MDDE Competitive Positioning

| Feature | MDDE | data.world | Atlan | Alation |
|---------|------|------------|-------|---------|
| Knowledge Graph | ✅ v3.22.0 | ✅ Native | ✅ | ✅ |
| Ontology Support | ✅ v3.22.0 | ✅ | ⚠️ Basic | ⚠️ Basic |
| Metrics Layer | ✅ v3.22.0 | ⚠️ | ✅ | ✅ |
| Metadata-as-Code | ✅ | ❌ | ❌ | ❌ |
| SQL Generation | ✅ | ❌ | ❌ | ❌ |
| AI Agent Context | ✅ v3.22.0 | ✅ | ✅ | ✅ |

---

## Implementation Status

### ✅ Completed Features (v3.22.0)

| Feature | ADR | Description | Status |
|---------|-----|-------------|--------|
| Ontology Support | ADR-244 | OWL-inspired concepts, properties, inference | ✅ Done |
| Metrics Layer | ADR-245 | Business metrics, SQL generation | ✅ Done |
| AI Agent Context | ADR-246 | KnowledgePlane for LLM reasoning | ✅ Done |

### 🔨 Future Work

| Feature | Description | Target |
|---------|-------------|--------|
| Knowledge Graph Export | RDF, OWL, JSON-LD exporters | v3.23.0 |
| GraphQL Schema Generation | Schema from ontology | v3.23.0 |
| SPARQL Endpoint | Query interface | v3.24.0 |

---

## Sources

- [Juan Sequeda - Lesson 17: Separate Knowledge from Data (LinkedIn)](https://www.linkedin.com/posts/juansequeda_lesson-17-separate-knowledge-from-data-share-7427730223548452865-_xkb)
- [Blindata - Architecting the Knowledge Plane for Humans and Agents](https://blindata.io/blog/2025/architecting-knowledge-plane/index.html)
- [Coalesce - Semantic Layers in 2025: A Catalog Owner and Data Leader Playbook](https://coalesce.io/data-insights/semantic-layers-2025-catalog-owner-data-leader-playbook/)
- [data.world - What does it mean for a data catalog to be powered by a knowledge graph?](https://data.world/blog/data-catalog-knowledge-graph/)
- [Timbr.ai - Beyond the Semantic Layer: How Ontologies Transform Data Strategy](https://medium.com/timbr-ai/beyond-the-semantic-layer-how-ontologies-transform-data-strategy-d5a80050e048)
- [Juan Sequeda - Designing and Building Enterprise Knowledge Graphs](https://www.amazon.com/Designing-Enterprise-Knowledge-Synthesis-Semantics/dp/1636391761)

---

*Analysis by: Claude (Opus 4.5)*
*For: MDDE Framework Enhancement Planning*
