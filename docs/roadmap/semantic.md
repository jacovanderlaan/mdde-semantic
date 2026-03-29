# Semantic Layer Module Roadmap

**Module**: `semantic/` (new)
**Purpose**: Ontology support, knowledge graphs, metrics layer, semantic linking

*Inspired by [Juan Sequeda's Knowledge-Data Separation](https://www.linkedin.com/posts/juansequeda_lesson-17-separate-knowledge-from-data-share-7427730223548452865-_xkb) and [Blindata's Knowledge Plane Architecture](https://blindata.io/blog/2025/architecting-knowledge-plane/index.html)*

---

## Vision

Elevate MDDE from a data modeling tool to a knowledge management platform by implementing formal ontology support, semantic linking, and a metrics layer. This enables AI agents and humans to understand not just the structure of data, but its meaning and business context.

**Core Principle**: "Separate Knowledge from Data" - The knowledge plane (semantics, context, definitions) is distinct from but linked to the data plane (facts in databases).

---

## Implemented

### Semantic API Layer (v3.88-3.90) - ✅

**ADRs**: ADR-413, ADR-414, ADR-415

The semantic layer has evolved from documentation to a **Semantic API** — machine-enforceable rules that AI agents must follow.

| Version | Feature | ADR | Description |
|---------|---------|-----|-------------|
| v3.88.0 | Semantic Contract Pattern | ADR-413 | Interpretation rules, state models, agent permissions |
| v3.89.0 | Decision Traces | ADR-414 | Who approved what, when, why (provenance) |
| v3.90.0 | Compliance Integration | ADR-415 | EU AI Act, SHACL constraints, regulatory export |

**Key Concepts:**

1. **Observations vs Interpretations** — dbt produces observations (facts), semantic API produces interpretations (rules)
2. **Description vs Contract** — Documentation increases probability of correct answer; contracts make correctness mandatory
3. **Context Backbone** — Stem (ontology/stereotypes) + Spine (taxonomy/glossary) + Connective Tissue (decision traces)

**Interpretation Rules (v3.88.0):**
```yaml
metric:
  name: active_customers
  rules:
    computation:
      must_use: payment_date
      must_exclude: [status = 'refunded']
    comparison:
      allowed: [month_over_month]
      forbidden: [day_over_day]
    thresholds:
      normal: "> 10000"
      warning: "5000 - 10000"
      critical: "< 5000"
```

**Decision Traces (v3.89.0):**
```yaml
decision:
  id: dec-2026-01-15-churn-threshold
  type: threshold_approval
  status: approved
  participants:
    - role: approver
      agent: data-governance-council
  rationale: "Aligned with industry benchmarks..."
```

**Compliance Integration (v3.90.0):**
```yaml
entity:
  compliance:
    eu_ai_act:
      system_type: high_risk
      article_6_reference: "Annex III, 5(a)"
```

**References:**
- Gromov, S. (2026). "The Semantic Layer Is Dead. Now It's an API for AI Agents."
- Honton, J. (2026). "From Tokens to Knowledge."
- Kumar, P. (2026). "EU AI Act Meets $140 Billion in Unclaimed Benefits."

---

### Knowledge Graph Export (v3.22.0) - ✅

**ADR-200: RDF/OWL/JSON-LD Export**

Full knowledge graph export capability supporting multiple semantic web formats:

| Format | File | Description |
|--------|------|-------------|
| RDF/Turtle | `rdf_exporter.py` | Standard RDF serialization with namespace prefixes |
| N-Triples | `rdf_exporter.py` | Line-based RDF for bulk loading |
| OWL | `owl_exporter.py` | OWL ontology with classes and properties |
| JSON-LD | `jsonld_exporter.py` | JSON-based linked data for web APIs |

**Usage**:
```python
from mdde.semantic.export import (
    RDFExporter,
    OWLExporter,
    JSONLDExporter,
    export_knowledge_graph,
)

# Quick export
turtle = export_knowledge_graph(conn, "MDL_sales", format="turtle")

# Full control
exporter = KnowledgeGraphExporter(conn, base_uri="https://mycompany.com/data/")
result = exporter.export("MDL_sales", format=ExportFormat.OWL)
```

---

## Planned

### Priority 1: Ontology Foundation (v3.39.0) - Q2 2026

#### Ontology Support (ADR-198)
- [ ] **Formal concept definitions** - RDF/OWL-compatible ontology
  - [ ] Concept hierarchy (class → subclass)
  - [ ] Property definitions (object and data properties)
  - [ ] Concept equivalence and disjointness
  - [ ] URI-based concept identification
  - [ ] Namespace management

**New Tables**:
```sql
CREATE TABLE metadata.ontology_def (
    ontology_id VARCHAR PRIMARY KEY,
    ontology_name VARCHAR NOT NULL,
    namespace_uri VARCHAR,
    version VARCHAR,
    description VARCHAR,
    created_at TIMESTAMP
);

CREATE TABLE metadata.ontology_concept (
    concept_id VARCHAR PRIMARY KEY,
    ontology_id VARCHAR NOT NULL,
    concept_uri VARCHAR,
    label VARCHAR NOT NULL,
    definition VARCHAR,
    superclass_id VARCHAR,
    equivalent_class_uri VARCHAR,
    FOREIGN KEY (ontology_id) REFERENCES ontology_def(ontology_id),
    FOREIGN KEY (superclass_id) REFERENCES ontology_concept(concept_id)
);

CREATE TABLE metadata.ontology_property (
    property_id VARCHAR PRIMARY KEY,
    ontology_id VARCHAR NOT NULL,
    property_uri VARCHAR,
    label VARCHAR NOT NULL,
    definition VARCHAR,
    domain_concept_id VARCHAR,
    range_concept_id VARCHAR,
    property_type VARCHAR,  -- object_property, data_property, annotation
    is_functional BOOLEAN DEFAULT FALSE,
    is_inverse_functional BOOLEAN DEFAULT FALSE,
    inverse_property_id VARCHAR,
    FOREIGN KEY (domain_concept_id) REFERENCES ontology_concept(concept_id),
    FOREIGN KEY (range_concept_id) REFERENCES ontology_concept(concept_id)
);
```

**YAML Definition**:
```yaml
ontology:
  id: enterprise_ontology
  name: Enterprise Core Ontology
  namespace: https://example.org/ontology/
  version: "1.0"

  concepts:
    - id: Party
      label: Party
      definition: A person or organization

    - id: Customer
      label: Customer
      definition: A party that purchases goods or services
      superclass: Party

    - id: Address
      label: Address
      definition: A physical or mailing location

  properties:
    - id: hasShippingAddress
      label: has shipping address
      domain: Customer
      range: Address
      type: object_property

    - id: hasBillingAddress
      label: has billing address
      domain: Customer
      range: Address
      type: object_property
```

#### Entity-Concept Linking (ADR-199)
- [ ] **Link entities to ontology concepts** - Semantic annotation
  - [ ] Link entities to concepts they represent
  - [ ] Link attributes to properties
  - [ ] Confidence scoring for inferred links
  - [ ] Multiple concepts per entity (multi-classification)

**New Tables**:
```sql
CREATE TABLE metadata.entity_concept_link (
    link_id VARCHAR PRIMARY KEY,
    entity_id VARCHAR NOT NULL,
    concept_id VARCHAR NOT NULL,
    link_type VARCHAR,  -- instance_of, represents, related_to
    confidence DECIMAL(3,2),
    source VARCHAR,  -- manual, inferred, imported
    FOREIGN KEY (entity_id) REFERENCES entity(entity_id),
    FOREIGN KEY (concept_id) REFERENCES ontology_concept(concept_id)
);

CREATE TABLE metadata.attribute_property_link (
    link_id VARCHAR PRIMARY KEY,
    attribute_id VARCHAR NOT NULL,
    property_id VARCHAR NOT NULL,
    role VARCHAR,  -- subject, object
    FOREIGN KEY (attribute_id) REFERENCES attribute(attribute_id),
    FOREIGN KEY (property_id) REFERENCES ontology_property(property_id)
);
```

**YAML Definition**:
```yaml
entity:
  name: customer
  semantic:
    concept: Customer
    link_type: represents
  attributes:
    - name: shipping_address_id
      semantic:
        property: hasShippingAddress
        role: subject
    - name: billing_address_id
      semantic:
        property: hasBillingAddress
        role: subject
```

---

### Priority 2: Knowledge Graph Export (v3.22.0) - ✅ IMPLEMENTED

#### RDF/OWL Export (ADR-200)
- [x] **Export as linked data** - Standard semantic formats
  - [x] RDF/Turtle export of entities and relationships
  - [x] OWL ontology export from concepts and properties
  - [x] JSON-LD export for web APIs
  - [x] N-Triples for bulk loading
  - [x] Namespace prefix management

**Implementation**: `src/mdde/semantic/export/`
- `rdf_exporter.py` - RDF/Turtle and N-Triples export
- `owl_exporter.py` - OWL ontology export
- `jsonld_exporter.py` - JSON-LD export
- `knowledge_graph.py` - Unified export interface

**Export CLI**:
```bash
# Export ontology only
python -m mdde.semantic export-ontology \
  --ontology enterprise_ontology \
  --format owl \
  --output ontology.owl

# Export knowledge graph (ontology + instances)
python -m mdde.semantic export-kg \
  --model sales_model \
  --format turtle \
  --output knowledge_graph.ttl

# Export as JSON-LD
python -m mdde.semantic export-kg \
  --model sales_model \
  --format jsonld \
  --output knowledge_graph.jsonld
```

**Output Example (Turtle)**:
```turtle
@prefix : <https://example.org/data/> .
@prefix ont: <https://example.org/ontology/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ont:Customer a owl:Class ;
    rdfs:subClassOf ont:Party ;
    rdfs:label "Customer" ;
    rdfs:comment "A party that purchases goods or services" .

ont:hasShippingAddress a owl:ObjectProperty ;
    rdfs:domain ont:Customer ;
    rdfs:range ont:Address .

:customer a ont:Customer ;
    ont:hasShippingAddress :address_123 ;
    ont:hasBillingAddress :address_456 .
```

#### GraphQL Schema Generation
- [ ] **Generate GraphQL from ontology** - API schema
  - [ ] Types from concepts
  - [ ] Relationships from properties
  - [ ] Query generation
  - [ ] Federation support

**Generated GraphQL**:
```graphql
type Customer {
  id: ID!
  name: String!
  shippingAddress: Address
  billingAddress: Address
  orders: [Order!]!
}

type Address {
  id: ID!
  street: String
  city: String
  country: String
}

type Query {
  customer(id: ID!): Customer
  customers(segment: String): [Customer!]!
}
```

---

### Priority 3: Metrics Layer (v3.41.0) - Q3 2026

#### Business Metrics (ADR-201)
- [ ] **Define metrics with semantic context** - BI abstraction
  - [ ] Simple metrics (direct aggregations)
  - [ ] Derived metrics (calculations on other metrics)
  - [ ] Ratio metrics (metric A / metric B)
  - [ ] Cumulative metrics (running totals)
  - [ ] Time grain support (daily, weekly, monthly)
  - [ ] Dimension associations

**New Tables**:
```sql
CREATE TABLE metadata.metric_def (
    metric_id VARCHAR PRIMARY KEY,
    metric_name VARCHAR NOT NULL,
    metric_type VARCHAR,  -- simple, derived, ratio, cumulative
    description VARCHAR,
    business_owner VARCHAR,
    calculation_expression VARCHAR,
    time_grain VARCHAR,  -- daily, weekly, monthly, quarterly, yearly
    entity_id VARCHAR,
    FOREIGN KEY (entity_id) REFERENCES entity(entity_id)
);

CREATE TABLE metadata.metric_dimension (
    metric_dimension_id VARCHAR PRIMARY KEY,
    metric_id VARCHAR NOT NULL,
    attribute_id VARCHAR NOT NULL,
    dimension_role VARCHAR,  -- slice, filter, group_by
    FOREIGN KEY (metric_id) REFERENCES metric_def(metric_id),
    FOREIGN KEY (attribute_id) REFERENCES attribute(attribute_id)
);

CREATE TABLE metadata.metric_dependency (
    dependency_id VARCHAR PRIMARY KEY,
    metric_id VARCHAR NOT NULL,
    depends_on_metric_id VARCHAR NOT NULL,
    FOREIGN KEY (metric_id) REFERENCES metric_def(metric_id),
    FOREIGN KEY (depends_on_metric_id) REFERENCES metric_def(metric_id)
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

  - name: order_count
    type: simple
    description: Count of orders
    entity: orders
    expression: COUNT(*)
    time_grain: daily
    dimensions:
      - customer_segment

  - name: average_order_value
    type: derived
    description: Average value per order
    expression: total_revenue / order_count
    depends_on:
      - total_revenue
      - order_count
    time_grain: daily

  - name: revenue_growth_pct
    type: ratio
    description: Revenue growth percentage
    numerator: current_period_revenue - prior_period_revenue
    denominator: prior_period_revenue
    time_grain: monthly
```

#### Metric SQL Generation
- [ ] **Generate SQL from metric definitions** - Executable queries
  - [ ] Simple metric to aggregate SQL
  - [ ] Dimension joins
  - [ ] Time grain windowing
  - [ ] Multi-dialect support

**Generated SQL**:
```sql
-- Metric: total_revenue by customer_segment (daily)
SELECT
    DATE_TRUNC('day', o.order_date) AS date_day,
    c.customer_segment,
    SUM(o.order_amount) AS total_revenue
FROM orders o
JOIN customer c ON o.customer_id = c.customer_id
GROUP BY 1, 2
ORDER BY 1, 2
```

---

### Priority 4: AI Agent Context (v3.42.0) - Q4 2026

#### Knowledge Context for LLMs
- [ ] **Provide semantic context for AI** - Agent foundation
  - [ ] Export knowledge graph as LLM context
  - [ ] Natural language ontology descriptions
  - [ ] Question-answering over metadata
  - [ ] Integration with GenAI Copilot

**Use Cases**:
```
User: "What entities contain customer information?"
Agent: [Uses ontology to find Customer concept and linked entities]
       → customer, customer_address, customer_contact, orders

User: "How is revenue calculated?"
Agent: [Uses metrics layer]
       → SUM(order_amount) from orders entity, sliced by customer_segment

User: "What is the relationship between orders and products?"
Agent: [Uses ontology properties]
       → Order contains OrderLine, OrderLine references Product
```

#### SPARQL Endpoint
- [ ] **Query knowledge graph with SPARQL** - Standard query language
  - [ ] SPARQL endpoint generation
  - [ ] Query translation to SQL
  - [ ] Federated queries across models

---

## Architecture

### Semantic Layer Maturity Model

| Level | Stage | Description | MDDE Features |
|-------|-------|-------------|---------------|
| 1 | **Crawl** | Business Glossary | ✅ Domains, descriptions |
| 2 | **Walk** | Taxonomy | ✅ Subject areas, hierarchies |
| 3 | **Run** | Core Ontology | 🔨 Concepts, semantic links |
| 4 | **Fly** | Federated + AI | 🔨 Domain ontologies, AI reasoning |

### Module Structure

```
src/mdde/semantic/
├── __init__.py
├── ontology/
│   ├── __init__.py
│   ├── ontology_manager.py      # Ontology CRUD
│   ├── concept_manager.py       # Concept definitions
│   ├── property_manager.py      # Property definitions
│   ├── inference.py             # Basic reasoning (subclass, transitive)
│   └── validators.py            # Ontology consistency checks
├── metrics/
│   ├── __init__.py
│   ├── metric_manager.py        # Metric definitions
│   ├── calculation_engine.py    # Expression parsing
│   ├── dependency_resolver.py   # Metric dependencies
│   └── sql_generator.py         # Metric to SQL
├── export/
│   ├── __init__.py
│   ├── rdf_exporter.py          # RDF/Turtle export
│   ├── owl_exporter.py          # OWL ontology export
│   ├── jsonld_exporter.py       # JSON-LD export
│   ├── graphql_generator.py     # GraphQL schema
│   └── sparql_endpoint.py       # SPARQL server
├── linking/
│   ├── __init__.py
│   ├── entity_linker.py         # Entity → Concept
│   ├── attribute_linker.py      # Attribute → Property
│   └── auto_linker.py           # AI-assisted linking
└── context/
    ├── __init__.py
    └── llm_context.py           # Context for AI agents
```

---

## Integration with MDDE Features

| MDDE Feature | Semantic Integration |
|--------------|---------------------|
| **Domains** | Link to ontology concepts |
| **Stereotypes** | Map to ontology classes |
| **Relationships** | Export as ontology properties |
| **Lineage** | Semantic provenance tracking |
| **GenAI Copilot** | Knowledge context for AI |
| **Validation** | Ontology consistency checks |

---

## Comparison: Knowledge vs Analytics Semantics

| Aspect | Ontology (Knowledge) | Metrics Layer (Analytics) |
|--------|---------------------|--------------------------|
| **Purpose** | Define what things ARE | Define how to CALCULATE |
| **Format** | RDF/OWL/SHACL | SQL expressions |
| **Consumers** | AI agents, catalogs | BI tools, analysts |
| **Governance** | Domain teams | Analytics team |
| **MDDE Module** | `semantic/ontology/` | `semantic/metrics/` |

---

## Dependencies

- `rdflib` - RDF parsing and serialization
- `owlready2` - OWL ontology support (optional)
- `graphql-core` - GraphQL schema generation
- Existing MDDE modules: `analyzer`, `generator`, `genai`

---

## Related ADRs

**Implemented:**
- ADR-413: Semantic Contract Pattern (✅ v3.88.0)
- ADR-414: Decision Traces (✅ v3.89.0)
- ADR-415: Compliance Integration (✅ v3.90.0)

**Planned:**
- ADR-198: Ontology Support (Planned)
- ADR-199: Entity-Concept Linking (Planned)
- ADR-200: Knowledge Graph Export (Planned)
- ADR-201: Metrics Layer (Planned)
- ADR-202: AI Agent Context (Planned)

---

## Competitive Analysis

See research documents for detailed feature comparisons:
- [Knowledge Plane Analysis](../../research/knowledge-plane-analysis.md) - Comprehensive semantic architecture analysis

---

## Success Metrics

1. **Ontology Coverage**: % of entities linked to ontology concepts
2. **Metric Adoption**: Number of metrics defined and used
3. **Export Usage**: Knowledge graph exports generated
4. **AI Context Quality**: LLM accuracy with semantic context
5. **Query Translation**: SPARQL to SQL conversion accuracy

---

**Document Version**: 1.1
**Last Updated**: 2026-03-29
