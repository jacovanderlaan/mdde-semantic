# Aboutness Layer Module

**ADR**: [ADR-247](../adr/semantic/ADR-247-Aboutness-Layer.md)
**Version**: 3.35.0
**Location**: `src/mdde/semantic/aboutness/`

## Overview

The Aboutness Layer bridges the gap between **raw data** (the Data Plane) and **conceptual understanding** (the Knowledge Plane). It answers the fundamental question: "What is this data *about*?"

While existing MDDE features like glossary terms and metrics provide semantic context, the Aboutness Layer adds explicit annotation of what each attribute **measures**, **identifies**, **classifies**, or **relates to** in the real world.

## Key Concepts

### Aboutness Dimensions

Every attribute in a data model has an inherent "aboutness" - what it represents semantically:

| Dimension | Description | Examples |
|-----------|-------------|----------|
| **measure** | Quantitative values that can be aggregated | revenue, quantity, weight, count |
| **identifier** | Keys that uniquely identify entities | customer_id, order_number, sku |
| **classifier** | Categorical values for grouping | status, type, category, region |
| **temporal** | Time-related values | order_date, created_at, fiscal_year |
| **relationship** | Foreign keys linking entities | customer_fk, parent_id |
| **quality** | Descriptive text attributes | name, description, email, address |
| **spatial** | Geographic or location data | latitude, longitude, city, country |
| **state** | Current condition or status | order_status, account_state |
| **flag** | Boolean indicators | is_active, has_shipped, is_deleted |
| **derived** | Computed from other attributes | profit_margin, age, tenure |

### Semantic Roles

Attributes can have functional roles in queries and analytics:

| Role | Description |
|------|-------------|
| **aggregatable** | Can be summed, averaged, counted |
| **filterable** | Commonly used in WHERE clauses |
| **groupable** | Used for GROUP BY operations |
| **sortable** | Natural ordering attribute |
| **joinable** | Used to join tables |
| **derivable** | Can be computed from other attributes |
| **displayable** | Shown in user interfaces |
| **sliceable** | Used for dimensional analysis |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Knowledge Plane                             │
│  ┌───────────┐  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ Ontology  │  │ Glossary │  │ Metrics  │  │   Aboutness   │  │
│  │ (classes) │  │ (terms)  │  │  (KPIs)  │  │ (intentions)  │  │
│  └───────────┘  └──────────┘  └──────────┘  └───────────────┘  │
└───────────────────────────────────┬─────────────────────────────┘
                                    │
                         Semantic Mapping
                                    │
┌───────────────────────────────────▼─────────────────────────────┐
│                        Data Plane                                │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                      Entities                               │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │ │
│  │  │ customer │  │  order   │  │ product  │  │ shipment │   │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Module Components

### 1. Data Models (`models.py`)

```python
from mdde.semantic.aboutness import (
    AboutnessDimension,
    SemanticRole,
    EntityAboutness,
    AttributeAboutness,
    SemanticDependency,
    AboutnessValidation,
)
```

**EntityAboutness** - Entity-level semantic intent:
- `purpose`: What is this entity about?
- `real_world_object`: What real-world thing does it represent?
- `business_use_cases`: How is it used in business processes?

**AttributeAboutness** - Attribute-level semantic annotation:
- `intent`: Human-readable description of what this measures/identifies
- `aboutness_dimension`: The semantic dimension (measure, identifier, etc.)
- `semantic_role`: Functional role (aggregatable, filterable, etc.)
- `measures_what`: For measures, what is being measured
- `identifies_what`: For identifiers, what is being identified
- `classifies_what`: For classifiers, what classification scheme
- `canonical_name`: Standardized attribute name
- `confidence_score`: Confidence in the annotation (0-1)

### 2. Manager (`manager.py`)

The `AboutnessManager` provides CRUD operations:

```python
from mdde.semantic.aboutness import AboutnessManager

manager = AboutnessManager(conn)

# Set entity-level aboutness
manager.set_entity_aboutness(EntityAboutness(
    entity_id="customer_order",
    purpose="Records customer purchase transactions",
    real_world_object="Sales Order",
    business_use_cases=["Revenue analysis", "Order fulfillment"],
))

# Set attribute-level aboutness
manager.set_attribute_aboutness(AttributeAboutness(
    entity_id="customer_order",
    attribute_id="order_total",
    intent="The total monetary value of the order",
    aboutness_dimension=AboutnessDimension.MEASURE,
    semantic_role=SemanticRole.AGGREGATABLE,
    measures_what="Order value in local currency",
    canonical_name="order_total_amount",
))

# Query by dimension
measures = manager.find_by_dimension("customer_order", AboutnessDimension.MEASURE)

# Query by semantic role
aggregatables = manager.find_by_role("customer_order", SemanticRole.AGGREGATABLE)

# Get coverage statistics
stats = manager.get_coverage_stats("customer_order")
# {'total': 15, 'with_aboutness': 12, 'coverage_percent': 80.0}
```

### 3. Inference Engine (`inference.py`)

Automatically infer aboutness from attribute names and types:

```python
from mdde.semantic.aboutness import AboutnessInferrer

inferrer = AboutnessInferrer(conn)

# Infer all attributes for an entity
inferred = inferrer.infer_entity("customer_order")

# Infer single attribute
aboutness = inferrer.infer_attribute(
    entity_id="customer_order",
    attribute_id="order_total",
    data_type="DECIMAL",
)
# Returns: AttributeAboutness with dimension=MEASURE, role=AGGREGATABLE
```

**Pattern Recognition:**
- `*_id`, `*_key`, `*_code` → IDENTIFIER
- `*_amount`, `*_total`, `*_price`, `*_qty` → MEASURE
- `*_date`, `*_time`, `*_at` → TEMPORAL
- `*_type`, `*_status`, `*_category` → CLASSIFIER
- `*_fk`, `*_parent`, `*_owner` → RELATIONSHIP
- `is_*`, `has_*`, `*_flag` → FLAG
- `*_lat`, `*_lng`, `*_city`, `*_country` → SPATIAL

### 4. Validator (`validator.py`)

Validate aboutness annotations for consistency:

```python
from mdde.semantic.aboutness import AboutnessValidator

validator = AboutnessValidator(conn)

# Run all validations
issues = validator.validate("customer_order")

for issue in issues:
    print(f"[{issue.check_code}] {issue.severity}: {issue.message}")
    print(f"  Recommendation: {issue.recommendation}")
```

**Validation Checks:**

| Code | Severity | Description |
|------|----------|-------------|
| A001 | warning | Entity missing purpose annotation |
| A002 | error | Attribute has conflicting dimension assignments |
| A003 | warning | Measure without aggregatable role |
| A004 | info | Attribute missing canonical name |
| A005 | error | Attribute references non-existent entity |
| A006 | warning | Low aboutness coverage (<50%) |
| A007 | warning | Inconsistent canonical names across entities |
| A008 | error | Conflicting semantic roles |
| A009 | warning | Data type doesn't match dimension |
| A010 | info | Low confidence score on inferred aboutness |

## Database Schema

The Aboutness Layer adds four tables:

### `entity_aboutness`
```sql
CREATE TABLE metadata.entity_aboutness (
    aboutness_id VARCHAR PRIMARY KEY,
    entity_id VARCHAR NOT NULL,
    model_id VARCHAR,
    purpose TEXT,
    real_world_object VARCHAR,
    business_use_cases TEXT,  -- JSON array
    source VARCHAR DEFAULT 'manual',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `attribute_aboutness`
```sql
CREATE TABLE metadata.attribute_aboutness (
    aboutness_id VARCHAR PRIMARY KEY,
    entity_id VARCHAR NOT NULL,
    attribute_id VARCHAR NOT NULL,
    model_id VARCHAR,
    intent TEXT,
    aboutness_dimension VARCHAR,  -- measure, identifier, classifier, etc.
    semantic_role VARCHAR,        -- aggregatable, filterable, etc.
    measures_what VARCHAR,
    identifies_what VARCHAR,
    classifies_what VARCHAR,
    relates_to VARCHAR,
    represents_property VARCHAR,
    canonical_name VARCHAR,
    expected_behavior TEXT,       -- JSON
    derived_from TEXT,            -- JSON array
    semantic_transform VARCHAR,
    confidence_score DECIMAL(3,2),
    source VARCHAR DEFAULT 'manual',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `semantic_dependency`
```sql
CREATE TABLE metadata.semantic_dependency (
    dependency_id VARCHAR PRIMARY KEY,
    source_entity_id VARCHAR NOT NULL,
    source_attribute_id VARCHAR NOT NULL,
    target_entity_id VARCHAR NOT NULL,
    target_attribute_id VARCHAR,
    dependency_type VARCHAR,  -- measures, identifies, derives_from, etc.
    model_id VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `aboutness_validation`
```sql
CREATE TABLE metadata.aboutness_validation (
    validation_id VARCHAR PRIMARY KEY,
    entity_id VARCHAR,
    attribute_id VARCHAR,
    model_id VARCHAR,
    check_code VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    message TEXT NOT NULL,
    recommendation TEXT,
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## YAML Schema

Define aboutness in entity YAML files:

```yaml
entity:
  entity_id: customer_order
  name: Customer Order

  aboutness:
    purpose: Records customer purchase transactions
    real_world_object: Sales Order
    business_use_cases:
      - Revenue analysis
      - Order fulfillment tracking
      - Customer behavior analysis

  attributes:
    - attribute_id: order_id
      name: Order ID
      data_type: VARCHAR(50)
      is_primary_key: true
      aboutness:
        dimension: identifier
        identifies_what: Customer purchase transaction
        canonical_name: order_identifier

    - attribute_id: order_total
      name: Order Total
      data_type: DECIMAL(15,2)
      aboutness:
        dimension: measure
        role: aggregatable
        intent: Total monetary value of the order
        measures_what: Order value in local currency
        expected_behavior:
          min_value: 0
          typical_range: [10, 10000]

    - attribute_id: order_date
      name: Order Date
      data_type: DATE
      aboutness:
        dimension: temporal
        role: filterable
        intent: When the order was placed
```

## Integration with Other MDDE Features

### Glossary Terms
```python
# Link aboutness to glossary terms
manager.set_attribute_aboutness(AttributeAboutness(
    entity_id="customer_order",
    attribute_id="revenue",
    glossary_term_id="GLO_revenue",  # Links to existing glossary
))
```

### Metrics
```python
# Measures can be linked to metric definitions
manager.set_attribute_aboutness(AttributeAboutness(
    entity_id="fact_sales",
    attribute_id="total_revenue",
    metric_id="MET_total_revenue",  # Links to metric definition
    aboutness_dimension=AboutnessDimension.MEASURE,
))
```

### Data Quality
```python
# Expected behavior defines quality rules
manager.set_attribute_aboutness(AttributeAboutness(
    entity_id="product",
    attribute_id="unit_price",
    aboutness_dimension=AboutnessDimension.MEASURE,
    expected_behavior={
        "min_value": 0,
        "max_value": 100000,
        "never_null": True,
    },
))
```

## CLI Commands

```bash
# Infer aboutness for an entity
mdde aboutness infer customer_order --model sales_model

# Validate aboutness annotations
mdde aboutness validate customer_order

# Export aboutness to YAML
mdde aboutness export customer_order --output aboutness.yaml

# Import aboutness from YAML
mdde aboutness import aboutness.yaml

# Show coverage statistics
mdde aboutness coverage --model sales_model
```

## Best Practices

1. **Start with inference**: Use the inference engine to bootstrap annotations, then refine manually.

2. **Annotate at entity level first**: Define the entity's purpose before drilling into attributes.

3. **Use canonical names**: Establish consistent naming across your data warehouse.

4. **Validate regularly**: Run validation checks as part of your CI/CD pipeline.

5. **Document measures thoroughly**: Measures are most valuable - include what's being measured and any caveats.

6. **Link to glossary**: Connect aboutness annotations to your business glossary for consistent terminology.

## Example Use Cases

### 1. Self-Service Analytics
Users can discover relevant attributes by searching for what they measure or identify:
```python
# "I need revenue data"
measures = manager.find_by_concept(model_id="sales", concept="revenue")
```

### 2. Automated Documentation
Generate data dictionaries with semantic context:
```python
aboutness = manager.get_attribute_aboutness("customer_order", "order_total")
print(f"{aboutness.attribute_id}: {aboutness.intent}")
# "order_total: The total monetary value of the order"
```

### 3. Query Optimization
Identify aggregatable measures for summary tables:
```python
aggregatables = manager.find_by_role("fact_sales", SemanticRole.AGGREGATABLE)
```

### 4. Data Lineage Enhancement
Understand semantic dependencies:
```python
deps = manager.get_dependencies("derived_metric")
# Shows which source attributes feed into derived measures
```

## See Also

- [ADR-247: Aboutness Layer](../adr/semantic/ADR-247-Aboutness-Layer.md)
- [Glossary Module](glossary.md)
- [Metrics Module](metrics.md)
- [Ontology Module](ontology.md)
