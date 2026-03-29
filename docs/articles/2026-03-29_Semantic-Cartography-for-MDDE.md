# Semantic Cartography for MDDE

**Date:** 2026-03-29
**Status:** Implementation Complete
**Inspired by:** Dr. Nicolas Figay's "Semantic Cartography: A New Way to Maintain Enterprise Continuity"

## The Problem

From the article:
> "Every effort to improve enterprise performance ends up building another representation of reality. It works well locally, but as soon as another team, another system, or another lifecycle phase interacts with it, the alignment drifts."

MDDE already has:
- Impact Analyzer (structural impact)
- Lineage Graph (data flow)
- Semantic Layer (metrics, dimensions)
- Governance (ownership, policies)
- Temporal Detector (lifecycle states)

**What was missing:** A unified view that connects ALL these together.

## The Solution: Semantic Cartography Module

New module at `src/mdde/cartography/` that provides:

### 1. Unified Semantic Map (`CartographyMap`)

A single addressable space containing:

| Node Type | Description |
|-----------|-------------|
| `ENTITY` | Tables, views, datasets |
| `ATTRIBUTE` | Columns, fields |
| `TERM` | Business glossary terms |
| `METRIC` | Business metrics |
| `OWNER` | Data owners/stewards |
| `CONSUMER` | Applications, services |
| `RULE` | Business rules, policies |

### 2. Continuity Edges (`ContinuityEdge`)

Relationships that capture HOW changes propagate:

| Edge Type | Propagation Behavior |
|-----------|---------------------|
| `DERIVES_FROM` | SHOULD_REVIEW |
| `CONTAINS` | MUST_ADAPT |
| `MEANS` | SHOULD_REVIEW |
| `MEASURES` | BLOCKS (can't delete) |
| `OWNS` | NOTIFIES |
| `CONSUMES` | NOTIFIES |

### 3. Navigator (`CartographyNavigator`)

Answers the article's key questions:

```python
nav = CartographyNavigator(carto_map)

# "What is the universe of 'Customer' across the enterprise?"
universe = carto_map.get_universe("customer", max_hops=3)

# "Who is impacted by changes to this entity?"
impacted = nav.who_is_impacted("entity:customer")

# "What becomes inconsistent if this changes?"
inconsistencies = nav.what_becomes_inconsistent("entity:customer")

# "How healthy is our cross-domain continuity?"
report = nav.continuity_report()
```

### 4. Change Propagator (`ChangePropagator`)

Simulates change impact BEFORE making changes:

```python
prop = ChangePropagator(carto_map)

# "What if we delete this?"
impact = prop.what_if_delete("entity:customer")

# "Can we even make this change?"
can_change, blockers = prop.can_change("entity:customer", "DELETE")

# Human-readable summary
print(prop.propagation_summary(impact))
```

Output:
```
Change: DELETE on customer
Source: model.customer

BLOCKED BY:
  - Total Customers (MEASURES)

MUST ADAPT (3):
  - order [entity]
  - order_line [entity]
  - s_customer_details [entity]

SHOULD REVIEW (2):
  - Customer [term]
  - CRM Application [application]

Domains affected: crm, sales, finance
Notify: Alice, Bob

Total impacted: 5
Critical impacts: 1
```

### 5. Builder (`CartographyBuilder`)

Constructs the map from MDDE components:

```python
builder = CartographyBuilder(name="Enterprise Cartography")

# Add structural metadata
builder.add_model(my_model)

# Add lineage
builder.add_lineage_edge("source", "target")

# Add semantics
builder.add_glossary_term("Customer", related_entities=["customer"])
builder.add_metric("Revenue", measures_entities=["order"])

# Add governance
builder.add_owner("Alice", owns_entities=["customer"])
builder.add_consumer("CRM App", consumes_entities=["customer"])

# Build unified map
carto_map = builder.build()
```

## Key Design Decisions

### 1. Propagation Behaviors

Different edge types have different propagation semantics:

| Behavior | When to Use |
|----------|-------------|
| `MUST_ADAPT` | Target will break if source changes |
| `SHOULD_REVIEW` | Target may need updates |
| `MAY_IMPACT` | Potential impact, worth checking |
| `BLOCKS` | Change is not allowed (e.g., metrics) |
| `NOTIFIES` | Inform stakeholders, no action required |
| `INDEPENDENT` | No propagation |

### 2. Bidirectional Relationships

Some relationships (like MEANS/synonyms) are bidirectional. The builder automatically creates reverse edges.

### 3. Propagation Rules

Default rules cover common patterns:
- Deleting a source blocks metrics that measure it
- Deprecation notifies all consumers
- Type changes require review of downstream entities
- Critical data always requires review

Custom rules can be added:
```python
carto_map.add_rule(PropagationRule(
    rule_id="no_delete_critical",
    name="Cannot delete critical entities",
    target_node_type=NodeType.ENTITY,
    change_type="DELETE",
    propagation=PropagationBehavior.BLOCKS,
    priority=100,
))
```

## Integration with Existing MDDE

| MDDE Component | Cartography Integration |
|----------------|------------------------|
| Impact Analyzer | Provides structural impact, cartography adds semantic impact |
| Lineage Graph | Builder imports lineage as DERIVES_FROM edges |
| Semantic Layer | Metrics/dimensions become METRIC/DIMENSION nodes |
| Governance | Owners/stewards become OWNER nodes with OWNS edges |
| Glossary | Terms become TERM nodes with MEANS edges |

## Files Created

| File | Purpose |
|------|---------|
| `cartography/__init__.py` | Module exports |
| `cartography/models.py` | Core data structures |
| `cartography/navigator.py` | Query and navigation |
| `cartography/propagator.py` | Change simulation |
| `cartography/builder.py` | Construction helpers |
| `tests/cartography/test_cartography.py` | 21 tests |

## Article Alignment

| Article Quote | Implementation |
|---------------|----------------|
| "Nothing maintains continuity between representations" | CartographyMap IS the continuity |
| "Real interoperability is preserving alignment while contexts evolve" | Propagation rules track evolution |
| "Shared semantic space" | Unified query across all domains |
| "When something changes anywhere, show what is impacted everywhere" | ChangePropagator.simulate_change() |
| "What should adapt, and what must remain stable?" | MUST_ADAPT vs BLOCKS behaviors |

## Usage Example

```python
from mdde.cartography import CartographyBuilder, CartographyNavigator, ChangePropagator

# Build the map
builder = CartographyBuilder(name="My Enterprise")
builder.add_model(sales_model)
builder.add_model(finance_model)
builder.add_lineage_edge("sales.customer", "finance.customer_dim")
builder.add_glossary_term("Customer", related_entities=["sales.customer", "finance.customer_dim"])
builder.add_owner("data_team", owns_entities=["sales.customer"])
builder.add_metric("Customer LTV", measures_entities=["sales.customer"])

carto_map = builder.build()

# Navigate
nav = CartographyNavigator(carto_map)
report = nav.continuity_report()
print(f"Coverage: {report.coverage_percentage:.0f}%")
print(f"Cross-domain connections: {report.cross_domain_edges}")

# What-if analysis
prop = ChangePropagator(carto_map)
impact = prop.what_if_delete("entity:sales.customer")
print(prop.propagation_summary(impact))
```

## Future Enhancements

1. **Visual Cartography Viewer** - D3.js visualization of the map
2. **MDDE Integration** - Automatic builder from parsed models
3. **Change Staging** - Stage changes and validate before applying
4. **Temporal Cartography** - Track how the map evolves over time
5. **AI-Assisted Analysis** - Use LLM to explain impact in business terms
