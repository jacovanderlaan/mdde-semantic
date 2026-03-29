# Why Your Semantic Layer Needs Ontology (And How Model-Driven Design Provides It)

*Your semantic layer defines how to query data. But does it know what the data means?*

---

## The Semantic Layer Gap

Semantic layers have become the darling of modern data stacks. dbt metrics, Cube, Looker, and AtScale all promise the same thing: define your metrics once, query them anywhere.

But there's a problem. As Adrian Brudaru from dltHub recently put it:

> "A semantic layer may contain information that is not in the data model and should not be guessed."

His example: **three types of gross margin** in one e-commerce company. The semantic layer defines how to calculate each one. But it doesn't explain:
- Why do three versions exist?
- When should you use which?
- What business process created this distinction?

This is the **ontological gap**. Semantic layers solve *query semantics* but miss *business semantics*.

---

## What Is Ontology?

In knowledge engineering, ontology defines how a domain works:

| Component | Definition | Example |
|-----------|------------|---------|
| **Entities** | Business nouns | Customer, Order, Campaign |
| **Relationships** | Connecting verbs | "Customer *places* Order" |
| **Attributes** | Properties | order_date, total_amount |
| **Constraints** | Business rules | "An Order requires a Customer" |
| **Context** | When/why distinctions matter | "Gross Margin A is for GAAP reporting" |

A dimensional model captures *some* of this. A semantic layer captures *less*. Neither captures the full picture.

---

## The LLM Wake-Up Call

This matters more now because of AI. When you ask an LLM to work with your data, it needs ontological context to reason correctly.

From Brudaru's experiments: a 20-question prompt establishing ontology made LLM transformations reliable. Without it, the LLM "guessed semantics differently every time."

Consider his case study: A support team celebrated a 45% ticket reduction. An AI agent without ontology recommended scaling this "success." But the company's business model was white-glove enterprise support—customers weren't satisfied, they had *abandoned* the service. With ontology context, the agent would have identified **churn risk**, not efficiency gains.

The semantic layer told the AI *how* to calculate ticket deflection. It didn't tell the AI *what ticket deflection means* in this business context.

---

## What's Missing from Semantic Layers

| Semantic Layer Has | Semantic Layer Lacks |
|--------------------|---------------------|
| Metric definitions | Why metrics exist |
| Join paths | Relationship semantics |
| Dimension hierarchies | Business process context |
| Aggregation rules | Temporal validity rules |
| Access policies | Data lineage meaning |

The semantic layer answers: "How do I calculate revenue?"

Ontology answers: "What is revenue in this business? Which of our three revenue definitions applies here? Why do we track it this way?"

---

## The Model-Driven Alternative

What if the **data model itself** carried ontological meaning?

This is the premise of model-driven data engineering. Instead of bolting ontology onto a metric layer, embed it at the foundation:

```yaml
# Entity definition with ontological context
entity:
  name: gross_margin
  business_name: Gross Margin (GAAP)
  description: >
    Revenue minus COGS, calculated per GAAP standards.
    Used for external financial reporting.
    Differs from operational_margin (excludes returns)
    and contribution_margin (excludes allocated overhead).

  context:
    reporting_standard: GAAP
    audience: [finance, investors, auditors]
    not_for: [operational_decisions, pricing]

  relationships:
    - type: alternative_to
      target: operational_margin
      reason: "Operational margin includes return adjustments"
    - type: component_of
      target: net_income

  attributes:
    - name: amount
      type: decimal(18,2)
      calculation: revenue - cogs

  classification:
    pii: false
    financial: true
    regulatory: SOX
```

This isn't just documentation—it's **machine-readable ontology** that:
1. LLMs can consume for reasoning
2. Generators use for code generation
3. Validators enforce as constraints
4. Discovery surfaces for data consumers

---

## Ontology Concepts in Model Metadata

| Ontology Concept | Model Implementation |
|------------------|---------------------|
| **Entities** | Entity definitions with `business_name`, `description` |
| **Relationships** | Explicit `relationships:` with types (containment, causality, alternative_to) |
| **Constraints** | Stereotypes (hub, dimension, fact) with implied rules |
| **Temporal semantics** | `delivery_properties:`, `snapshot_period:`, `valid_from/to` |
| **Context** | `tags:`, `classification:`, `audience:`, `owner:` |
| **Lineage meaning** | `derived_from:` with transformation semantics |

---

## The Three Gross Margins Problem

Let's revisit Adrian's example. Three gross margin definitions in one company:

### Semantic Layer Approach

```yaml
# dbt metrics
metrics:
  - name: gross_margin_gaap
    calculation: revenue - cogs

  - name: gross_margin_operational
    calculation: revenue - cogs - returns

  - name: gross_margin_contribution
    calculation: revenue - cogs - allocated_overhead
```

This tells you *how* to calculate each. An LLM could execute any of them. But which one answers "what's our margin?"

### Ontology-Embedded Approach

```yaml
# Model with ontological context
entities:
  - name: gross_margin_gaap
    stereotype: metric
    business_name: Gross Margin (GAAP)
    description: External financial reporting margin
    context:
      use_when: "Financial statements, investor reporting, audits"
      not_for: "Operational decisions (use operational_margin)"
    relationships:
      - type: alternative_to
        target: gross_margin_operational
        distinction: "Excludes return adjustments"
      - type: alternative_to
        target: gross_margin_contribution
        distinction: "Excludes overhead allocation"

  - name: gross_margin_operational
    stereotype: metric
    business_name: Gross Margin (Operational)
    description: Internal operational performance margin
    context:
      use_when: "Operations review, performance management"
      not_for: "External reporting (use gaap)"
    includes:
      - return_adjustments

  - name: gross_margin_contribution
    stereotype: metric
    business_name: Contribution Margin
    description: Product-level profitability analysis
    context:
      use_when: "Product pricing, portfolio decisions"
      not_for: "Financial reporting, compensation"
    includes:
      - allocated_overhead
```

Now when an LLM is asked "what's our gross margin?", it has context to:
1. Ask which context (reporting? operations? product analysis?)
2. Explain the differences
3. Warn against using the wrong one

---

## The "Several Flavors of Member" Problem

Another Adrian example: multiple definitions of "member" in a subscription business.

Without ontology, a `member_type` column is just a dimension. With ontology:

```yaml
entity:
  name: member
  description: Subscription account holder

  subtypes:
    - name: trial_member
      description: In free trial period (30 days)
      context:
        counts_in: [user_growth, trial_conversion]
        excludes_from: [revenue_metrics, churn_analysis]

    - name: active_member
      description: Paying subscription, current billing
      context:
        counts_in: [mrr, active_users, churn_base]

    - name: churned_member
      description: Cancelled subscription, may retain data access
      context:
        counts_in: [churn_metrics, win_back_campaigns]
        excludes_from: [mrr, active_users]

    - name: enterprise_member
      description: Custom contract, annual billing
      context:
        counts_in: [arr, enterprise_revenue]
        special_handling: "Contact sales for churn status"

  business_rules:
    - "A member can only be one type at a time"
    - "Type transitions trigger lifecycle events"
    - "Enterprise members require manual churn classification"
```

This tells an AI agent:
- Don't count trial members in revenue
- Enterprise churn isn't in the standard churn table
- "Active users" has a specific definition

---

## Why This Matters for AI

Brudaru's thesis: **ontology will be the only thing not commoditized by LLMs**.

LLMs can generate SQL. They can build dashboards. They can write transformations. What they can't do is understand *your specific business context* unless you give it to them.

The race isn't to build more AI features. It's to **encode your business knowledge** in a form AI can consume.

Semantic layers don't do this. They're query interfaces, not knowledge bases.

Model-driven metadata does. Every entity definition, every relationship, every business rule becomes part of the ontological context an AI agent needs to reason correctly.

---

## From Reporting Analyst to Strategic Analyst

This shifts AI from correlation to causation:

| Without Ontology | With Ontology |
|------------------|---------------|
| "Ticket deflection up 45%" | "Ticket deflection up, but enterprise customers reducing engagement—churn risk" |
| "Revenue is $X" | "GAAP revenue is $X; operational revenue (excluding returns) is $Y" |
| "Customer count: 10,000" | "10,000 accounts: 7,000 active, 2,000 trial, 1,000 churned with data retention" |

The semantic layer tells AI what query to run. Ontology tells AI what the answer *means*.

---

## Practical Steps

1. **Audit your "flavors"**: Where do you have multiple definitions of the same concept? (Gross margin, member, revenue, customer)

2. **Document the distinctions**: Not just the calculation, but *when* and *why* each applies

3. **Embed in metadata**: Move from documentation to machine-readable model definitions

4. **Add relationship semantics**: Explicitly state "alternative_to", "component_of", "derived_from"

5. **Include context**: `use_when`, `not_for`, `audience`, `reporting_standard`

6. **Test with LLMs**: Ask an AI agent to answer business questions. Does it choose the right metric? Does it caveat appropriately?

---

## Key Takeaways

1. **Semantic layers solve query semantics, not business semantics**
2. **Ontology defines entities, relationships, and context—the "why" behind the "what"**
3. **LLMs need ontological context to reason correctly**
4. **Model-driven metadata embeds ontology at the foundation**
5. **Your private ontology is your moat against AI commoditization**

The companies that win won't be those with the best AI features. They'll be those whose AI agents understand what their data actually *means*.

---

*Thanks to Adrian Brudaru for the thought-provoking discussion and examples. His articles on [ontology in data engineering](https://dlthub.com/blog/ontology) and [moving beyond vibe coding](https://dlthub.com/blog/unvibe) are worth reading.*

---

**Tags**: `ontology`, `semantic-layer`, `ai`, `metadata`, `model-driven`
