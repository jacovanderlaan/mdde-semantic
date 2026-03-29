# MDDE Semantic

Semantic layer with contracts, metrics, and decision traces.

## Purpose

Provides semantic layer capabilities:
- Semantic contracts (machine-enforceable rules)
- Business metrics definitions
- Decision traces (who approved what, when, why)
- Entity state models
- AI agent permissions
- Context backbone (Ontology + Glossary + Decisions)

## Structure

```
src/mdde/semantic/
├── contract/            # Semantic contracts
├── metrics/             # Metrics definitions
├── traces/              # Decision traces
├── state/               # State models
├── permissions/         # Agent permissions
└── backbone/            # Context backbone
```

## Key Modules

| Module | Purpose |
|--------|---------|
| contract | Semantic contracts (ADR-413) |
| metrics | Business metrics definitions |
| traces | Decision traces (ADR-414) |
| state | Entity health tracking |
| permissions | AI agent boundaries |

## Semantic Contract Example

```yaml
semantic_contract:
  entity: customer

  interpretation_rules:
    must_use:
      - customer_hk
    forbidden:
      - raw_password

  state_model:
    healthy:
      condition: "last_activity < 90 days"
```

## ADRs

- ADR-413: Semantic Contract Pattern
- ADR-414: Decision Traces
- ADR-415: Compliance Integration

## Part of MDDE Enterprise

| Repository | Description |
|------------|-------------|
| mdde | Core framework (open source) |
| mdde-semantic | Semantic layer (this repo) |
