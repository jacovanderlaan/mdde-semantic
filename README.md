# MDDE Semantic Module

Semantic layer with contracts, metrics, and decision traces.

---

## Features

- **Semantic Contracts**: Machine-enforceable interpretation rules
- **Metrics Layer**: Business metrics definitions
- **Decision Traces**: Who approved what, when, why
- **State Models**: Entity health tracking
- **Agent Permissions**: AI agent boundaries
- **Context Backbone**: Ontology + Glossary + Decisions

## Components

```
src/mdde/semantic/
├── contract/            # Semantic contracts (ADR-413)
├── metrics/             # Metrics definitions
├── traces/              # Decision traces (ADR-414)
├── state/               # State models
├── permissions/         # Agent permissions
└── backbone/            # Context backbone
```

## Key ADRs

| ADR | Title |
|-----|-------|
| ADR-413 | Semantic Contract Pattern |
| ADR-414 | Decision Traces |
| ADR-415 | Compliance Integration |

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
    warning:
      condition: "last_activity >= 90 days"
```

## Installation

```bash
pip install mdde-semantic
```

Requires `mdde-core` as dependency.

## License

Premium module - requires MDDE Enterprise license.
