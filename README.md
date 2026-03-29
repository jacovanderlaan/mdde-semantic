# MDDE Semantic Module

Semantic layer with contracts, metrics, and decision traces.

## Installation

```bash
pip install mdde-semantic
```

Requires `mdde-core` as dependency.

## Features

- **Semantic Contracts**: Machine-enforceable interpretation rules
- **Metrics Layer**: Business metrics definitions
- **Decision Traces**: Who approved what, when, why
- **State Models**: Entity health tracking
- **Agent Permissions**: AI agent boundaries
- **Context Backbone**: Ontology + Glossary + Decisions
- **Aboutness Layer**: Semantic annotation of data meaning

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

## Usage

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

## Documentation

- [Modules](docs/modules/) - Aboutness layer documentation
- [Instructions](docs/instructions/) - Semantic layer configuration
- [Research](docs/research/) - Knowledge plane analysis
- [Inspiration](docs/inspiration/) - DBML semantic layer inspiration
- [Demos](demos/semantic_layer/) - Semantic layer demos
- [Roadmap](docs/roadmap/) - Semantic roadmap
- [ADRs](docs/adrs/) - 13 architecture decisions
- [CLAUDE.md](CLAUDE.md) - Repository documentation

## Key ADRs

| ADR | Title |
|-----|-------|
| ADR-244 | Ontology Support |
| ADR-245 | Metrics Layer |
| ADR-301 | Semantic Layer Module |
| ADR-413 | Semantic Contract Pattern |
| ADR-414 | Decision Traces |
| ADR-412 | USS-Aware Text-to-SQL |

## Part of MDDE Open-Core

| Repository | Description |
|------------|-------------|
| [mdde](https://github.com/jacovanderlaan/mdde) | Core framework (open source) |
| mdde-semantic | Semantic layer (this repo) |

## License

Enterprise module - requires MDDE Enterprise license.
