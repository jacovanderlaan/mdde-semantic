# ADR-246: AI Agent Context

## Status
Accepted

## Date
2026-02-15

## Context

AI/LLM agents need structured context to reason about data models, relationships, and business semantics. A Knowledge Plane aggregates metadata from various sources into a format optimized for AI consumption, enabling semantic search, natural language queries, and intelligent code generation.

### Requirements
1. Aggregate metadata for AI context windows
2. Support various context types (entities, relationships, glossary, etc.)
3. Optimize for token limits
4. Intent-based context building
5. Multiple output formats (text, JSON, markdown)
6. Search and discovery capabilities

## Decision

### 1. Agent Context Module

Location: `src/mdde/semantic/agent/`

**Components**:
- `models.py` - Context data models and KnowledgePlane
- `context_builder.py` - ContextBuilder and AgentContextProvider
- `__init__.py` - Module exports

### 2. Context Types

```python
class ContextType(Enum):
    ENTITY = "entity"           # Entity metadata
    RELATIONSHIP = "relationship"  # Relationships
    LINEAGE = "lineage"         # Data lineage
    GLOSSARY = "glossary"       # Business glossary
    METRIC = "metric"           # Business metrics
    CONCEPT = "concept"         # Ontology concepts
    DOMAIN = "domain"           # Data domains
    QUALITY = "quality"         # Data quality rules
    HISTORY = "history"         # Change history
```

### 3. Query Intents

```python
class QueryIntent(Enum):
    EXPLORE = "explore"         # Explore data model
    UNDERSTAND = "understand"   # Understand semantics
    FIND = "find"               # Find entities/attributes
    ANALYZE = "analyze"         # Analyze relationships
    GENERATE = "generate"       # Generate code/queries
    VALIDATE = "validate"       # Validate against rules
    SUGGEST = "suggest"         # Suggest improvements
```

### 4. Knowledge Plane

The KnowledgePlane is the core data structure for AI context:

```python
@dataclass
class KnowledgePlane:
    request: ContextRequest
    entities: List[EntityContext]
    relationships: List[RelationshipContext]
    lineage: List[LineageContext]
    glossary: List[GlossaryContext]
    metrics: List[MetricContext]
    concepts: List[ConceptContext]
    domains: List[DomainContext]
    quality: List[QualityContext]

    # Statistics
    total_entities: int
    total_relationships: int
    total_attributes: int

    # Token optimization
    token_estimate: int
    truncated: bool
```

### 5. Usage Examples

**Basic Context Building**:
```python
from mdde.semantic import (
    AgentContextProvider, ContextType, ContextRequest
)

provider = AgentContextProvider(conn)

# Get context for natural language query
context = provider.get_context_for_query(
    "What is the customer order relationship?",
    max_tokens=4000,
)

print(context)
# ## Entities
# ### Customer
# A person or organization that purchases products
# **Attributes:**
# - `customer_id` (VARCHAR): Unique identifier
# - `customer_name` (VARCHAR): Full name
# ...
```

**Entity-Specific Context**:
```python
plane = provider.get_context_for_entity(
    entity_name="order_fact",
    include_related=True,
)

# Access structured data
for entity in plane.entities:
    print(f"{entity.entity_name}: {entity.description}")
    for attr in entity.attributes:
        print(f"  - {attr['name']}")
```

**Domain Context**:
```python
plane = provider.get_context_for_domain("Sales")

print(f"Entities: {len(plane.entities)}")
print(f"Metrics: {len(plane.metrics)}")
print(f"Glossary terms: {len(plane.glossary)}")
```

**Search Context**:
```python
plane = provider.search_context(
    search_terms=["customer", "revenue"],
    context_types=[ContextType.ENTITY, ContextType.METRIC],
)
```

**JSON Context for API**:
```python
json_context = plane.to_json_context()

# Use with AI API
response = client.chat(
    messages=[
        {"role": "system", "content": f"Context: {json.dumps(json_context)}"},
        {"role": "user", "content": "Generate a SQL query for total revenue by customer"},
    ]
)
```

### 6. Context Request

```python
@dataclass
class ContextRequest:
    request_id: str
    context_types: List[ContextType]  # What to include
    entity_ids: List[str]             # Specific entities
    domain: Optional[str]             # Filter by domain
    search_terms: List[str]           # Search keywords
    max_depth: int = 2                # Relationship depth
    include_examples: bool = True
    include_statistics: bool = False
```

### 7. Output Formats

**Prompt Context** (for LLM prompts):
```python
context_str = plane.to_prompt_context(max_tokens=8000)
# Returns markdown-formatted context optimized for token limits
```

**JSON Context** (for structured APIs):
```python
context_json = plane.to_json_context()
# Returns JSON-serializable dictionary
```

### 8. Intent Detection

The provider automatically detects intent from natural language:

```python
# Auto-detected intents:
"What is a customer?" → QueryIntent.UNDERSTAND
"Find orders table" → QueryIntent.FIND
"How does data flow from source to target?" → QueryIntent.ANALYZE
"Generate SQL for revenue" → QueryIntent.GENERATE
"Validate the customer entity" → QueryIntent.VALIDATE
"Suggest improvements" → QueryIntent.SUGGEST
```

### 9. Token Optimization

- Estimates token count (~4 chars per token)
- Truncates context when exceeding limits
- Prioritizes most relevant information
- Marks truncated responses

```python
# Check truncation
if plane.truncated:
    print(f"Context truncated at ~{plane.token_estimate} tokens")
```

## Consequences

### Positive
- AI-ready metadata context
- Token-optimized output
- Multiple output formats
- Intent-based context building
- Search and discovery

### Negative
- Token estimation is approximate
- Context selection may miss relevant info
- Requires careful prompt engineering

### Risks
- Context too large for model limits
- Missing relevant context
- Over-reliance on automatic intent detection

## Implementation

Files created:
1. `src/mdde/semantic/agent/models.py` - Context data models
2. `src/mdde/semantic/agent/context_builder.py` - Builders
3. `src/mdde/semantic/agent/__init__.py` - Module exports

## Integration with GenAI

The AI Agent Context integrates with MDDE's existing GenAI module:

```python
from mdde.genai import MetadataCopilot
from mdde.semantic import AgentContextProvider

provider = AgentContextProvider(conn)
copilot = MetadataCopilot(conn)

# Get context
plane = provider.get_context_for_domain("Sales")

# Use with copilot
suggestion = copilot.suggest_entity_improvements(
    entity_id="order_fact",
    context=plane.to_json_context(),
)
```

## References

- [LangChain Context](https://docs.langchain.com/docs/)
- [Semantic Kernel](https://learn.microsoft.com/semantic-kernel/)
- [Knowledge Plane Analysis](../research/knowledge-plane-analysis.md)
- [Juan Sequeda - AI + Knowledge Graphs](https://juansequeda.com/)
