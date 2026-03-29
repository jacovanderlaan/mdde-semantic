"""
AI Agent Context Data Models for MDDE Semantic Layer.

Provides structured context for AI/LLM agents to reason about
metadata, relationships, and business semantics.

ADR-246: AI Agent Context
Feb 2026
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


class ContextType(Enum):
    """Types of context information."""
    ENTITY = "entity"  # Entity metadata
    RELATIONSHIP = "relationship"  # Relationship info
    LINEAGE = "lineage"  # Data lineage
    GLOSSARY = "glossary"  # Business glossary
    METRIC = "metric"  # Business metrics
    CONCEPT = "concept"  # Ontology concepts
    DOMAIN = "domain"  # Data domains
    QUALITY = "quality"  # Data quality rules
    HISTORY = "history"  # Change history


class QueryIntent(Enum):
    """Intent classification for AI queries."""
    EXPLORE = "explore"  # Explore the data model
    UNDERSTAND = "understand"  # Understand meaning/semantics
    FIND = "find"  # Find specific entities/attributes
    ANALYZE = "analyze"  # Analyze relationships/lineage
    GENERATE = "generate"  # Generate code/queries
    VALIDATE = "validate"  # Validate against rules
    SUGGEST = "suggest"  # Suggest improvements


class ResponseFormat(Enum):
    """Output format for agent responses."""
    TEXT = "text"  # Plain text
    MARKDOWN = "markdown"  # Markdown formatted
    JSON = "json"  # JSON structure
    SQL = "sql"  # SQL query
    YAML = "yaml"  # YAML structure
    TABLE = "table"  # Tabular data


@dataclass
class ContextRequest:
    """Request for context information."""
    request_id: str
    context_types: List[ContextType] = field(default_factory=list)
    entity_ids: List[str] = field(default_factory=list)
    domain: Optional[str] = None
    search_terms: List[str] = field(default_factory=list)
    max_depth: int = 2  # Relationship traversal depth
    include_examples: bool = True
    include_statistics: bool = False
    created_at: datetime = field(default_factory=_utc_now)


@dataclass
class EntityContext:
    """Context information for an entity."""
    entity_id: str
    entity_name: str
    display_name: str
    description: str
    domain: Optional[str] = None
    stereotype: Optional[str] = None
    layer: Optional[str] = None
    attributes: List[Dict[str, Any]] = field(default_factory=list)
    primary_key: List[str] = field(default_factory=list)
    business_owner: Optional[str] = None
    data_classification: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    glossary_terms: List[str] = field(default_factory=list)


@dataclass
class RelationshipContext:
    """Context information for relationships."""
    from_entity: str
    to_entity: str
    relationship_type: str
    cardinality: str
    description: str = ""
    join_columns: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class LineageContext:
    """Context information for data lineage."""
    entity_id: str
    entity_name: str
    upstream: List[Dict[str, Any]] = field(default_factory=list)
    downstream: List[Dict[str, Any]] = field(default_factory=list)
    transformations: List[str] = field(default_factory=list)


@dataclass
class GlossaryContext:
    """Context information for business glossary."""
    term_id: str
    term_name: str
    definition: str
    synonyms: List[str] = field(default_factory=list)
    related_terms: List[str] = field(default_factory=list)
    domain: Optional[str] = None
    linked_entities: List[str] = field(default_factory=list)
    linked_attributes: List[str] = field(default_factory=list)


@dataclass
class MetricContext:
    """Context information for business metrics."""
    metric_id: str
    metric_name: str
    display_name: str
    description: str
    calculation: str
    unit: Optional[str] = None
    domain: Optional[str] = None
    source_entity: Optional[str] = None
    dimensions: List[str] = field(default_factory=list)


@dataclass
class ConceptContext:
    """Context information for ontology concepts."""
    concept_id: str
    concept_name: str
    definition: str
    parent_concepts: List[str] = field(default_factory=list)
    child_concepts: List[str] = field(default_factory=list)
    related_concepts: List[str] = field(default_factory=list)
    linked_entities: List[str] = field(default_factory=list)
    properties: List[str] = field(default_factory=list)


@dataclass
class DomainContext:
    """Context information for a data domain."""
    domain_id: str
    domain_name: str
    description: str
    owner: Optional[str] = None
    entities: List[str] = field(default_factory=list)
    glossary_terms: List[str] = field(default_factory=list)
    metrics: List[str] = field(default_factory=list)
    concepts: List[str] = field(default_factory=list)


@dataclass
class QualityContext:
    """Context information for data quality."""
    entity_id: str
    rules: List[Dict[str, Any]] = field(default_factory=list)
    expectations: List[Dict[str, Any]] = field(default_factory=list)
    validation_results: List[Dict[str, Any]] = field(default_factory=list)
    quality_score: Optional[float] = None


@dataclass
class KnowledgePlane:
    """
    Complete knowledge plane for AI agent context.

    Aggregates all semantic information for AI reasoning.
    """
    request: ContextRequest
    entities: List[EntityContext] = field(default_factory=list)
    relationships: List[RelationshipContext] = field(default_factory=list)
    lineage: List[LineageContext] = field(default_factory=list)
    glossary: List[GlossaryContext] = field(default_factory=list)
    metrics: List[MetricContext] = field(default_factory=list)
    concepts: List[ConceptContext] = field(default_factory=list)
    domains: List[DomainContext] = field(default_factory=list)
    quality: List[QualityContext] = field(default_factory=list)

    # Summary statistics
    total_entities: int = 0
    total_relationships: int = 0
    total_attributes: int = 0

    # Context window optimization
    token_estimate: int = 0
    truncated: bool = False

    generated_at: datetime = field(default_factory=_utc_now)

    def to_prompt_context(self, max_tokens: int = 8000) -> str:
        """
        Convert knowledge plane to prompt-friendly context.

        Args:
            max_tokens: Maximum estimated tokens

        Returns:
            Formatted context string for AI prompts
        """
        sections = []

        # Domains overview
        if self.domains:
            domain_section = "## Data Domains\n"
            for domain in self.domains:
                domain_section += f"- **{domain.domain_name}**: {domain.description}\n"
                domain_section += f"  - Entities: {', '.join(domain.entities[:5])}"
                if len(domain.entities) > 5:
                    domain_section += f" (+{len(domain.entities) - 5} more)"
                domain_section += "\n"
            sections.append(domain_section)

        # Entities
        if self.entities:
            entity_section = "## Entities\n"
            for entity in self.entities:
                entity_section += f"### {entity.entity_name}\n"
                entity_section += f"{entity.description}\n" if entity.description else ""
                if entity.attributes:
                    entity_section += "**Attributes:**\n"
                    for attr in entity.attributes[:10]:
                        entity_section += f"- `{attr.get('name')}` ({attr.get('data_type', 'unknown')})"
                        if attr.get('description'):
                            entity_section += f": {attr['description']}"
                        entity_section += "\n"
                    if len(entity.attributes) > 10:
                        entity_section += f"- ... and {len(entity.attributes) - 10} more attributes\n"
                entity_section += "\n"
            sections.append(entity_section)

        # Relationships
        if self.relationships:
            rel_section = "## Relationships\n"
            for rel in self.relationships:
                rel_section += f"- {rel.from_entity} → {rel.to_entity} ({rel.relationship_type}, {rel.cardinality})\n"
            sections.append(rel_section)

        # Glossary
        if self.glossary:
            glossary_section = "## Business Glossary\n"
            for term in self.glossary:
                glossary_section += f"- **{term.term_name}**: {term.definition}\n"
            sections.append(glossary_section)

        # Metrics
        if self.metrics:
            metric_section = "## Business Metrics\n"
            for metric in self.metrics:
                metric_section += f"- **{metric.metric_name}**: {metric.description}\n"
                metric_section += f"  - Calculation: `{metric.calculation}`\n"
            sections.append(metric_section)

        # Lineage summary
        if self.lineage:
            lineage_section = "## Data Lineage\n"
            for lin in self.lineage:
                lineage_section += f"- {lin.entity_name}: "
                if lin.upstream:
                    lineage_section += f"← {len(lin.upstream)} upstream "
                if lin.downstream:
                    lineage_section += f"→ {len(lin.downstream)} downstream"
                lineage_section += "\n"
            sections.append(lineage_section)

        result = "\n".join(sections)

        # Estimate tokens (rough: ~4 chars per token)
        self.token_estimate = len(result) // 4

        if self.token_estimate > max_tokens:
            # Truncate and mark
            char_limit = max_tokens * 4
            result = result[:char_limit]
            result += "\n\n*[Context truncated due to length]*"
            self.truncated = True

        return result

    def to_json_context(self) -> Dict[str, Any]:
        """
        Convert knowledge plane to JSON structure for API context.

        Returns:
            JSON-serializable dictionary
        """
        return {
            "summary": {
                "total_entities": self.total_entities,
                "total_relationships": self.total_relationships,
                "total_attributes": self.total_attributes,
                "domains": [d.domain_name for d in self.domains],
            },
            "entities": [
                {
                    "id": e.entity_id,
                    "name": e.entity_name,
                    "description": e.description,
                    "domain": e.domain,
                    "stereotype": e.stereotype,
                    "attributes": e.attributes,
                }
                for e in self.entities
            ],
            "relationships": [
                {
                    "from": r.from_entity,
                    "to": r.to_entity,
                    "type": r.relationship_type,
                    "cardinality": r.cardinality,
                }
                for r in self.relationships
            ],
            "glossary": [
                {
                    "term": g.term_name,
                    "definition": g.definition,
                    "domain": g.domain,
                }
                for g in self.glossary
            ],
            "metrics": [
                {
                    "name": m.metric_name,
                    "description": m.description,
                    "calculation": m.calculation,
                }
                for m in self.metrics
            ],
        }


@dataclass
class AgentQuery:
    """A query from an AI agent."""
    query_id: str
    query_text: str
    intent: Optional[QueryIntent] = None
    context_request: Optional[ContextRequest] = None
    response_format: ResponseFormat = ResponseFormat.MARKDOWN
    max_context_tokens: int = 8000
    created_at: datetime = field(default_factory=_utc_now)


@dataclass
class AgentResponse:
    """Response to an AI agent query."""
    query_id: str
    response_text: str
    response_format: ResponseFormat
    knowledge_plane: Optional[KnowledgePlane] = None
    confidence: float = 1.0
    sources: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=_utc_now)
