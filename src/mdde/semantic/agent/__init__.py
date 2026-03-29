"""
MDDE Semantic Agent Context.

Provides AI/LLM agent context through knowledge planes
for semantic reasoning about metadata.

ADR-246: AI Agent Context
Feb 2026
"""

from .models import (
    ContextType,
    QueryIntent,
    ResponseFormat,
    ContextRequest,
    EntityContext,
    RelationshipContext,
    LineageContext,
    GlossaryContext,
    MetricContext,
    ConceptContext,
    DomainContext,
    QualityContext,
    KnowledgePlane,
    AgentQuery,
    AgentResponse,
)

from .context_builder import (
    ContextBuilder,
    AgentContextProvider,
)

__all__ = [
    # Enums
    "ContextType",
    "QueryIntent",
    "ResponseFormat",
    # Context models
    "ContextRequest",
    "EntityContext",
    "RelationshipContext",
    "LineageContext",
    "GlossaryContext",
    "MetricContext",
    "ConceptContext",
    "DomainContext",
    "QualityContext",
    "KnowledgePlane",
    # Query/Response
    "AgentQuery",
    "AgentResponse",
    # Builders
    "ContextBuilder",
    "AgentContextProvider",
]
