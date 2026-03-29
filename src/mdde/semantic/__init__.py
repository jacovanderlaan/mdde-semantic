"""
MDDE Semantic Layer.

Provides semantic capabilities for metadata:
- Ontology support (OWL/RDF concepts and properties)
- Metrics layer (business metrics with semantic queries)
- AI Agent context (knowledge planes for LLM reasoning)
- Semantic Layer (ADR-301): Complete semantic model with metrics, dimensions, hierarchies
  Inspired by Patrick Okare's "Five Must-Have Layers" - Analytics & Consumption Layer

Modules:
- ontology: OWL-inspired concepts, properties, entity-concept linking
- metrics: Business metrics, dimensions, semantic query generation
- agent: Knowledge plane context for AI/LLM agents
- model/types/manager: Complete semantic layer model (ADR-301)

ADR-244: Ontology Support
ADR-245: Metrics Layer
ADR-246: AI Agent Context
ADR-301: Semantic Layer Module (Patrick Okare inspired)
Feb 2026
"""

# Ontology module (ADR-244)
from .ontology import (
    # Enums
    ConceptType,
    PropertyType,
    PropertyCharacteristic,
    LinkType,
    # Data classes
    Ontology,
    OntologyConcept,
    OntologyProperty,
    OntologyRestriction,
    EntityConceptLink,
    AttributeSemanticLink,
    ConceptHierarchy,
    # Manager
    OntologyManager,
    # Constants
    STANDARD_NAMESPACES,
    XSD_DATATYPES,
)

# Metrics module (ADR-245)
from .metrics import (
    # Enums
    MetricType,
    AggregationType,
    TimeGrain,
    DimensionRole,
    MetricStatus,
    # Data classes
    MetricDefinition,
    MetricDimension,
    MetricFilter,
    DerivedMetricFormula,
    MetricGoal,
    MetricAlert,
    MetricQuery,
    MetricQueryResult,
    # Manager
    MetricsManager,
)

# Agent context module (ADR-246)
from .agent import (
    # Enums
    ContextType,
    QueryIntent,
    ResponseFormat,
    # Context models
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
    # Query/Response
    AgentQuery,
    AgentResponse,
    # Builders
    ContextBuilder,
    AgentContextProvider,
)

# Semantic Layer Model (ADR-301) - Patrick Okare inspired
from .types import (
    # Re-export types (note: some overlap with metrics module, these are ADR-301 specific)
    MetricType as SemanticMetricType,
    AggregationType as SemanticAggregationType,
    DimensionType,
    HierarchyType,
    TimeGrain as SemanticTimeGrain,
    MetricFilter as SemanticMetricFilter,
    Metric as SemanticMetric,
    Dimension as SemanticDimension,
    HierarchyLevel,
    Hierarchy,
    generate_id as semantic_generate_id,
)
from .model import SemanticModel
from .manager import SemanticLayerManager

__all__ = [
    # ==================== Ontology (ADR-244) ====================
    # Enums
    "ConceptType",
    "PropertyType",
    "PropertyCharacteristic",
    "LinkType",
    # Data classes
    "Ontology",
    "OntologyConcept",
    "OntologyProperty",
    "OntologyRestriction",
    "EntityConceptLink",
    "AttributeSemanticLink",
    "ConceptHierarchy",
    # Manager
    "OntologyManager",
    # Constants
    "STANDARD_NAMESPACES",
    "XSD_DATATYPES",
    # ==================== Metrics (ADR-245) ====================
    # Enums
    "MetricType",
    "AggregationType",
    "TimeGrain",
    "DimensionRole",
    "MetricStatus",
    # Data classes
    "MetricDefinition",
    "MetricDimension",
    "MetricFilter",
    "DerivedMetricFormula",
    "MetricGoal",
    "MetricAlert",
    "MetricQuery",
    "MetricQueryResult",
    # Manager
    "MetricsManager",
    # ==================== Agent Context (ADR-246) ====================
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
    # ==================== Semantic Layer (ADR-301) ====================
    # Patrick Okare "Five Must-Have Layers" inspired
    # Enums (aliased to avoid conflicts with ADR-245)
    "SemanticMetricType",
    "SemanticAggregationType",
    "DimensionType",
    "HierarchyType",
    "SemanticTimeGrain",
    # Data classes
    "SemanticMetricFilter",
    "SemanticMetric",
    "SemanticDimension",
    "HierarchyLevel",
    "Hierarchy",
    "SemanticModel",
    # Manager
    "SemanticLayerManager",
    # Utilities
    "semantic_generate_id",
]
