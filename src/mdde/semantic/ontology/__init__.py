"""
MDDE Ontology Support.

Formal representation of domain concepts and relationships.
Includes OWL, SKOS, and semantic similarity support.

ADR-359 adds Business Ontology Layer for:
- Causal relationships (why things connect)
- Context-aware metric interpretation
- Reasoning rules for AI guidance
"""

from .models import (
    # Core ontology
    Ontology,
    OntologyConcept,
    OntologyProperty,
    EntityConceptLink,
    AttributeSemanticLink,
    OntologyRestriction,
    ConceptHierarchy,
    ConceptType,
    PropertyType,
    PropertyCharacteristic,
    LinkType,
    # SKOS support (v3.23.0)
    SKOSConcept,
    SKOSConceptScheme,
    SKOSLabel,
    SKOSLabelType,
    SKOSRelationType,
    OntologyMapping,
    SemanticSimilarity,
    sql_type_to_xsd,
    # Constants
    STANDARD_NAMESPACES,
    XSD_DATATYPES,
    SQL_TO_XSD_MAP,
)

from .manager import OntologyManager

# Business Ontology Layer (ADR-359)
from .business_ontology import (
    CausalType,
    CausalDirection,
    Sentiment,
    CausalRelationship,
    BusinessConcept,
    Threshold,
    MetricInterpretation,
    InterpretationResult,
    BusinessOntology,
    BusinessOntologyManager,
)

from .questionnaire import (
    QuestionCategory,
    AnswerType,
    QuestionOption,
    OntologyQuestion,
    QuestionAnswer,
    OntologyQuestionnaire,
    STANDARD_QUESTIONS,
)

# SQL-Executable Ontology (ADR-364)
from .executable import (
    RelationshipType,
    JoinType,
    Cardinality,
    FilterCompositionMode,
    ComposedFilter,
    OptimizationHint,
    QueryContext,
    ExecutableMetric,
    SemanticRelationship,
    ExecutableQuery,
    ExecutableOntology,
    OntologyQuery,
)

__all__ = [
    # Core Models
    "Ontology",
    "OntologyConcept",
    "OntologyProperty",
    "EntityConceptLink",
    "AttributeSemanticLink",
    "OntologyRestriction",
    "ConceptHierarchy",
    # Enums
    "ConceptType",
    "PropertyType",
    "PropertyCharacteristic",
    "LinkType",
    # SKOS Models (v3.23.0)
    "SKOSConcept",
    "SKOSConceptScheme",
    "SKOSLabel",
    "SKOSLabelType",
    "SKOSRelationType",
    "OntologyMapping",
    "SemanticSimilarity",
    # Utilities
    "sql_type_to_xsd",
    # Constants
    "STANDARD_NAMESPACES",
    "XSD_DATATYPES",
    "SQL_TO_XSD_MAP",
    # Manager
    "OntologyManager",
    # Business Ontology Layer (ADR-359)
    "CausalType",
    "CausalDirection",
    "Sentiment",
    "CausalRelationship",
    "BusinessConcept",
    "Threshold",
    "MetricInterpretation",
    "InterpretationResult",
    "BusinessOntology",
    "BusinessOntologyManager",
    # Questionnaire (ADR-362)
    "QuestionCategory",
    "AnswerType",
    "QuestionOption",
    "OntologyQuestion",
    "QuestionAnswer",
    "OntologyQuestionnaire",
    "STANDARD_QUESTIONS",
    # SQL-Executable Ontology (ADR-364)
    "RelationshipType",
    "JoinType",
    "Cardinality",
    "FilterCompositionMode",
    "ComposedFilter",
    "OptimizationHint",
    "QueryContext",
    "ExecutableMetric",
    "SemanticRelationship",
    "ExecutableQuery",
    "ExecutableOntology",
    "OntologyQuery",
]
