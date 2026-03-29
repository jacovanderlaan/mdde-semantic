# MDDE Aboutness Models
# ADR-247: Aboutness Layer
# Feb 2026

"""Data models for semantic aboutness."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class AboutnessDimension(Enum):
    """
    Classification of what data semantically represents.

    "Aboutness" captures the fundamental semantic nature of data -
    not just its type, but what it means in the real world.
    """

    # Quantitative: captures magnitude, amount, quantity
    MEASURE = "measure"

    # Identity: uniquely distinguishes instances
    IDENTIFIER = "identifier"

    # Categorical: groups or categorizes
    CLASSIFIER = "classifier"

    # Time-related: when something occurred or applies
    TEMPORAL = "temporal"

    # Relational: connection between entities
    RELATIONSHIP = "relationship"

    # Descriptive: characteristic or property
    QUALITY = "quality"

    # Geographic: location or spatial information
    SPATIAL = "spatial"

    # Lifecycle: current condition or phase
    STATE = "state"

    # Binary: presence/absence indicator
    FLAG = "flag"

    # Computed: calculated from other attributes
    DERIVED = "derived"


class SemanticRole(Enum):
    """How data can be used semantically in queries and analysis."""

    # Can be summed, averaged, counted
    AGGREGATABLE = "aggregatable"

    # Can filter/slice data
    FILTERABLE = "filterable"

    # Can group by for aggregation
    GROUPABLE = "groupable"

    # Has meaningful sort order
    SORTABLE = "sortable"

    # Can join with other entities
    JOINABLE = "joinable"

    # Source for calculations
    DERIVABLE = "derivable"

    # Suitable for display/reporting
    DISPLAYABLE = "displayable"

    # Can be used in WHERE clauses
    SLICEABLE = "sliceable"


class DependencyType(Enum):
    """Types of semantic dependencies between concepts."""

    # Concept A requires concept B to exist
    REQUIRES = "requires"

    # Concept A implies concept B
    IMPLIES = "implies"

    # Concept A conflicts with concept B
    CONFLICTS = "conflicts"

    # Concept A refines/specializes concept B
    REFINES = "refines"

    # Concept A is equivalent to concept B
    EQUIVALENT = "equivalent"

    # Concept A is derived from concept B
    DERIVED_FROM = "derived_from"


@dataclass
class EntityAboutness:
    """
    Semantic intent and purpose for an entity.

    Captures why an entity exists and what real-world concept it models.
    """

    entity_id: str
    purpose: str  # Why this entity exists
    real_world_object: str  # What it represents (e.g., "Customer", "Transaction")
    aboutness_dimension: AboutnessDimension  # Primary semantic dimension

    # Optional fields
    model_id: Optional[str] = None
    aboutness_id: Optional[str] = None
    business_context: Optional[str] = None  # Business domain context
    semantic_category: Optional[str] = None  # Business category

    # Usage
    business_use_cases: List[str] = field(default_factory=list)
    stakeholder_groups: List[str] = field(default_factory=list)

    # Ontology links
    represents_concept: Optional[str] = None  # URI to ontology concept
    equivalent_to: List[str] = field(default_factory=list)  # External concept URIs

    # Metadata
    confidence_score: float = 1.0  # 0.0-1.0
    source: str = "manual"  # manual, inferred, imported
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "aboutness_id": self.aboutness_id,
            "entity_id": self.entity_id,
            "model_id": self.model_id,
            "purpose": self.purpose,
            "real_world_object": self.real_world_object,
            "aboutness_dimension": self.aboutness_dimension.value,
            "business_context": self.business_context,
            "semantic_category": self.semantic_category,
            "business_use_cases": self.business_use_cases,
            "stakeholder_groups": self.stakeholder_groups,
            "represents_concept": self.represents_concept,
            "equivalent_to": self.equivalent_to,
            "confidence_score": self.confidence_score,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EntityAboutness":
        """Create from dictionary."""
        return cls(
            aboutness_id=data.get("aboutness_id"),
            entity_id=data["entity_id"],
            model_id=data.get("model_id"),
            purpose=data["purpose"],
            real_world_object=data["real_world_object"],
            aboutness_dimension=AboutnessDimension(data["aboutness_dimension"]),
            business_context=data.get("business_context"),
            semantic_category=data.get("semantic_category"),
            business_use_cases=data.get("business_use_cases", []),
            stakeholder_groups=data.get("stakeholder_groups", []),
            represents_concept=data.get("represents_concept"),
            equivalent_to=data.get("equivalent_to", []),
            confidence_score=data.get("confidence_score", 1.0),
            source=data.get("source", "manual"),
        )


@dataclass
class AttributeAboutness:
    """
    Semantic intent and purpose for an attribute.

    Captures why an attribute exists and what aspect of reality it models.
    """

    entity_id: str
    attribute_id: str
    intent: str  # Why this attribute exists
    aboutness_dimension: AboutnessDimension
    semantic_role: SemanticRole

    # Optional fields
    model_id: Optional[str] = None
    aboutness_id: Optional[str] = None

    # Dimension-specific descriptions
    measures_what: Optional[str] = None  # For MEASURE: what is quantified
    identifies_what: Optional[str] = None  # For IDENTIFIER: what is identified
    classifies_what: Optional[str] = None  # For CLASSIFIER: what is categorized
    relates_to: Optional[str] = None  # For RELATIONSHIP: target entity

    # Ontology mapping
    represents_property: Optional[str] = None  # URI to ontology property
    canonical_name: Optional[str] = None  # Cross-system standard name

    # Expected behavior based on aboutness
    expected_behavior: Dict[str, Any] = field(default_factory=dict)
    # e.g., {"aggregation": "SUM", "nullability": "required", "positive_only": true}

    # Semantic lineage
    derived_from: Optional[str] = None  # Another attribute this derives from
    semantic_transform: Optional[str] = None  # How meaning transforms

    # Metadata
    confidence_score: float = 1.0
    source: str = "manual"
    created_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "aboutness_id": self.aboutness_id,
            "entity_id": self.entity_id,
            "attribute_id": self.attribute_id,
            "model_id": self.model_id,
            "intent": self.intent,
            "aboutness_dimension": self.aboutness_dimension.value,
            "semantic_role": self.semantic_role.value,
            "measures_what": self.measures_what,
            "identifies_what": self.identifies_what,
            "classifies_what": self.classifies_what,
            "relates_to": self.relates_to,
            "represents_property": self.represents_property,
            "canonical_name": self.canonical_name,
            "expected_behavior": self.expected_behavior,
            "derived_from": self.derived_from,
            "semantic_transform": self.semantic_transform,
            "confidence_score": self.confidence_score,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AttributeAboutness":
        """Create from dictionary."""
        return cls(
            aboutness_id=data.get("aboutness_id"),
            entity_id=data["entity_id"],
            attribute_id=data["attribute_id"],
            model_id=data.get("model_id"),
            intent=data["intent"],
            aboutness_dimension=AboutnessDimension(data["aboutness_dimension"]),
            semantic_role=SemanticRole(data["semantic_role"]),
            measures_what=data.get("measures_what"),
            identifies_what=data.get("identifies_what"),
            classifies_what=data.get("classifies_what"),
            relates_to=data.get("relates_to"),
            represents_property=data.get("represents_property"),
            canonical_name=data.get("canonical_name"),
            expected_behavior=data.get("expected_behavior", {}),
            derived_from=data.get("derived_from"),
            semantic_transform=data.get("semantic_transform"),
            confidence_score=data.get("confidence_score", 1.0),
            source=data.get("source", "manual"),
        )


@dataclass
class SemanticDependency:
    """
    Semantic dependency between concepts.

    Separate from technical lineage - captures conceptual relationships.
    """

    dependency_id: Optional[str]
    source_concept: str  # Concept or attribute path
    target_concept: str  # Concept or attribute path
    dependency_type: DependencyType
    strength: str = "moderate"  # strong, moderate, weak
    description: Optional[str] = None
    model_id: Optional[str] = None
    created_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dependency_id": self.dependency_id,
            "source_concept": self.source_concept,
            "target_concept": self.target_concept,
            "dependency_type": self.dependency_type.value,
            "strength": self.strength,
            "description": self.description,
            "model_id": self.model_id,
        }


@dataclass
class AboutnessValidation:
    """
    Result of aboutness validation check.

    Captures semantic consistency issues found during validation.
    """

    validation_id: Optional[str]
    entity_id: Optional[str]
    attribute_id: Optional[str]
    model_id: Optional[str]
    check_code: str  # A001, A002, etc.
    severity: str  # error, warning, info
    message: str
    recommendation: Optional[str] = None
    validated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "validation_id": self.validation_id,
            "entity_id": self.entity_id,
            "attribute_id": self.attribute_id,
            "model_id": self.model_id,
            "check_code": self.check_code,
            "severity": self.severity,
            "message": self.message,
            "recommendation": self.recommendation,
        }
