"""
Ontology Data Models for MDDE Semantic Layer.

Provides formal representation of domain concepts and their relationships,
following OWL/RDF semantics for interoperability.

ADR-244: Ontology Support
Feb 2026
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


class ConceptType(Enum):
    """Types of ontology concepts."""
    CLASS = "class"  # OWL Class
    INDIVIDUAL = "individual"  # OWL Named Individual
    DATATYPE = "datatype"  # XSD Datatype


class PropertyType(Enum):
    """Types of ontology properties."""
    OBJECT_PROPERTY = "object_property"  # Links concepts to concepts
    DATA_PROPERTY = "data_property"  # Links concepts to literals
    ANNOTATION_PROPERTY = "annotation_property"  # Metadata


class PropertyCharacteristic(Enum):
    """OWL property characteristics."""
    FUNCTIONAL = "functional"
    INVERSE_FUNCTIONAL = "inverse_functional"
    TRANSITIVE = "transitive"
    SYMMETRIC = "symmetric"
    ASYMMETRIC = "asymmetric"
    REFLEXIVE = "reflexive"
    IRREFLEXIVE = "irreflexive"


class LinkType(Enum):
    """Types of entity-concept links."""
    INSTANCE_OF = "instance_of"  # Entity is instance of concept
    REPRESENTS = "represents"  # Entity represents concept
    RELATED_TO = "related_to"  # Entity is related to concept
    DERIVED_FROM = "derived_from"  # Entity derived from concept


@dataclass
class Ontology:
    """An ontology definition."""
    ontology_id: str
    name: str
    namespace: str  # Base URI for the ontology
    version: str = "1.0"
    description: str = ""
    prefix: str = ""  # Short prefix for URIs
    imports: List[str] = field(default_factory=list)  # Imported ontologies
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)

    def get_uri(self, local_name: str) -> str:
        """Get full URI for a local name."""
        return f"{self.namespace.rstrip('#/')}/{local_name}"

    def get_prefixed(self, local_name: str) -> str:
        """Get prefixed name."""
        return f"{self.prefix}:{local_name}" if self.prefix else local_name


@dataclass
class OntologyConcept:
    """A concept (class) in the ontology."""
    concept_id: str
    ontology_id: str
    label: str
    definition: str = ""
    concept_uri: Optional[str] = None  # Full URI
    concept_type: ConceptType = ConceptType.CLASS
    superclass_ids: List[str] = field(default_factory=list)  # Parent concepts
    equivalent_class_ids: List[str] = field(default_factory=list)  # OWL equivalentClass
    disjoint_class_ids: List[str] = field(default_factory=list)  # OWL disjointWith
    deprecated: bool = False
    examples: List[str] = field(default_factory=list)
    synonyms: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)

    @property
    def local_name(self) -> str:
        """Get local name from URI or ID."""
        if self.concept_uri and "#" in self.concept_uri:
            return self.concept_uri.split("#")[-1]
        elif self.concept_uri and "/" in self.concept_uri:
            return self.concept_uri.split("/")[-1]
        return self.concept_id


@dataclass
class OntologyProperty:
    """A property (relationship) in the ontology."""
    property_id: str
    ontology_id: str
    label: str
    definition: str = ""
    property_uri: Optional[str] = None
    property_type: PropertyType = PropertyType.OBJECT_PROPERTY
    domain_concept_ids: List[str] = field(default_factory=list)  # Subject concepts
    range_concept_ids: List[str] = field(default_factory=list)  # Object concepts/datatypes
    range_datatype: Optional[str] = None  # XSD datatype for data properties
    super_property_ids: List[str] = field(default_factory=list)  # Parent properties
    inverse_property_id: Optional[str] = None  # Inverse property
    characteristics: List[PropertyCharacteristic] = field(default_factory=list)
    deprecated: bool = False
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)

    @property
    def is_functional(self) -> bool:
        return PropertyCharacteristic.FUNCTIONAL in self.characteristics

    @property
    def is_inverse_functional(self) -> bool:
        return PropertyCharacteristic.INVERSE_FUNCTIONAL in self.characteristics

    @property
    def is_transitive(self) -> bool:
        return PropertyCharacteristic.TRANSITIVE in self.characteristics


@dataclass
class EntityConceptLink:
    """Link between an MDDE entity and an ontology concept."""
    link_id: str
    entity_id: str
    concept_id: str
    link_type: LinkType = LinkType.INSTANCE_OF
    confidence: float = 1.0  # 0.0 to 1.0
    rationale: str = ""  # Why this link exists
    created_by: Optional[str] = None
    created_at: datetime = field(default_factory=_utc_now)


@dataclass
class AttributeSemanticLink:
    """Semantic annotation for an MDDE attribute."""
    link_id: str
    attribute_id: str
    concept_id: Optional[str] = None  # Linked concept
    property_id: Optional[str] = None  # Linked property
    semantic_role: str = ""  # e.g., "identifier", "measure", "dimension"
    relationship_context: str = ""  # e.g., "hasShippingAddress" vs "hasBillingAddress"
    canonical_identifier: Optional[str] = None  # e.g., "iso_3166_1_alpha_2"
    created_at: datetime = field(default_factory=_utc_now)


@dataclass
class OntologyRestriction:
    """OWL restriction on a concept."""
    restriction_id: str
    concept_id: str
    property_id: str
    restriction_type: str  # "some", "all", "exact", "min", "max"
    value_concept_id: Optional[str] = None  # For object restrictions
    value_datatype: Optional[str] = None  # For data restrictions
    cardinality: Optional[int] = None  # For cardinality restrictions


@dataclass
class ConceptHierarchy:
    """Computed concept hierarchy for navigation."""
    concept_id: str
    depth: int
    path: List[str]  # Concept IDs from root
    children: List[str] = field(default_factory=list)
    descendant_count: int = 0


# Common ontology namespaces
STANDARD_NAMESPACES = {
    "owl": "http://www.w3.org/2002/07/owl#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "schema": "http://schema.org/",
    "qudt": "http://qudt.org/schema/qudt/",
}

# Common XSD datatypes
XSD_DATATYPES = {
    "string": "xsd:string",
    "integer": "xsd:integer",
    "decimal": "xsd:decimal",
    "float": "xsd:float",
    "double": "xsd:double",
    "boolean": "xsd:boolean",
    "date": "xsd:date",
    "dateTime": "xsd:dateTime",
    "time": "xsd:time",
    "anyURI": "xsd:anyURI",
}


# ==================== SKOS Support (v3.23.0) ====================

class SKOSRelationType(Enum):
    """SKOS semantic relations between concepts."""
    BROADER = "broader"  # skos:broader (parent)
    NARROWER = "narrower"  # skos:narrower (child)
    RELATED = "related"  # skos:related (associative)
    BROAD_MATCH = "broadMatch"  # Inter-scheme broader
    NARROW_MATCH = "narrowMatch"  # Inter-scheme narrower
    CLOSE_MATCH = "closeMatch"  # Near equivalent
    EXACT_MATCH = "exactMatch"  # Exact equivalent
    RELATED_MATCH = "relatedMatch"  # Inter-scheme related


class SKOSLabelType(Enum):
    """SKOS label types."""
    PREF_LABEL = "prefLabel"  # Preferred label (one per language)
    ALT_LABEL = "altLabel"  # Alternative labels (synonyms)
    HIDDEN_LABEL = "hiddenLabel"  # For search indexing only


@dataclass
class SKOSLabel:
    """A SKOS label with language tag."""
    label_type: SKOSLabelType
    value: str
    language: str = "en"


@dataclass
class SKOSConcept:
    """
    A SKOS concept for thesaurus/taxonomy support.

    SKOS provides a simpler model than OWL for controlled vocabularies,
    thesauri, and taxonomies.
    """
    concept_id: str
    scheme_id: str  # ConceptScheme this belongs to
    notation: Optional[str] = None  # Code/identifier
    labels: List[SKOSLabel] = field(default_factory=list)
    definition: str = ""
    scope_note: str = ""  # Usage guidance
    example: str = ""
    history_note: str = ""  # Change history
    editorial_note: str = ""  # Internal notes
    broader: List[str] = field(default_factory=list)  # Parent concept IDs
    narrower: List[str] = field(default_factory=list)  # Child concept IDs
    related: List[str] = field(default_factory=list)  # Related concept IDs
    close_match: List[str] = field(default_factory=list)  # External URIs
    exact_match: List[str] = field(default_factory=list)  # External URIs
    created: datetime = field(default_factory=_utc_now)
    modified: datetime = field(default_factory=_utc_now)

    @property
    def pref_label(self) -> str:
        """Get preferred label (first one)."""
        for label in self.labels:
            if label.label_type == SKOSLabelType.PREF_LABEL:
                return label.value
        return self.concept_id

    def get_labels_by_type(self, label_type: SKOSLabelType) -> List[str]:
        """Get all labels of a specific type."""
        return [l.value for l in self.labels if l.label_type == label_type]

    def add_label(self, value: str, label_type: SKOSLabelType = SKOSLabelType.ALT_LABEL, language: str = "en"):
        """Add a label to the concept."""
        self.labels.append(SKOSLabel(label_type=label_type, value=value, language=language))


@dataclass
class SKOSConceptScheme:
    """
    A SKOS Concept Scheme (vocabulary/taxonomy).

    Groups related SKOS concepts together.
    """
    scheme_id: str
    title: str
    description: str = ""
    namespace: str = ""  # Base URI
    creator: str = ""
    publisher: str = ""
    version: str = "1.0"
    top_concepts: List[str] = field(default_factory=list)  # Root concept IDs
    created: datetime = field(default_factory=_utc_now)
    modified: datetime = field(default_factory=_utc_now)


@dataclass
class OntologyMapping:
    """
    Mapping between ontology concepts (for alignment).

    Used to align concepts between different ontologies.
    """
    mapping_id: str
    source_ontology_id: str
    source_concept_id: str
    target_ontology_id: str
    target_concept_id: str
    mapping_type: str  # "equivalent", "broader", "narrower", "related"
    confidence: float = 1.0
    rationale: str = ""
    created_at: datetime = field(default_factory=_utc_now)


@dataclass
class SemanticSimilarity:
    """Result of semantic similarity calculation."""
    concept_a_id: str
    concept_b_id: str
    similarity_score: float  # 0.0 to 1.0
    method: str  # "path", "wu_palmer", "resnik", "lin"
    common_ancestor_id: Optional[str] = None


# SQL type to XSD datatype mapping
SQL_TO_XSD_MAP = [
    ("VARCHAR", "xsd:string"),
    ("CHAR", "xsd:string"),
    ("TEXT", "xsd:string"),
    ("BIGINT", "xsd:long"),
    ("SMALLINT", "xsd:short"),
    ("TINYINT", "xsd:byte"),
    ("INTEGER", "xsd:integer"),
    ("INT", "xsd:integer"),
    ("DECIMAL", "xsd:decimal"),
    ("NUMERIC", "xsd:decimal"),
    ("FLOAT", "xsd:float"),
    ("DOUBLE", "xsd:double"),
    ("REAL", "xsd:double"),
    ("BOOLEAN", "xsd:boolean"),
    ("BOOL", "xsd:boolean"),
    ("DATE", "xsd:date"),
    ("TIMESTAMP", "xsd:dateTime"),
    ("DATETIME", "xsd:dateTime"),
    ("TIME", "xsd:time"),
    ("BINARY", "xsd:base64Binary"),
    ("BLOB", "xsd:base64Binary"),
    ("UUID", "xsd:string"),
    ("JSON", "xsd:string"),
]


def sql_type_to_xsd(sql_type: str) -> str:
    """Convert SQL data type to XSD datatype."""
    sql_upper = sql_type.upper()
    for sql_pattern, xsd_type in SQL_TO_XSD_MAP:
        if sql_pattern in sql_upper:
            return xsd_type
    return "xsd:string"
