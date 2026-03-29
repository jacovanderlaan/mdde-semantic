"""
Ontology Manager for MDDE Semantic Layer.

Manages ontology concepts, properties, and entity linkages.
Supports CRUD operations, hierarchy traversal, and reasoning.

ADR-244: Ontology Support
Feb 2026
"""

from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import logging
import uuid

from .models import (
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
    STANDARD_NAMESPACES,
    sql_type_to_xsd,
)

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


def _generate_id(prefix: str = "") -> str:
    """Generate a unique ID."""
    return f"{prefix}{uuid.uuid4().hex[:12]}"


class OntologyManager:
    """
    Manages ontology definitions and entity linkages.

    Provides:
    - Ontology CRUD operations
    - Concept hierarchy management
    - Property relationship management
    - Entity-concept linking
    - Basic inference/reasoning
    """

    def __init__(self, conn):
        """
        Initialize ontology manager.

        Args:
            conn: DuckDB connection to metadata database
        """
        self.conn = conn
        self._ensure_tables()

    def _ensure_tables(self):
        """Ensure ontology tables exist."""
        # Check if tables exist, create if not
        try:
            self.conn.execute("SELECT 1 FROM metadata.ontology LIMIT 1")
        except Exception:
            self._create_tables()

    def _create_tables(self):
        """Create ontology tables."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.ontology (
                ontology_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                namespace VARCHAR NOT NULL,
                version VARCHAR DEFAULT '1.0',
                description VARCHAR,
                prefix VARCHAR,
                imports VARCHAR,  -- JSON array
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.ontology_concept (
                concept_id VARCHAR PRIMARY KEY,
                ontology_id VARCHAR NOT NULL,
                label VARCHAR NOT NULL,
                definition VARCHAR,
                concept_uri VARCHAR,
                concept_type VARCHAR DEFAULT 'class',
                superclass_ids VARCHAR,  -- JSON array
                equivalent_class_ids VARCHAR,
                disjoint_class_ids VARCHAR,
                deprecated BOOLEAN DEFAULT FALSE,
                examples VARCHAR,  -- JSON array
                synonyms VARCHAR,  -- JSON array
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (ontology_id) REFERENCES metadata.ontology(ontology_id)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.ontology_property (
                property_id VARCHAR PRIMARY KEY,
                ontology_id VARCHAR NOT NULL,
                label VARCHAR NOT NULL,
                definition VARCHAR,
                property_uri VARCHAR,
                property_type VARCHAR DEFAULT 'object_property',
                domain_concept_ids VARCHAR,  -- JSON array
                range_concept_ids VARCHAR,  -- JSON array
                range_datatype VARCHAR,
                super_property_ids VARCHAR,
                inverse_property_id VARCHAR,
                characteristics VARCHAR,  -- JSON array
                deprecated BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (ontology_id) REFERENCES metadata.ontology(ontology_id)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.entity_concept_link (
                link_id VARCHAR PRIMARY KEY,
                entity_id VARCHAR NOT NULL,
                concept_id VARCHAR NOT NULL,
                link_type VARCHAR DEFAULT 'instance_of',
                confidence DECIMAL(3,2) DEFAULT 1.0,
                rationale VARCHAR,
                created_by VARCHAR,
                created_at TIMESTAMP,
                FOREIGN KEY (concept_id) REFERENCES metadata.ontology_concept(concept_id)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.attribute_semantic_link (
                link_id VARCHAR PRIMARY KEY,
                attribute_id VARCHAR NOT NULL,
                concept_id VARCHAR,
                property_id VARCHAR,
                semantic_role VARCHAR,
                relationship_context VARCHAR,
                canonical_identifier VARCHAR,
                created_at TIMESTAMP
            )
        """)

        logger.info("Ontology tables created")

    # ==================== Ontology CRUD ====================

    def create_ontology(self, ontology: Ontology) -> str:
        """
        Create a new ontology.

        Args:
            ontology: Ontology to create

        Returns:
            Created ontology ID
        """
        import json

        self.conn.execute(
            """
            INSERT INTO metadata.ontology
            (ontology_id, name, namespace, version, description, prefix, imports, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ontology.ontology_id,
                ontology.name,
                ontology.namespace,
                ontology.version,
                ontology.description,
                ontology.prefix,
                json.dumps(ontology.imports),
                ontology.created_at,
                ontology.updated_at,
            ]
        )

        logger.info(f"Created ontology: {ontology.ontology_id}")
        return ontology.ontology_id

    def get_ontology(self, ontology_id: str) -> Optional[Ontology]:
        """Get an ontology by ID."""
        import json

        row = self.conn.execute(
            "SELECT * FROM metadata.ontology WHERE ontology_id = ?",
            [ontology_id]
        ).fetchone()

        if not row:
            return None

        return Ontology(
            ontology_id=row[0],
            name=row[1],
            namespace=row[2],
            version=row[3],
            description=row[4] or "",
            prefix=row[5] or "",
            imports=json.loads(row[6]) if row[6] else [],
            created_at=row[7],
            updated_at=row[8],
        )

    def list_ontologies(self) -> List[Ontology]:
        """List all ontologies."""
        import json

        rows = self.conn.execute(
            "SELECT * FROM metadata.ontology ORDER BY name"
        ).fetchall()

        return [
            Ontology(
                ontology_id=row[0],
                name=row[1],
                namespace=row[2],
                version=row[3],
                description=row[4] or "",
                prefix=row[5] or "",
                imports=json.loads(row[6]) if row[6] else [],
                created_at=row[7],
                updated_at=row[8],
            )
            for row in rows
        ]

    def delete_ontology(self, ontology_id: str) -> bool:
        """Delete an ontology and all its concepts/properties."""
        # Delete in order due to foreign keys
        self.conn.execute(
            "DELETE FROM metadata.entity_concept_link WHERE concept_id IN "
            "(SELECT concept_id FROM metadata.ontology_concept WHERE ontology_id = ?)",
            [ontology_id]
        )
        self.conn.execute(
            "DELETE FROM metadata.ontology_property WHERE ontology_id = ?",
            [ontology_id]
        )
        self.conn.execute(
            "DELETE FROM metadata.ontology_concept WHERE ontology_id = ?",
            [ontology_id]
        )
        self.conn.execute(
            "DELETE FROM metadata.ontology WHERE ontology_id = ?",
            [ontology_id]
        )

        logger.info(f"Deleted ontology: {ontology_id}")
        return True

    # ==================== Concept CRUD ====================

    def create_concept(self, concept: OntologyConcept) -> str:
        """
        Create a new concept in an ontology.

        Args:
            concept: Concept to create

        Returns:
            Created concept ID
        """
        import json

        self.conn.execute(
            """
            INSERT INTO metadata.ontology_concept
            (concept_id, ontology_id, label, definition, concept_uri, concept_type,
             superclass_ids, equivalent_class_ids, disjoint_class_ids, deprecated,
             examples, synonyms, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                concept.concept_id,
                concept.ontology_id,
                concept.label,
                concept.definition,
                concept.concept_uri,
                concept.concept_type.value,
                json.dumps(concept.superclass_ids),
                json.dumps(concept.equivalent_class_ids),
                json.dumps(concept.disjoint_class_ids),
                concept.deprecated,
                json.dumps(concept.examples),
                json.dumps(concept.synonyms),
                concept.created_at,
                concept.updated_at,
            ]
        )

        logger.info(f"Created concept: {concept.concept_id}")
        return concept.concept_id

    def get_concept(self, concept_id: str) -> Optional[OntologyConcept]:
        """Get a concept by ID."""
        import json

        row = self.conn.execute(
            "SELECT * FROM metadata.ontology_concept WHERE concept_id = ?",
            [concept_id]
        ).fetchone()

        if not row:
            return None

        return self._row_to_concept(row)

    def _row_to_concept(self, row) -> OntologyConcept:
        """Convert database row to OntologyConcept."""
        import json

        return OntologyConcept(
            concept_id=row[0],
            ontology_id=row[1],
            label=row[2],
            definition=row[3] or "",
            concept_uri=row[4],
            concept_type=ConceptType(row[5]) if row[5] else ConceptType.CLASS,
            superclass_ids=json.loads(row[6]) if row[6] else [],
            equivalent_class_ids=json.loads(row[7]) if row[7] else [],
            disjoint_class_ids=json.loads(row[8]) if row[8] else [],
            deprecated=row[9],
            examples=json.loads(row[10]) if row[10] else [],
            synonyms=json.loads(row[11]) if row[11] else [],
            created_at=row[12],
            updated_at=row[13],
        )

    def list_concepts(self, ontology_id: str) -> List[OntologyConcept]:
        """List all concepts in an ontology."""
        rows = self.conn.execute(
            "SELECT * FROM metadata.ontology_concept WHERE ontology_id = ? ORDER BY label",
            [ontology_id]
        ).fetchall()

        return [self._row_to_concept(row) for row in rows]

    def get_root_concepts(self, ontology_id: str) -> List[OntologyConcept]:
        """Get concepts with no superclasses (root concepts)."""
        rows = self.conn.execute(
            """
            SELECT * FROM metadata.ontology_concept
            WHERE ontology_id = ?
            AND (superclass_ids IS NULL OR superclass_ids = '[]')
            ORDER BY label
            """,
            [ontology_id]
        ).fetchall()

        return [self._row_to_concept(row) for row in rows]

    def get_subclasses(self, concept_id: str) -> List[OntologyConcept]:
        """Get direct subclasses of a concept."""
        import json

        rows = self.conn.execute(
            "SELECT * FROM metadata.ontology_concept ORDER BY label"
        ).fetchall()

        subclasses = []
        for row in rows:
            superclass_ids = json.loads(row[6]) if row[6] else []
            if concept_id in superclass_ids:
                subclasses.append(self._row_to_concept(row))

        return subclasses

    def get_superclasses(self, concept_id: str) -> List[OntologyConcept]:
        """Get direct superclasses of a concept."""
        concept = self.get_concept(concept_id)
        if not concept:
            return []

        superclasses = []
        for superclass_id in concept.superclass_ids:
            superclass = self.get_concept(superclass_id)
            if superclass:
                superclasses.append(superclass)

        return superclasses

    def get_concept_hierarchy(self, ontology_id: str) -> List[ConceptHierarchy]:
        """Build complete concept hierarchy."""
        concepts = self.list_concepts(ontology_id)
        concept_map = {c.concept_id: c for c in concepts}

        hierarchies = []

        def build_hierarchy(concept_id: str, depth: int, path: List[str]) -> ConceptHierarchy:
            concept = concept_map.get(concept_id)
            if not concept:
                return None

            children = []
            for c in concepts:
                if concept_id in c.superclass_ids:
                    children.append(c.concept_id)

            return ConceptHierarchy(
                concept_id=concept_id,
                depth=depth,
                path=path + [concept_id],
                children=children,
                descendant_count=self._count_descendants(concept_id, concept_map, concepts),
            )

        # Start from root concepts
        roots = self.get_root_concepts(ontology_id)
        for root in roots:
            hierarchy = build_hierarchy(root.concept_id, 0, [])
            if hierarchy:
                hierarchies.append(hierarchy)

        return hierarchies

    def _count_descendants(
        self,
        concept_id: str,
        concept_map: Dict[str, OntologyConcept],
        all_concepts: List[OntologyConcept],
    ) -> int:
        """Count all descendants of a concept."""
        count = 0
        for concept in all_concepts:
            if concept_id in concept.superclass_ids:
                count += 1 + self._count_descendants(concept.concept_id, concept_map, all_concepts)
        return count

    # ==================== Property CRUD ====================

    def create_property(self, prop: OntologyProperty) -> str:
        """Create a new property in an ontology."""
        import json

        self.conn.execute(
            """
            INSERT INTO metadata.ontology_property
            (property_id, ontology_id, label, definition, property_uri, property_type,
             domain_concept_ids, range_concept_ids, range_datatype, super_property_ids,
             inverse_property_id, characteristics, deprecated, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                prop.property_id,
                prop.ontology_id,
                prop.label,
                prop.definition,
                prop.property_uri,
                prop.property_type.value,
                json.dumps(prop.domain_concept_ids),
                json.dumps(prop.range_concept_ids),
                prop.range_datatype,
                json.dumps(prop.super_property_ids),
                prop.inverse_property_id,
                json.dumps([c.value for c in prop.characteristics]),
                prop.deprecated,
                prop.created_at,
                prop.updated_at,
            ]
        )

        logger.info(f"Created property: {prop.property_id}")
        return prop.property_id

    def get_property(self, property_id: str) -> Optional[OntologyProperty]:
        """Get a property by ID."""
        import json

        row = self.conn.execute(
            "SELECT * FROM metadata.ontology_property WHERE property_id = ?",
            [property_id]
        ).fetchone()

        if not row:
            return None

        return self._row_to_property(row)

    def _row_to_property(self, row) -> OntologyProperty:
        """Convert database row to OntologyProperty."""
        import json

        characteristics = []
        if row[11]:
            for c in json.loads(row[11]):
                try:
                    characteristics.append(PropertyCharacteristic(c))
                except ValueError:
                    pass

        return OntologyProperty(
            property_id=row[0],
            ontology_id=row[1],
            label=row[2],
            definition=row[3] or "",
            property_uri=row[4],
            property_type=PropertyType(row[5]) if row[5] else PropertyType.OBJECT_PROPERTY,
            domain_concept_ids=json.loads(row[6]) if row[6] else [],
            range_concept_ids=json.loads(row[7]) if row[7] else [],
            range_datatype=row[8],
            super_property_ids=json.loads(row[9]) if row[9] else [],
            inverse_property_id=row[10],
            characteristics=characteristics,
            deprecated=row[12],
            created_at=row[13],
            updated_at=row[14],
        )

    def list_properties(self, ontology_id: str) -> List[OntologyProperty]:
        """List all properties in an ontology."""
        rows = self.conn.execute(
            "SELECT * FROM metadata.ontology_property WHERE ontology_id = ? ORDER BY label",
            [ontology_id]
        ).fetchall()

        return [self._row_to_property(row) for row in rows]

    def get_properties_for_concept(self, concept_id: str) -> List[OntologyProperty]:
        """Get all properties where concept is in domain."""
        import json

        rows = self.conn.execute(
            "SELECT * FROM metadata.ontology_property"
        ).fetchall()

        properties = []
        for row in rows:
            domain_ids = json.loads(row[6]) if row[6] else []
            if concept_id in domain_ids:
                properties.append(self._row_to_property(row))

        return properties

    # ==================== Entity Linking ====================

    def link_entity_to_concept(
        self,
        entity_id: str,
        concept_id: str,
        link_type: LinkType = LinkType.INSTANCE_OF,
        confidence: float = 1.0,
        rationale: str = "",
        created_by: Optional[str] = None,
    ) -> str:
        """
        Link an MDDE entity to an ontology concept.

        Args:
            entity_id: MDDE entity ID
            concept_id: Ontology concept ID
            link_type: Type of link
            confidence: Confidence score (0-1)
            rationale: Explanation for the link
            created_by: User who created the link

        Returns:
            Link ID
        """
        link_id = _generate_id("ecl_")

        self.conn.execute(
            """
            INSERT INTO metadata.entity_concept_link
            (link_id, entity_id, concept_id, link_type, confidence, rationale, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                link_id,
                entity_id,
                concept_id,
                link_type.value,
                confidence,
                rationale,
                created_by,
                _utc_now(),
            ]
        )

        logger.info(f"Linked entity {entity_id} to concept {concept_id}")
        return link_id

    def get_entity_concepts(self, entity_id: str) -> List[Tuple[OntologyConcept, EntityConceptLink]]:
        """Get all concepts linked to an entity."""
        rows = self.conn.execute(
            """
            SELECT l.*, c.*
            FROM metadata.entity_concept_link l
            JOIN metadata.ontology_concept c ON l.concept_id = c.concept_id
            WHERE l.entity_id = ?
            ORDER BY l.confidence DESC
            """,
            [entity_id]
        ).fetchall()

        results = []
        for row in rows:
            link = EntityConceptLink(
                link_id=row[0],
                entity_id=row[1],
                concept_id=row[2],
                link_type=LinkType(row[3]) if row[3] else LinkType.INSTANCE_OF,
                confidence=row[4] or 1.0,
                rationale=row[5] or "",
                created_by=row[6],
                created_at=row[7],
            )
            concept = self._row_to_concept(row[8:])
            results.append((concept, link))

        return results

    def get_entities_for_concept(self, concept_id: str) -> List[Tuple[str, EntityConceptLink]]:
        """Get all entities linked to a concept."""
        rows = self.conn.execute(
            """
            SELECT *
            FROM metadata.entity_concept_link
            WHERE concept_id = ?
            ORDER BY confidence DESC
            """,
            [concept_id]
        ).fetchall()

        results = []
        for row in rows:
            link = EntityConceptLink(
                link_id=row[0],
                entity_id=row[1],
                concept_id=row[2],
                link_type=LinkType(row[3]) if row[3] else LinkType.INSTANCE_OF,
                confidence=row[4] or 1.0,
                rationale=row[5] or "",
                created_by=row[6],
                created_at=row[7],
            )
            results.append((row[1], link))  # (entity_id, link)

        return results

    def unlink_entity_from_concept(self, entity_id: str, concept_id: str) -> bool:
        """Remove link between entity and concept."""
        self.conn.execute(
            "DELETE FROM metadata.entity_concept_link WHERE entity_id = ? AND concept_id = ?",
            [entity_id, concept_id]
        )
        return True

    # ==================== Attribute Semantic Linking ====================

    def add_attribute_semantic(
        self,
        attribute_id: str,
        concept_id: Optional[str] = None,
        property_id: Optional[str] = None,
        semantic_role: str = "",
        relationship_context: str = "",
        canonical_identifier: Optional[str] = None,
    ) -> str:
        """
        Add semantic annotation to an attribute.

        Args:
            attribute_id: MDDE attribute ID
            concept_id: Linked concept
            property_id: Linked property
            semantic_role: Role like "identifier", "measure", "dimension"
            relationship_context: Context like "hasShippingAddress"
            canonical_identifier: Standard identifier like "iso_3166_1_alpha_2"

        Returns:
            Link ID
        """
        link_id = _generate_id("asl_")

        self.conn.execute(
            """
            INSERT INTO metadata.attribute_semantic_link
            (link_id, attribute_id, concept_id, property_id, semantic_role,
             relationship_context, canonical_identifier, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                link_id,
                attribute_id,
                concept_id,
                property_id,
                semantic_role,
                relationship_context,
                canonical_identifier,
                _utc_now(),
            ]
        )

        return link_id

    def get_attribute_semantics(self, attribute_id: str) -> List[AttributeSemanticLink]:
        """Get semantic annotations for an attribute."""
        rows = self.conn.execute(
            "SELECT * FROM metadata.attribute_semantic_link WHERE attribute_id = ?",
            [attribute_id]
        ).fetchall()

        return [
            AttributeSemanticLink(
                link_id=row[0],
                attribute_id=row[1],
                concept_id=row[2],
                property_id=row[3],
                semantic_role=row[4] or "",
                relationship_context=row[5] or "",
                canonical_identifier=row[6],
                created_at=row[7],
            )
            for row in rows
        ]

    # ==================== Reasoning / Inference ====================

    def get_all_superclasses(self, concept_id: str) -> List[OntologyConcept]:
        """Get all superclasses (transitive closure)."""
        visited = set()
        result = []

        def traverse(cid: str):
            if cid in visited:
                return
            visited.add(cid)

            concept = self.get_concept(cid)
            if not concept:
                return

            for superclass_id in concept.superclass_ids:
                superclass = self.get_concept(superclass_id)
                if superclass:
                    result.append(superclass)
                    traverse(superclass_id)

        traverse(concept_id)
        return result

    def get_all_subclasses(self, concept_id: str) -> List[OntologyConcept]:
        """Get all subclasses (transitive closure)."""
        visited = set()
        result = []

        def traverse(cid: str):
            if cid in visited:
                return
            visited.add(cid)

            subclasses = self.get_subclasses(cid)
            for subclass in subclasses:
                result.append(subclass)
                traverse(subclass.concept_id)

        traverse(concept_id)
        return result

    def is_subclass_of(self, concept_id: str, potential_superclass_id: str) -> bool:
        """Check if one concept is a subclass of another."""
        superclasses = self.get_all_superclasses(concept_id)
        return any(s.concept_id == potential_superclass_id for s in superclasses)

    def find_common_superclass(self, concept_ids: List[str]) -> Optional[OntologyConcept]:
        """Find the most specific common superclass of concepts."""
        if not concept_ids:
            return None

        # Get all superclasses for each concept
        superclass_sets = []
        for cid in concept_ids:
            superclasses = self.get_all_superclasses(cid)
            superclass_sets.append(set(s.concept_id for s in superclasses))

        # Find intersection
        common = superclass_sets[0]
        for s in superclass_sets[1:]:
            common = common.intersection(s)

        if not common:
            return None

        # Find most specific (deepest in hierarchy)
        best = None
        best_depth = -1

        for cid in common:
            depth = len(self.get_all_superclasses(cid))
            if depth > best_depth:
                best_depth = depth
                best = self.get_concept(cid)

        return best

    # ==================== SKOS Support (v3.23.0) ====================

    def _ensure_skos_tables(self):
        """Ensure SKOS tables exist."""
        try:
            self.conn.execute("SELECT 1 FROM metadata.skos_scheme LIMIT 1")
        except Exception:
            self._create_skos_tables()

    def _create_skos_tables(self):
        """Create SKOS-specific tables."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.skos_scheme (
                scheme_id VARCHAR PRIMARY KEY,
                title VARCHAR NOT NULL,
                description VARCHAR,
                namespace VARCHAR,
                creator VARCHAR,
                publisher VARCHAR,
                version VARCHAR DEFAULT '1.0',
                top_concepts VARCHAR,  -- JSON array
                created TIMESTAMP,
                modified TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.skos_concept (
                concept_id VARCHAR PRIMARY KEY,
                scheme_id VARCHAR NOT NULL,
                notation VARCHAR,
                labels VARCHAR,  -- JSON array of SKOSLabel
                definition VARCHAR,
                scope_note VARCHAR,
                example VARCHAR,
                history_note VARCHAR,
                editorial_note VARCHAR,
                broader VARCHAR,  -- JSON array
                narrower VARCHAR,  -- JSON array
                related VARCHAR,  -- JSON array
                close_match VARCHAR,  -- JSON array
                exact_match VARCHAR,  -- JSON array
                created TIMESTAMP,
                modified TIMESTAMP,
                FOREIGN KEY (scheme_id) REFERENCES metadata.skos_scheme(scheme_id)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.ontology_mapping (
                mapping_id VARCHAR PRIMARY KEY,
                source_ontology_id VARCHAR NOT NULL,
                source_concept_id VARCHAR NOT NULL,
                target_ontology_id VARCHAR NOT NULL,
                target_concept_id VARCHAR NOT NULL,
                mapping_type VARCHAR,
                confidence DECIMAL(3,2) DEFAULT 1.0,
                rationale VARCHAR,
                created_at TIMESTAMP
            )
        """)

        logger.info("SKOS tables created")

    def create_concept_scheme(self, scheme: SKOSConceptScheme) -> str:
        """Create a new SKOS Concept Scheme."""
        import json

        self._ensure_skos_tables()

        self.conn.execute(
            """
            INSERT INTO metadata.skos_scheme
            (scheme_id, title, description, namespace, creator, publisher, version,
             top_concepts, created, modified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                scheme.scheme_id,
                scheme.title,
                scheme.description,
                scheme.namespace,
                scheme.creator,
                scheme.publisher,
                scheme.version,
                json.dumps(scheme.top_concepts),
                scheme.created,
                scheme.modified,
            ]
        )

        logger.info(f"Created SKOS scheme: {scheme.scheme_id}")
        return scheme.scheme_id

    def get_concept_scheme(self, scheme_id: str) -> Optional[SKOSConceptScheme]:
        """Get a SKOS Concept Scheme by ID."""
        import json

        self._ensure_skos_tables()

        row = self.conn.execute(
            "SELECT * FROM metadata.skos_scheme WHERE scheme_id = ?",
            [scheme_id]
        ).fetchone()

        if not row:
            return None

        return SKOSConceptScheme(
            scheme_id=row[0],
            title=row[1],
            description=row[2] or "",
            namespace=row[3] or "",
            creator=row[4] or "",
            publisher=row[5] or "",
            version=row[6] or "1.0",
            top_concepts=json.loads(row[7]) if row[7] else [],
            created=row[8],
            modified=row[9],
        )

    def create_skos_concept(self, concept: SKOSConcept) -> str:
        """Create a new SKOS Concept."""
        import json

        self._ensure_skos_tables()

        labels_json = json.dumps([
            {"type": l.label_type.value, "value": l.value, "language": l.language}
            for l in concept.labels
        ])

        self.conn.execute(
            """
            INSERT INTO metadata.skos_concept
            (concept_id, scheme_id, notation, labels, definition, scope_note,
             example, history_note, editorial_note, broader, narrower, related,
             close_match, exact_match, created, modified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                concept.concept_id,
                concept.scheme_id,
                concept.notation,
                labels_json,
                concept.definition,
                concept.scope_note,
                concept.example,
                concept.history_note,
                concept.editorial_note,
                json.dumps(concept.broader),
                json.dumps(concept.narrower),
                json.dumps(concept.related),
                json.dumps(concept.close_match),
                json.dumps(concept.exact_match),
                concept.created,
                concept.modified,
            ]
        )

        logger.info(f"Created SKOS concept: {concept.concept_id}")
        return concept.concept_id

    def get_skos_concept(self, concept_id: str) -> Optional[SKOSConcept]:
        """Get a SKOS Concept by ID."""
        import json

        self._ensure_skos_tables()

        row = self.conn.execute(
            "SELECT * FROM metadata.skos_concept WHERE concept_id = ?",
            [concept_id]
        ).fetchone()

        if not row:
            return None

        return self._row_to_skos_concept(row)

    def _row_to_skos_concept(self, row) -> SKOSConcept:
        """Convert database row to SKOSConcept."""
        import json

        labels = []
        if row[3]:
            for l in json.loads(row[3]):
                labels.append(SKOSLabel(
                    label_type=SKOSLabelType(l["type"]),
                    value=l["value"],
                    language=l.get("language", "en"),
                ))

        return SKOSConcept(
            concept_id=row[0],
            scheme_id=row[1],
            notation=row[2],
            labels=labels,
            definition=row[4] or "",
            scope_note=row[5] or "",
            example=row[6] or "",
            history_note=row[7] or "",
            editorial_note=row[8] or "",
            broader=json.loads(row[9]) if row[9] else [],
            narrower=json.loads(row[10]) if row[10] else [],
            related=json.loads(row[11]) if row[11] else [],
            close_match=json.loads(row[12]) if row[12] else [],
            exact_match=json.loads(row[13]) if row[13] else [],
            created=row[14],
            modified=row[15],
        )

    def list_skos_concepts(self, scheme_id: str) -> List[SKOSConcept]:
        """List all SKOS concepts in a scheme."""
        self._ensure_skos_tables()

        rows = self.conn.execute(
            "SELECT * FROM metadata.skos_concept WHERE scheme_id = ? ORDER BY notation, concept_id",
            [scheme_id]
        ).fetchall()

        return [self._row_to_skos_concept(row) for row in rows]

    def search_skos_concepts(
        self,
        query: str,
        scheme_id: Optional[str] = None,
        search_labels: bool = True,
        search_definitions: bool = True,
    ) -> List[SKOSConcept]:
        """
        Search SKOS concepts by text.

        Args:
            query: Search text
            scheme_id: Limit to specific scheme (optional)
            search_labels: Search in labels
            search_definitions: Search in definitions

        Returns:
            List of matching concepts
        """
        self._ensure_skos_tables()

        query_lower = query.lower()
        results = []

        if scheme_id:
            rows = self.conn.execute(
                "SELECT * FROM metadata.skos_concept WHERE scheme_id = ?",
                [scheme_id]
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM metadata.skos_concept"
            ).fetchall()

        for row in rows:
            concept = self._row_to_skos_concept(row)
            match = False

            if search_labels:
                for label in concept.labels:
                    if query_lower in label.value.lower():
                        match = True
                        break

            if search_definitions and not match:
                if query_lower in concept.definition.lower():
                    match = True

            if match:
                results.append(concept)

        return results

    # ==================== Ontology Export (v3.23.0) ====================

    def export_ontology_owl(self, ontology_id: str, include_individuals: bool = False) -> str:
        """
        Export ontology to OWL/XML format.

        Args:
            ontology_id: Ontology to export
            include_individuals: Whether to include entity links as individuals

        Returns:
            OWL/XML string
        """
        ontology = self.get_ontology(ontology_id)
        if not ontology:
            raise ValueError(f"Ontology not found: {ontology_id}")

        concepts = self.list_concepts(ontology_id)
        properties = self.list_properties(ontology_id)

        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<rdf:RDF',
            f'    xmlns="{ontology.namespace}#"',
            f'    xml:base="{ontology.namespace}"',
            '    xmlns:owl="http://www.w3.org/2002/07/owl#"',
            '    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"',
            '    xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"',
            '    xmlns:xsd="http://www.w3.org/2001/XMLSchema#">',
            '',
            f'    <owl:Ontology rdf:about="{ontology.namespace}">',
            f'        <rdfs:label>{ontology.name}</rdfs:label>',
        ]

        if ontology.description:
            lines.append(f'        <rdfs:comment>{_escape_xml(ontology.description)}</rdfs:comment>')

        lines.append(f'        <owl:versionInfo>{ontology.version}</owl:versionInfo>')
        lines.append('    </owl:Ontology>')
        lines.append('')

        # Export classes
        for concept in concepts:
            lines.append(f'    <owl:Class rdf:about="#{concept.local_name}">')
            lines.append(f'        <rdfs:label>{_escape_xml(concept.label)}</rdfs:label>')

            if concept.definition:
                lines.append(f'        <rdfs:comment>{_escape_xml(concept.definition)}</rdfs:comment>')

            for superclass_id in concept.superclass_ids:
                superclass = self.get_concept(superclass_id)
                if superclass:
                    lines.append(f'        <rdfs:subClassOf rdf:resource="#{superclass.local_name}"/>')

            if concept.deprecated:
                lines.append('        <owl:deprecated rdf:datatype="xsd:boolean">true</owl:deprecated>')

            lines.append('    </owl:Class>')
            lines.append('')

        # Export object properties
        for prop in properties:
            if prop.property_type == PropertyType.OBJECT_PROPERTY:
                lines.append(f'    <owl:ObjectProperty rdf:about="#{prop.label}">')
            elif prop.property_type == PropertyType.DATA_PROPERTY:
                lines.append(f'    <owl:DatatypeProperty rdf:about="#{prop.label}">')
            else:
                lines.append(f'    <owl:AnnotationProperty rdf:about="#{prop.label}">')

            lines.append(f'        <rdfs:label>{_escape_xml(prop.label)}</rdfs:label>')

            if prop.definition:
                lines.append(f'        <rdfs:comment>{_escape_xml(prop.definition)}</rdfs:comment>')

            for domain_id in prop.domain_concept_ids:
                domain = self.get_concept(domain_id)
                if domain:
                    lines.append(f'        <rdfs:domain rdf:resource="#{domain.local_name}"/>')

            for range_id in prop.range_concept_ids:
                range_concept = self.get_concept(range_id)
                if range_concept:
                    lines.append(f'        <rdfs:range rdf:resource="#{range_concept.local_name}"/>')

            if prop.range_datatype:
                lines.append(f'        <rdfs:range rdf:resource="{prop.range_datatype}"/>')

            # Property characteristics
            if PropertyCharacteristic.FUNCTIONAL in prop.characteristics:
                lines.append('        <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#FunctionalProperty"/>')
            if PropertyCharacteristic.TRANSITIVE in prop.characteristics:
                lines.append('        <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#TransitiveProperty"/>')
            if PropertyCharacteristic.SYMMETRIC in prop.characteristics:
                lines.append('        <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#SymmetricProperty"/>')

            if prop.property_type == PropertyType.OBJECT_PROPERTY:
                lines.append('    </owl:ObjectProperty>')
            elif prop.property_type == PropertyType.DATA_PROPERTY:
                lines.append('    </owl:DatatypeProperty>')
            else:
                lines.append('    </owl:AnnotationProperty>')
            lines.append('')

        # Export individuals (entity links) if requested
        if include_individuals:
            for concept in concepts:
                entity_links = self.get_entities_for_concept(concept.concept_id)
                for entity_id, link in entity_links:
                    lines.append(f'    <owl:NamedIndividual rdf:about="#{entity_id}">')
                    lines.append(f'        <rdf:type rdf:resource="#{concept.local_name}"/>')
                    lines.append('    </owl:NamedIndividual>')
                    lines.append('')

        lines.append('</rdf:RDF>')

        return '\n'.join(lines)

    def export_ontology_turtle(self, ontology_id: str) -> str:
        """
        Export ontology to Turtle format.

        Args:
            ontology_id: Ontology to export

        Returns:
            Turtle format string
        """
        ontology = self.get_ontology(ontology_id)
        if not ontology:
            raise ValueError(f"Ontology not found: {ontology_id}")

        concepts = self.list_concepts(ontology_id)
        properties = self.list_properties(ontology_id)

        lines = [
            '@prefix owl: <http://www.w3.org/2002/07/owl#> .',
            '@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .',
            '@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .',
            '@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .',
            f'@prefix : <{ontology.namespace}#> .',
            f'@base <{ontology.namespace}> .',
            '',
            f'<{ontology.namespace}> rdf:type owl:Ontology ;',
            f'    rdfs:label "{ontology.name}" ;',
            f'    owl:versionInfo "{ontology.version}" .',
            '',
        ]

        # Export classes
        for concept in concepts:
            lines.append(f':{concept.local_name} rdf:type owl:Class ;')
            lines.append(f'    rdfs:label "{concept.label}" ;')

            if concept.definition:
                lines.append(f'    rdfs:comment "{_escape_turtle(concept.definition)}" ;')

            for i, superclass_id in enumerate(concept.superclass_ids):
                superclass = self.get_concept(superclass_id)
                if superclass:
                    lines.append(f'    rdfs:subClassOf :{superclass.local_name} ;')

            # Remove trailing semicolon and add period
            if lines[-1].endswith(' ;'):
                lines[-1] = lines[-1][:-2] + ' .'

            lines.append('')

        # Export properties
        for prop in properties:
            prop_type = "owl:ObjectProperty" if prop.property_type == PropertyType.OBJECT_PROPERTY else "owl:DatatypeProperty"
            lines.append(f':{prop.label} rdf:type {prop_type} ;')
            lines.append(f'    rdfs:label "{prop.label}" ;')

            for domain_id in prop.domain_concept_ids:
                domain = self.get_concept(domain_id)
                if domain:
                    lines.append(f'    rdfs:domain :{domain.local_name} ;')

            if lines[-1].endswith(' ;'):
                lines[-1] = lines[-1][:-2] + ' .'

            lines.append('')

        return '\n'.join(lines)

    def export_skos_scheme(self, scheme_id: str) -> str:
        """
        Export SKOS Concept Scheme to Turtle format.

        Args:
            scheme_id: Scheme to export

        Returns:
            SKOS Turtle string
        """
        scheme = self.get_concept_scheme(scheme_id)
        if not scheme:
            raise ValueError(f"Scheme not found: {scheme_id}")

        concepts = self.list_skos_concepts(scheme_id)

        lines = [
            '@prefix skos: <http://www.w3.org/2004/02/skos/core#> .',
            '@prefix dct: <http://purl.org/dc/terms/> .',
            '@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .',
            f'@prefix : <{scheme.namespace}#> .' if scheme.namespace else '@prefix : <urn:scheme:> .',
            '',
            f':{scheme.scheme_id} a skos:ConceptScheme ;',
            f'    dct:title "{scheme.title}" ;',
        ]

        if scheme.description:
            lines.append(f'    dct:description "{_escape_turtle(scheme.description)}" ;')
        if scheme.creator:
            lines.append(f'    dct:creator "{scheme.creator}" ;')
        if scheme.version:
            lines.append(f'    owl:versionInfo "{scheme.version}" ;')

        for top_id in scheme.top_concepts:
            lines.append(f'    skos:hasTopConcept :{top_id} ;')

        if lines[-1].endswith(' ;'):
            lines[-1] = lines[-1][:-2] + ' .'

        lines.append('')

        # Export concepts
        for concept in concepts:
            lines.append(f':{concept.concept_id} a skos:Concept ;')
            lines.append(f'    skos:inScheme :{scheme.scheme_id} ;')

            if concept.notation:
                lines.append(f'    skos:notation "{concept.notation}" ;')

            for label in concept.labels:
                if label.label_type == SKOSLabelType.PREF_LABEL:
                    lines.append(f'    skos:prefLabel "{label.value}"@{label.language} ;')
                elif label.label_type == SKOSLabelType.ALT_LABEL:
                    lines.append(f'    skos:altLabel "{label.value}"@{label.language} ;')

            if concept.definition:
                lines.append(f'    skos:definition "{_escape_turtle(concept.definition)}" ;')

            if concept.scope_note:
                lines.append(f'    skos:scopeNote "{_escape_turtle(concept.scope_note)}" ;')

            for broader_id in concept.broader:
                lines.append(f'    skos:broader :{broader_id} ;')

            for narrower_id in concept.narrower:
                lines.append(f'    skos:narrower :{narrower_id} ;')

            for related_id in concept.related:
                lines.append(f'    skos:related :{related_id} ;')

            for match_uri in concept.exact_match:
                lines.append(f'    skos:exactMatch <{match_uri}> ;')

            for match_uri in concept.close_match:
                lines.append(f'    skos:closeMatch <{match_uri}> ;')

            if lines[-1].endswith(' ;'):
                lines[-1] = lines[-1][:-2] + ' .'

            lines.append('')

        return '\n'.join(lines)

    # ==================== Semantic Similarity (v3.23.0) ====================

    def calculate_path_similarity(
        self,
        concept_a_id: str,
        concept_b_id: str,
    ) -> SemanticSimilarity:
        """
        Calculate path-based similarity between concepts.

        Uses shortest path through hierarchy.
        Similarity = 1 / (1 + path_length)

        Args:
            concept_a_id: First concept
            concept_b_id: Second concept

        Returns:
            SemanticSimilarity result
        """
        if concept_a_id == concept_b_id:
            return SemanticSimilarity(
                concept_a_id=concept_a_id,
                concept_b_id=concept_b_id,
                similarity_score=1.0,
                method="path",
                common_ancestor_id=concept_a_id,
            )

        # Find common ancestor
        ancestors_a = set(c.concept_id for c in self.get_all_superclasses(concept_a_id))
        ancestors_a.add(concept_a_id)

        ancestors_b = set(c.concept_id for c in self.get_all_superclasses(concept_b_id))
        ancestors_b.add(concept_b_id)

        common = ancestors_a.intersection(ancestors_b)

        if not common:
            return SemanticSimilarity(
                concept_a_id=concept_a_id,
                concept_b_id=concept_b_id,
                similarity_score=0.0,
                method="path",
            )

        # Find lowest common ancestor
        lca = None
        lca_depth = -1
        for cid in common:
            depth = len(self.get_all_superclasses(cid))
            if depth > lca_depth:
                lca_depth = depth
                lca = cid

        # Calculate path length
        depth_a = self._depth_to_ancestor(concept_a_id, lca)
        depth_b = self._depth_to_ancestor(concept_b_id, lca)
        path_length = depth_a + depth_b

        similarity = 1.0 / (1.0 + path_length)

        return SemanticSimilarity(
            concept_a_id=concept_a_id,
            concept_b_id=concept_b_id,
            similarity_score=similarity,
            method="path",
            common_ancestor_id=lca,
        )

    def _depth_to_ancestor(self, concept_id: str, ancestor_id: str) -> int:
        """Calculate depth from concept to ancestor."""
        if concept_id == ancestor_id:
            return 0

        depth = 0
        current = concept_id
        visited = set()

        while current and current not in visited:
            visited.add(current)
            concept = self.get_concept(current)
            if not concept:
                break

            depth += 1
            if ancestor_id in concept.superclass_ids:
                return depth

            # Move to first superclass
            if concept.superclass_ids:
                current = concept.superclass_ids[0]
            else:
                break

        return depth

    def calculate_wu_palmer_similarity(
        self,
        concept_a_id: str,
        concept_b_id: str,
    ) -> SemanticSimilarity:
        """
        Calculate Wu-Palmer similarity between concepts.

        sim(c1, c2) = 2 * depth(lca) / (depth(c1) + depth(c2))

        Args:
            concept_a_id: First concept
            concept_b_id: Second concept

        Returns:
            SemanticSimilarity result
        """
        if concept_a_id == concept_b_id:
            return SemanticSimilarity(
                concept_a_id=concept_a_id,
                concept_b_id=concept_b_id,
                similarity_score=1.0,
                method="wu_palmer",
                common_ancestor_id=concept_a_id,
            )

        # Find depths
        depth_a = len(self.get_all_superclasses(concept_a_id)) + 1
        depth_b = len(self.get_all_superclasses(concept_b_id)) + 1

        # Find LCA
        ancestors_a = set(c.concept_id for c in self.get_all_superclasses(concept_a_id))
        ancestors_a.add(concept_a_id)

        ancestors_b = set(c.concept_id for c in self.get_all_superclasses(concept_b_id))
        ancestors_b.add(concept_b_id)

        common = ancestors_a.intersection(ancestors_b)

        if not common:
            return SemanticSimilarity(
                concept_a_id=concept_a_id,
                concept_b_id=concept_b_id,
                similarity_score=0.0,
                method="wu_palmer",
            )

        # Find LCA with greatest depth
        lca = None
        lca_depth = -1
        for cid in common:
            depth = len(self.get_all_superclasses(cid)) + 1
            if depth > lca_depth:
                lca_depth = depth
                lca = cid

        # Wu-Palmer formula
        similarity = (2.0 * lca_depth) / (depth_a + depth_b)

        return SemanticSimilarity(
            concept_a_id=concept_a_id,
            concept_b_id=concept_b_id,
            similarity_score=similarity,
            method="wu_palmer",
            common_ancestor_id=lca,
        )

    def find_similar_concepts(
        self,
        concept_id: str,
        top_k: int = 5,
        method: str = "wu_palmer",
    ) -> List[SemanticSimilarity]:
        """
        Find most similar concepts to a given concept.

        Args:
            concept_id: Source concept
            top_k: Number of results
            method: Similarity method ("path" or "wu_palmer")

        Returns:
            List of similarities sorted by score
        """
        concept = self.get_concept(concept_id)
        if not concept:
            return []

        all_concepts = self.list_concepts(concept.ontology_id)
        similarities = []

        for other in all_concepts:
            if other.concept_id == concept_id:
                continue

            if method == "path":
                sim = self.calculate_path_similarity(concept_id, other.concept_id)
            else:
                sim = self.calculate_wu_palmer_similarity(concept_id, other.concept_id)

            similarities.append(sim)

        # Sort by score descending
        similarities.sort(key=lambda s: s.similarity_score, reverse=True)

        return similarities[:top_k]


def _escape_xml(text: str) -> str:
    """Escape XML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _escape_turtle(text: str) -> str:
    """Escape Turtle string special characters."""
    return (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
