# MDDE Aboutness Manager
# ADR-247: Aboutness Layer
# Feb 2026

"""
Manager for semantic aboutness operations.

The AboutnessManager provides CRUD operations and queries for
entity and attribute aboutness metadata.
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from .models import (
    AboutnessDimension,
    AttributeAboutness,
    DependencyType,
    EntityAboutness,
    SemanticDependency,
    SemanticRole,
)

logger = logging.getLogger(__name__)


class AboutnessManager:
    """
    Manage semantic aboutness for entities and attributes.

    Example:
        manager = AboutnessManager(conn)

        # Set entity aboutness
        manager.set_entity_aboutness(EntityAboutness(
            entity_id="customer",
            purpose="Represents individuals who purchase products",
            real_world_object="Person",
            aboutness_dimension=AboutnessDimension.CLASSIFIER,
        ))

        # Query by dimension
        measures = manager.find_by_dimension(AboutnessDimension.MEASURE)
    """

    def __init__(self, conn, model_id: Optional[str] = None):
        """
        Initialize AboutnessManager.

        Args:
            conn: Database connection.
            model_id: Optional default model ID for operations.
        """
        self.conn = conn
        self.model_id = model_id

    # -------------------------------------------------------------------------
    # Entity Aboutness CRUD
    # -------------------------------------------------------------------------

    def set_entity_aboutness(self, aboutness: EntityAboutness) -> str:
        """
        Set or update aboutness for an entity.

        Args:
            aboutness: EntityAboutness to set.

        Returns:
            Aboutness ID.
        """
        aboutness_id = aboutness.aboutness_id or f"ea_{uuid.uuid4().hex[:12]}"
        model_id = aboutness.model_id or self.model_id

        self.conn.execute(
            """
            INSERT INTO metadata.entity_aboutness (
                aboutness_id, entity_id, model_id, purpose, business_context,
                real_world_object, aboutness_dimension, semantic_category,
                business_use_cases, stakeholder_groups, represents_concept,
                equivalent_to, confidence_score, source, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (entity_id, model_id) DO UPDATE SET
                purpose = EXCLUDED.purpose,
                business_context = EXCLUDED.business_context,
                real_world_object = EXCLUDED.real_world_object,
                aboutness_dimension = EXCLUDED.aboutness_dimension,
                semantic_category = EXCLUDED.semantic_category,
                business_use_cases = EXCLUDED.business_use_cases,
                stakeholder_groups = EXCLUDED.stakeholder_groups,
                represents_concept = EXCLUDED.represents_concept,
                equivalent_to = EXCLUDED.equivalent_to,
                confidence_score = EXCLUDED.confidence_score,
                source = EXCLUDED.source
            """,
            [
                aboutness_id,
                aboutness.entity_id,
                model_id,
                aboutness.purpose,
                aboutness.business_context,
                aboutness.real_world_object,
                aboutness.aboutness_dimension.value,
                aboutness.semantic_category,
                json.dumps(aboutness.business_use_cases),
                json.dumps(aboutness.stakeholder_groups),
                aboutness.represents_concept,
                json.dumps(aboutness.equivalent_to),
                aboutness.confidence_score,
                aboutness.source,
            ],
        )

        logger.info(f"Set entity aboutness for {aboutness.entity_id}")
        return aboutness_id

    def get_entity_aboutness(
        self, entity_id: str, model_id: Optional[str] = None
    ) -> Optional[EntityAboutness]:
        """
        Get aboutness for an entity.

        Args:
            entity_id: Entity ID.
            model_id: Optional model ID.

        Returns:
            EntityAboutness or None.
        """
        model_id = model_id or self.model_id

        row = self.conn.execute(
            """
            SELECT aboutness_id, entity_id, model_id, purpose, business_context,
                   real_world_object, aboutness_dimension, semantic_category,
                   business_use_cases, stakeholder_groups, represents_concept,
                   equivalent_to, confidence_score, source, created_at, created_by
            FROM metadata.entity_aboutness
            WHERE entity_id = ? AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, model_id],
        ).fetchone()

        if not row:
            return None

        return EntityAboutness(
            aboutness_id=row[0],
            entity_id=row[1],
            model_id=row[2],
            purpose=row[3],
            business_context=row[4],
            real_world_object=row[5],
            aboutness_dimension=AboutnessDimension(row[6]),
            semantic_category=row[7],
            business_use_cases=json.loads(row[8]) if row[8] else [],
            stakeholder_groups=json.loads(row[9]) if row[9] else [],
            represents_concept=row[10],
            equivalent_to=json.loads(row[11]) if row[11] else [],
            confidence_score=row[12] or 1.0,
            source=row[13] or "manual",
            created_at=row[14],
            created_by=row[15],
        )

    def delete_entity_aboutness(
        self, entity_id: str, model_id: Optional[str] = None
    ) -> bool:
        """
        Delete aboutness for an entity.

        Args:
            entity_id: Entity ID.
            model_id: Optional model ID.

        Returns:
            True if deleted.
        """
        model_id = model_id or self.model_id

        self.conn.execute(
            """
            DELETE FROM metadata.entity_aboutness
            WHERE entity_id = ? AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, model_id],
        )

        return True

    # -------------------------------------------------------------------------
    # Attribute Aboutness CRUD
    # -------------------------------------------------------------------------

    def set_attribute_aboutness(self, aboutness: AttributeAboutness) -> str:
        """
        Set or update aboutness for an attribute.

        Args:
            aboutness: AttributeAboutness to set.

        Returns:
            Aboutness ID.
        """
        aboutness_id = aboutness.aboutness_id or f"aa_{uuid.uuid4().hex[:12]}"
        model_id = aboutness.model_id or self.model_id

        self.conn.execute(
            """
            INSERT INTO metadata.attribute_aboutness (
                aboutness_id, entity_id, attribute_id, model_id, intent,
                aboutness_dimension, semantic_role, measures_what, identifies_what,
                classifies_what, relates_to, represents_property, canonical_name,
                expected_behavior, derived_from, semantic_transform,
                confidence_score, source, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (entity_id, attribute_id, model_id) DO UPDATE SET
                intent = EXCLUDED.intent,
                aboutness_dimension = EXCLUDED.aboutness_dimension,
                semantic_role = EXCLUDED.semantic_role,
                measures_what = EXCLUDED.measures_what,
                identifies_what = EXCLUDED.identifies_what,
                classifies_what = EXCLUDED.classifies_what,
                relates_to = EXCLUDED.relates_to,
                represents_property = EXCLUDED.represents_property,
                canonical_name = EXCLUDED.canonical_name,
                expected_behavior = EXCLUDED.expected_behavior,
                derived_from = EXCLUDED.derived_from,
                semantic_transform = EXCLUDED.semantic_transform,
                confidence_score = EXCLUDED.confidence_score,
                source = EXCLUDED.source
            """,
            [
                aboutness_id,
                aboutness.entity_id,
                aboutness.attribute_id,
                model_id,
                aboutness.intent,
                aboutness.aboutness_dimension.value,
                aboutness.semantic_role.value,
                aboutness.measures_what,
                aboutness.identifies_what,
                aboutness.classifies_what,
                aboutness.relates_to,
                aboutness.represents_property,
                aboutness.canonical_name,
                json.dumps(aboutness.expected_behavior),
                aboutness.derived_from,
                aboutness.semantic_transform,
                aboutness.confidence_score,
                aboutness.source,
            ],
        )

        logger.info(
            f"Set attribute aboutness for {aboutness.entity_id}.{aboutness.attribute_id}"
        )
        return aboutness_id

    def get_attribute_aboutness(
        self,
        entity_id: str,
        attribute_id: str,
        model_id: Optional[str] = None,
    ) -> Optional[AttributeAboutness]:
        """
        Get aboutness for an attribute.

        Args:
            entity_id: Entity ID.
            attribute_id: Attribute ID.
            model_id: Optional model ID.

        Returns:
            AttributeAboutness or None.
        """
        model_id = model_id or self.model_id

        row = self.conn.execute(
            """
            SELECT aboutness_id, entity_id, attribute_id, model_id, intent,
                   aboutness_dimension, semantic_role, measures_what, identifies_what,
                   classifies_what, relates_to, represents_property, canonical_name,
                   expected_behavior, derived_from, semantic_transform,
                   confidence_score, source, created_at
            FROM metadata.attribute_aboutness
            WHERE entity_id = ? AND attribute_id = ?
              AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, attribute_id, model_id],
        ).fetchone()

        if not row:
            return None

        return AttributeAboutness(
            aboutness_id=row[0],
            entity_id=row[1],
            attribute_id=row[2],
            model_id=row[3],
            intent=row[4],
            aboutness_dimension=AboutnessDimension(row[5]),
            semantic_role=SemanticRole(row[6]),
            measures_what=row[7],
            identifies_what=row[8],
            classifies_what=row[9],
            relates_to=row[10],
            represents_property=row[11],
            canonical_name=row[12],
            expected_behavior=json.loads(row[13]) if row[13] else {},
            derived_from=row[14],
            semantic_transform=row[15],
            confidence_score=row[16] or 1.0,
            source=row[17] or "manual",
            created_at=row[18],
        )

    def get_all_attribute_aboutness(
        self, entity_id: str, model_id: Optional[str] = None
    ) -> List[AttributeAboutness]:
        """
        Get all attribute aboutness for an entity.

        Args:
            entity_id: Entity ID.
            model_id: Optional model ID.

        Returns:
            List of AttributeAboutness.
        """
        model_id = model_id or self.model_id

        rows = self.conn.execute(
            """
            SELECT aboutness_id, entity_id, attribute_id, model_id, intent,
                   aboutness_dimension, semantic_role, measures_what, identifies_what,
                   classifies_what, relates_to, represents_property, canonical_name,
                   expected_behavior, derived_from, semantic_transform,
                   confidence_score, source, created_at
            FROM metadata.attribute_aboutness
            WHERE entity_id = ? AND (model_id = ? OR model_id IS NULL)
            ORDER BY attribute_id
            """,
            [entity_id, model_id],
        ).fetchall()

        return [
            AttributeAboutness(
                aboutness_id=r[0],
                entity_id=r[1],
                attribute_id=r[2],
                model_id=r[3],
                intent=r[4],
                aboutness_dimension=AboutnessDimension(r[5]),
                semantic_role=SemanticRole(r[6]),
                measures_what=r[7],
                identifies_what=r[8],
                classifies_what=r[9],
                relates_to=r[10],
                represents_property=r[11],
                canonical_name=r[12],
                expected_behavior=json.loads(r[13]) if r[13] else {},
                derived_from=r[14],
                semantic_transform=r[15],
                confidence_score=r[16] or 1.0,
                source=r[17] or "manual",
                created_at=r[18],
            )
            for r in rows
        ]

    def delete_attribute_aboutness(
        self,
        entity_id: str,
        attribute_id: str,
        model_id: Optional[str] = None,
    ) -> bool:
        """
        Delete aboutness for an attribute.

        Args:
            entity_id: Entity ID.
            attribute_id: Attribute ID.
            model_id: Optional model ID.

        Returns:
            True if deleted.
        """
        model_id = model_id or self.model_id

        self.conn.execute(
            """
            DELETE FROM metadata.attribute_aboutness
            WHERE entity_id = ? AND attribute_id = ?
              AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, attribute_id, model_id],
        )

        return True

    # -------------------------------------------------------------------------
    # Semantic Dependencies
    # -------------------------------------------------------------------------

    def add_dependency(self, dependency: SemanticDependency) -> str:
        """
        Add a semantic dependency.

        Args:
            dependency: SemanticDependency to add.

        Returns:
            Dependency ID.
        """
        dep_id = dependency.dependency_id or f"sd_{uuid.uuid4().hex[:12]}"
        model_id = dependency.model_id or self.model_id

        self.conn.execute(
            """
            INSERT INTO metadata.semantic_dependency (
                dependency_id, source_concept, target_concept, dependency_type,
                strength, description, model_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                dep_id,
                dependency.source_concept,
                dependency.target_concept,
                dependency.dependency_type.value,
                dependency.strength,
                dependency.description,
                model_id,
            ],
        )

        return dep_id

    def get_dependencies(
        self, concept: str, model_id: Optional[str] = None
    ) -> List[SemanticDependency]:
        """
        Get semantic dependencies for a concept.

        Args:
            concept: Concept or attribute path.
            model_id: Optional model ID.

        Returns:
            List of SemanticDependency.
        """
        model_id = model_id or self.model_id

        rows = self.conn.execute(
            """
            SELECT dependency_id, source_concept, target_concept, dependency_type,
                   strength, description, model_id, created_at
            FROM metadata.semantic_dependency
            WHERE (source_concept = ? OR target_concept = ?)
              AND (model_id = ? OR model_id IS NULL)
            """,
            [concept, concept, model_id],
        ).fetchall()

        return [
            SemanticDependency(
                dependency_id=r[0],
                source_concept=r[1],
                target_concept=r[2],
                dependency_type=DependencyType(r[3]),
                strength=r[4],
                description=r[5],
                model_id=r[6],
                created_at=r[7],
            )
            for r in rows
        ]

    # -------------------------------------------------------------------------
    # Query Methods
    # -------------------------------------------------------------------------

    def find_by_dimension(
        self,
        dimension: AboutnessDimension,
        model_id: Optional[str] = None,
    ) -> List[AttributeAboutness]:
        """
        Find all attributes with a given aboutness dimension.

        Args:
            dimension: Aboutness dimension to search for.
            model_id: Optional model ID.

        Returns:
            List of AttributeAboutness.
        """
        model_id = model_id or self.model_id

        rows = self.conn.execute(
            """
            SELECT aboutness_id, entity_id, attribute_id, model_id, intent,
                   aboutness_dimension, semantic_role, measures_what, identifies_what,
                   classifies_what, relates_to, represents_property, canonical_name,
                   expected_behavior, derived_from, semantic_transform,
                   confidence_score, source, created_at
            FROM metadata.attribute_aboutness
            WHERE aboutness_dimension = ?
              AND (model_id = ? OR model_id IS NULL)
            ORDER BY entity_id, attribute_id
            """,
            [dimension.value, model_id],
        ).fetchall()

        return [
            AttributeAboutness(
                aboutness_id=r[0],
                entity_id=r[1],
                attribute_id=r[2],
                model_id=r[3],
                intent=r[4],
                aboutness_dimension=AboutnessDimension(r[5]),
                semantic_role=SemanticRole(r[6]),
                measures_what=r[7],
                identifies_what=r[8],
                classifies_what=r[9],
                relates_to=r[10],
                represents_property=r[11],
                canonical_name=r[12],
                expected_behavior=json.loads(r[13]) if r[13] else {},
                derived_from=r[14],
                semantic_transform=r[15],
                confidence_score=r[16] or 1.0,
                source=r[17] or "manual",
                created_at=r[18],
            )
            for r in rows
        ]

    def find_by_role(
        self,
        role: SemanticRole,
        model_id: Optional[str] = None,
    ) -> List[AttributeAboutness]:
        """
        Find all attributes with a given semantic role.

        Args:
            role: Semantic role to search for.
            model_id: Optional model ID.

        Returns:
            List of AttributeAboutness.
        """
        model_id = model_id or self.model_id

        rows = self.conn.execute(
            """
            SELECT aboutness_id, entity_id, attribute_id, model_id, intent,
                   aboutness_dimension, semantic_role, measures_what, identifies_what,
                   classifies_what, relates_to, represents_property, canonical_name,
                   expected_behavior, derived_from, semantic_transform,
                   confidence_score, source, created_at
            FROM metadata.attribute_aboutness
            WHERE semantic_role = ?
              AND (model_id = ? OR model_id IS NULL)
            ORDER BY entity_id, attribute_id
            """,
            [role.value, model_id],
        ).fetchall()

        return [
            AttributeAboutness(
                aboutness_id=r[0],
                entity_id=r[1],
                attribute_id=r[2],
                model_id=r[3],
                intent=r[4],
                aboutness_dimension=AboutnessDimension(r[5]),
                semantic_role=SemanticRole(r[6]),
                measures_what=r[7],
                identifies_what=r[8],
                classifies_what=r[9],
                relates_to=r[10],
                represents_property=r[11],
                canonical_name=r[12],
                expected_behavior=json.loads(r[13]) if r[13] else {},
                derived_from=r[14],
                semantic_transform=r[15],
                confidence_score=r[16] or 1.0,
                source=r[17] or "manual",
                created_at=r[18],
            )
            for r in rows
        ]

    def find_by_concept(
        self, concept: str, model_id: Optional[str] = None
    ) -> List[EntityAboutness]:
        """
        Find entities representing a concept.

        Args:
            concept: Concept to search for.
            model_id: Optional model ID.

        Returns:
            List of EntityAboutness.
        """
        model_id = model_id or self.model_id

        rows = self.conn.execute(
            """
            SELECT aboutness_id, entity_id, model_id, purpose, business_context,
                   real_world_object, aboutness_dimension, semantic_category,
                   business_use_cases, stakeholder_groups, represents_concept,
                   equivalent_to, confidence_score, source, created_at, created_by
            FROM metadata.entity_aboutness
            WHERE (real_world_object LIKE ? OR represents_concept LIKE ?)
              AND (model_id = ? OR model_id IS NULL)
            """,
            [f"%{concept}%", f"%{concept}%", model_id],
        ).fetchall()

        return [
            EntityAboutness(
                aboutness_id=r[0],
                entity_id=r[1],
                model_id=r[2],
                purpose=r[3],
                business_context=r[4],
                real_world_object=r[5],
                aboutness_dimension=AboutnessDimension(r[6]),
                semantic_category=r[7],
                business_use_cases=json.loads(r[8]) if r[8] else [],
                stakeholder_groups=json.loads(r[9]) if r[9] else [],
                represents_concept=r[10],
                equivalent_to=json.loads(r[11]) if r[11] else [],
                confidence_score=r[12] or 1.0,
                source=r[13] or "manual",
                created_at=r[14],
                created_by=r[15],
            )
            for r in rows
        ]

    def find_by_canonical_name(
        self, canonical_name: str, model_id: Optional[str] = None
    ) -> List[AttributeAboutness]:
        """
        Find attributes by canonical name.

        Args:
            canonical_name: Cross-system standard name.
            model_id: Optional model ID.

        Returns:
            List of AttributeAboutness.
        """
        model_id = model_id or self.model_id

        rows = self.conn.execute(
            """
            SELECT aboutness_id, entity_id, attribute_id, model_id, intent,
                   aboutness_dimension, semantic_role, measures_what, identifies_what,
                   classifies_what, relates_to, represents_property, canonical_name,
                   expected_behavior, derived_from, semantic_transform,
                   confidence_score, source, created_at
            FROM metadata.attribute_aboutness
            WHERE canonical_name = ?
              AND (model_id = ? OR model_id IS NULL)
            ORDER BY entity_id, attribute_id
            """,
            [canonical_name, model_id],
        ).fetchall()

        return [
            AttributeAboutness(
                aboutness_id=r[0],
                entity_id=r[1],
                attribute_id=r[2],
                model_id=r[3],
                intent=r[4],
                aboutness_dimension=AboutnessDimension(r[5]),
                semantic_role=SemanticRole(r[6]),
                measures_what=r[7],
                identifies_what=r[8],
                classifies_what=r[9],
                relates_to=r[10],
                represents_property=r[11],
                canonical_name=r[12],
                expected_behavior=json.loads(r[13]) if r[13] else {},
                derived_from=r[14],
                semantic_transform=r[15],
                confidence_score=r[16] or 1.0,
                source=r[17] or "manual",
                created_at=r[18],
            )
            for r in rows
        ]

    # -------------------------------------------------------------------------
    # Statistics
    # -------------------------------------------------------------------------

    def get_coverage_stats(
        self, model_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get aboutness coverage statistics.

        Args:
            model_id: Optional model ID.

        Returns:
            Coverage statistics.
        """
        model_id = model_id or self.model_id

        # Total entities and attributes
        entity_count = self.conn.execute(
            """
            SELECT COUNT(DISTINCT entity_id)
            FROM metadata.entity
            WHERE model_id = ? OR model_id IS NULL
            """,
            [model_id],
        ).fetchone()[0]

        attr_count = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM metadata.attribute
            WHERE model_id = ? OR model_id IS NULL
            """,
            [model_id],
        ).fetchone()[0]

        # Aboutness coverage
        entity_aboutness_count = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM metadata.entity_aboutness
            WHERE model_id = ? OR model_id IS NULL
            """,
            [model_id],
        ).fetchone()[0]

        attr_aboutness_count = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM metadata.attribute_aboutness
            WHERE model_id = ? OR model_id IS NULL
            """,
            [model_id],
        ).fetchone()[0]

        # Dimension distribution
        dim_dist = self.conn.execute(
            """
            SELECT aboutness_dimension, COUNT(*)
            FROM metadata.attribute_aboutness
            WHERE model_id = ? OR model_id IS NULL
            GROUP BY aboutness_dimension
            """,
            [model_id],
        ).fetchall()

        return {
            "total_entities": entity_count,
            "total_attributes": attr_count,
            "entities_with_aboutness": entity_aboutness_count,
            "attributes_with_aboutness": attr_aboutness_count,
            "entity_coverage_pct": (
                round(entity_aboutness_count / entity_count * 100, 1)
                if entity_count > 0
                else 0
            ),
            "attribute_coverage_pct": (
                round(attr_aboutness_count / attr_count * 100, 1)
                if attr_count > 0
                else 0
            ),
            "dimension_distribution": {d[0]: d[1] for d in dim_dist},
        }
