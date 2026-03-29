# MDDE Aboutness Validator
# ADR-247: Aboutness Layer
# Feb 2026

"""
Validation checks for semantic aboutness consistency.

Ensures that aboutness assignments are semantically coherent
and don't contain conflicting information.
"""

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from .models import (
    AboutnessDimension,
    AboutnessValidation,
    SemanticRole,
)

logger = logging.getLogger(__name__)


# Validation check definitions
ABOUTNESS_CHECKS = {
    "A001": {
        "name": "Missing Entity Purpose",
        "severity": "warning",
        "description": "Entity has no explicit purpose defined",
    },
    "A002": {
        "name": "Conflicting Dimension",
        "severity": "error",
        "description": "Attribute claims incompatible aboutness dimensions",
    },
    "A003": {
        "name": "Aggregation Mismatch",
        "severity": "error",
        "description": "Non-measure attribute marked as aggregatable",
    },
    "A004": {
        "name": "Missing Canonical Name",
        "severity": "info",
        "description": "No cross-system standard name defined for key attribute",
    },
    "A005": {
        "name": "Orphaned Semantic Reference",
        "severity": "warning",
        "description": "Aboutness references non-existent concept or property",
    },
    "A006": {
        "name": "Incomplete Coverage",
        "severity": "info",
        "description": "Entity has attributes without aboutness definitions",
    },
    "A007": {
        "name": "Semantic Drift",
        "severity": "warning",
        "description": "Aboutness intent differs from glossary definition",
    },
    "A008": {
        "name": "Role Conflict",
        "severity": "error",
        "description": "Incompatible semantic roles assigned to attribute",
    },
    "A009": {
        "name": "Type Dimension Mismatch",
        "severity": "warning",
        "description": "Data type doesn't match aboutness dimension expectations",
    },
    "A010": {
        "name": "Low Confidence Inference",
        "severity": "info",
        "description": "Aboutness was inferred with low confidence",
    },
}

# Invalid combinations
INVALID_DIMENSION_ROLE_COMBOS = [
    # Identifiers should not be aggregatable
    (AboutnessDimension.IDENTIFIER, SemanticRole.AGGREGATABLE),
    # Flags should not be aggregatable
    (AboutnessDimension.FLAG, SemanticRole.AGGREGATABLE),
    # States should not be aggregatable
    (AboutnessDimension.STATE, SemanticRole.AGGREGATABLE),
]

# Expected types per dimension
EXPECTED_TYPES = {
    AboutnessDimension.MEASURE: {"INTEGER", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"},
    AboutnessDimension.TEMPORAL: {"DATE", "DATETIME", "TIMESTAMP", "TIME"},
    AboutnessDimension.FLAG: {"BOOLEAN", "BOOL", "BIT", "INTEGER"},
    AboutnessDimension.SPATIAL: {"VARCHAR", "TEXT", "GEOMETRY", "GEOGRAPHY", "POINT"},
}


class AboutnessValidator:
    """
    Validate semantic aboutness for consistency and completeness.

    Example:
        validator = AboutnessValidator(conn)

        # Validate entire model
        issues = validator.validate_model("sales_analytics")

        # Validate single entity
        issues = validator.validate_entity("customer_order")

        # Check specific attribute
        issues = validator.validate_attribute("customer_order", "order_total")
    """

    def __init__(self, conn):
        """
        Initialize validator.

        Args:
            conn: Database connection.
        """
        self.conn = conn

    def validate_model(
        self, model_id: str, save_results: bool = True
    ) -> List[AboutnessValidation]:
        """
        Validate all aboutness in a model.

        Args:
            model_id: Model ID.
            save_results: Whether to save validation results.

        Returns:
            List of validation issues.
        """
        issues = []

        # Get all entities
        entities = self.conn.execute(
            """
            SELECT entity_id FROM metadata.entity
            WHERE model_id = ? OR model_id IS NULL
            """,
            [model_id],
        ).fetchall()

        for (entity_id,) in entities:
            entity_issues = self.validate_entity(entity_id, model_id)
            issues.extend(entity_issues)

        # Save results if requested
        if save_results:
            self._save_validations(issues)

        logger.info(f"Validated model {model_id}: {len(issues)} issues found")
        return issues

    def validate_entity(
        self, entity_id: str, model_id: Optional[str] = None
    ) -> List[AboutnessValidation]:
        """
        Validate aboutness for an entity.

        Args:
            entity_id: Entity ID.
            model_id: Optional model ID.

        Returns:
            List of validation issues.
        """
        issues = []

        # A001: Check entity has purpose
        issues.extend(self._check_entity_purpose(entity_id, model_id))

        # A006: Check attribute coverage
        issues.extend(self._check_attribute_coverage(entity_id, model_id))

        # Validate each attribute
        attributes = self.conn.execute(
            """
            SELECT attribute_id FROM metadata.attribute
            WHERE entity_id = ? AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, model_id],
        ).fetchall()

        for (attr_id,) in attributes:
            attr_issues = self.validate_attribute(entity_id, attr_id, model_id)
            issues.extend(attr_issues)

        return issues

    def validate_attribute(
        self,
        entity_id: str,
        attribute_id: str,
        model_id: Optional[str] = None,
    ) -> List[AboutnessValidation]:
        """
        Validate aboutness for an attribute.

        Args:
            entity_id: Entity ID.
            attribute_id: Attribute ID.
            model_id: Optional model ID.

        Returns:
            List of validation issues.
        """
        issues = []

        # Get attribute aboutness
        aboutness = self.conn.execute(
            """
            SELECT aboutness_dimension, semantic_role, canonical_name,
                   confidence_score, source, represents_property
            FROM metadata.attribute_aboutness
            WHERE entity_id = ? AND attribute_id = ?
              AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, attribute_id, model_id],
        ).fetchone()

        if not aboutness:
            return issues  # No aboutness to validate

        dimension = AboutnessDimension(aboutness[0])
        role = SemanticRole(aboutness[1])
        canonical_name = aboutness[2]
        confidence = aboutness[3] or 1.0
        source = aboutness[4]
        represents_property = aboutness[5]

        # Get attribute data type
        attr_row = self.conn.execute(
            """
            SELECT data_type FROM metadata.attribute
            WHERE entity_id = ? AND attribute_id = ?
              AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, attribute_id, model_id],
        ).fetchone()

        data_type = attr_row[0] if attr_row else None

        # A003: Check aggregation mismatch
        if (dimension, role) in INVALID_DIMENSION_ROLE_COMBOS:
            issues.append(
                AboutnessValidation(
                    validation_id=f"av_{uuid.uuid4().hex[:8]}",
                    entity_id=entity_id,
                    attribute_id=attribute_id,
                    model_id=model_id,
                    check_code="A003",
                    severity="error",
                    message=f"'{attribute_id}' is {dimension.value} but marked as {role.value}",
                    recommendation=f"Change semantic role from {role.value} to a compatible role",
                )
            )

        # A004: Check canonical name for key attributes
        if dimension in [AboutnessDimension.IDENTIFIER, AboutnessDimension.MEASURE]:
            if not canonical_name:
                issues.append(
                    AboutnessValidation(
                        validation_id=f"av_{uuid.uuid4().hex[:8]}",
                        entity_id=entity_id,
                        attribute_id=attribute_id,
                        model_id=model_id,
                        check_code="A004",
                        severity="info",
                        message=f"Key attribute '{attribute_id}' has no canonical name",
                        recommendation="Add canonical_name for cross-system alignment",
                    )
                )

        # A005: Check orphaned references
        if represents_property:
            # Check if property exists in ontology
            prop_exists = self.conn.execute(
                """
                SELECT 1 FROM metadata.ontology_property
                WHERE property_uri = ? OR property_id = ?
                LIMIT 1
                """,
                [represents_property, represents_property],
            ).fetchone()

            if not prop_exists:
                issues.append(
                    AboutnessValidation(
                        validation_id=f"av_{uuid.uuid4().hex[:8]}",
                        entity_id=entity_id,
                        attribute_id=attribute_id,
                        model_id=model_id,
                        check_code="A005",
                        severity="warning",
                        message=f"Property '{represents_property}' not found in ontology",
                        recommendation="Verify ontology reference or remove invalid link",
                    )
                )

        # A009: Check type-dimension mismatch
        if data_type and dimension in EXPECTED_TYPES:
            base_type = self._normalize_type(data_type)
            expected = EXPECTED_TYPES[dimension]
            if base_type not in expected:
                issues.append(
                    AboutnessValidation(
                        validation_id=f"av_{uuid.uuid4().hex[:8]}",
                        entity_id=entity_id,
                        attribute_id=attribute_id,
                        model_id=model_id,
                        check_code="A009",
                        severity="warning",
                        message=f"Data type '{data_type}' unusual for {dimension.value} dimension",
                        recommendation=f"Expected types: {', '.join(expected)}",
                    )
                )

        # A010: Check low confidence
        if source == "inferred" and confidence < 0.5:
            issues.append(
                AboutnessValidation(
                    validation_id=f"av_{uuid.uuid4().hex[:8]}",
                    entity_id=entity_id,
                    attribute_id=attribute_id,
                    model_id=model_id,
                    check_code="A010",
                    severity="info",
                    message=f"Aboutness inferred with low confidence ({confidence:.0%})",
                    recommendation="Review and confirm or manually set aboutness",
                )
            )

        # A007: Check semantic drift from glossary
        issues.extend(
            self._check_semantic_drift(entity_id, attribute_id, model_id)
        )

        return issues

    def _check_entity_purpose(
        self, entity_id: str, model_id: Optional[str]
    ) -> List[AboutnessValidation]:
        """Check if entity has purpose defined."""
        issues = []

        aboutness = self.conn.execute(
            """
            SELECT purpose FROM metadata.entity_aboutness
            WHERE entity_id = ? AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, model_id],
        ).fetchone()

        if not aboutness or not aboutness[0]:
            issues.append(
                AboutnessValidation(
                    validation_id=f"av_{uuid.uuid4().hex[:8]}",
                    entity_id=entity_id,
                    attribute_id=None,
                    model_id=model_id,
                    check_code="A001",
                    severity="warning",
                    message=f"Entity '{entity_id}' has no purpose defined",
                    recommendation="Add purpose explaining why this entity exists",
                )
            )

        return issues

    def _check_attribute_coverage(
        self, entity_id: str, model_id: Optional[str]
    ) -> List[AboutnessValidation]:
        """Check attribute aboutness coverage."""
        issues = []

        # Count total attributes
        total = self.conn.execute(
            """
            SELECT COUNT(*) FROM metadata.attribute
            WHERE entity_id = ? AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, model_id],
        ).fetchone()[0]

        # Count attributes with aboutness
        with_aboutness = self.conn.execute(
            """
            SELECT COUNT(*) FROM metadata.attribute_aboutness
            WHERE entity_id = ? AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, model_id],
        ).fetchone()[0]

        if total > 0 and with_aboutness < total:
            coverage_pct = with_aboutness / total * 100
            if coverage_pct < 50:
                issues.append(
                    AboutnessValidation(
                        validation_id=f"av_{uuid.uuid4().hex[:8]}",
                        entity_id=entity_id,
                        attribute_id=None,
                        model_id=model_id,
                        check_code="A006",
                        severity="info",
                        message=f"Only {coverage_pct:.0f}% of attributes have aboutness ({with_aboutness}/{total})",
                        recommendation="Run 'mdde aboutness infer' to auto-populate",
                    )
                )

        return issues

    def _check_semantic_drift(
        self, entity_id: str, attribute_id: str, model_id: Optional[str]
    ) -> List[AboutnessValidation]:
        """Check for semantic drift from glossary."""
        issues = []

        # Get aboutness intent
        aboutness = self.conn.execute(
            """
            SELECT intent FROM metadata.attribute_aboutness
            WHERE entity_id = ? AND attribute_id = ?
              AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, attribute_id, model_id],
        ).fetchone()

        if not aboutness or not aboutness[0]:
            return issues

        intent = aboutness[0].lower()

        # Get linked glossary term
        term = self.conn.execute(
            """
            SELECT gt.definition
            FROM metadata.glossary_term_link gtl
            JOIN metadata.glossary_term gt ON gtl.term_id = gt.term_id
            WHERE gtl.entity_id = ? AND gtl.attribute_id = ?
            """,
            [entity_id, attribute_id],
        ).fetchone()

        if term and term[0]:
            definition = term[0].lower()

            # Simple similarity check (could use embeddings for better comparison)
            intent_words = set(intent.split())
            def_words = set(definition.split())

            overlap = len(intent_words & def_words)
            total = len(intent_words | def_words)

            if total > 0 and overlap / total < 0.3:  # Less than 30% overlap
                issues.append(
                    AboutnessValidation(
                        validation_id=f"av_{uuid.uuid4().hex[:8]}",
                        entity_id=entity_id,
                        attribute_id=attribute_id,
                        model_id=model_id,
                        check_code="A007",
                        severity="warning",
                        message=f"Aboutness intent differs from glossary definition",
                        recommendation="Align intent with glossary or update glossary definition",
                    )
                )

        return issues

    def _normalize_type(self, data_type: str) -> str:
        """Normalize data type to base type."""
        import re
        if not data_type:
            return "VARCHAR"
        base = re.sub(r"\(.*\)", "", data_type.upper())
        return base.strip()

    def _save_validations(self, validations: List[AboutnessValidation]) -> None:
        """Save validation results to database."""
        for v in validations:
            self.conn.execute(
                """
                INSERT INTO metadata.aboutness_validation (
                    validation_id, entity_id, attribute_id, model_id,
                    check_code, severity, message, recommendation, validated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    v.validation_id,
                    v.entity_id,
                    v.attribute_id,
                    v.model_id,
                    v.check_code,
                    v.severity,
                    v.message,
                    v.recommendation,
                ],
            )

    def get_check_definitions(self) -> Dict[str, Dict[str, str]]:
        """
        Get all validation check definitions.

        Returns:
            Dict of check code to definition.
        """
        return ABOUTNESS_CHECKS

    def clear_validations(self, model_id: str) -> int:
        """
        Clear validation results for a model.

        Args:
            model_id: Model ID.

        Returns:
            Number of records deleted.
        """
        result = self.conn.execute(
            """
            DELETE FROM metadata.aboutness_validation
            WHERE model_id = ?
            """,
            [model_id],
        )
        return result.rowcount
