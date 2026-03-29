# MDDE Aboutness Inference
# ADR-247: Aboutness Layer
# Feb 2026

"""
Pattern-based inference of semantic aboutness.

Automatically infers aboutness dimensions and semantic roles
from attribute names, data types, and patterns.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    AboutnessDimension,
    AttributeAboutness,
    EntityAboutness,
    SemanticRole,
)

logger = logging.getLogger(__name__)


# Pattern definitions for aboutness inference
IDENTIFIER_PATTERNS = [
    r"^id$",
    r"_id$",
    r"^pk_",
    r"_pk$",
    r"_key$",
    r"^key_",
    r"_code$",
    r"_number$",
    r"_num$",
    r"_no$",
    r"^uuid$",
    r"_uuid$",
    r"^guid$",
    r"_guid$",
    r"^sku$",
    r"^ssn$",
    r"^ein$",
    r"_sku$",
]

MEASURE_PATTERNS = [
    r"_amount$",
    r"_amt$",
    r"_total$",
    r"_sum$",
    r"_count$",
    r"_cnt$",
    r"_quantity$",
    r"_qty$",
    r"_price$",
    r"_cost$",
    r"_rate$",
    r"_pct$",
    r"_percent$",
    r"_score$",
    r"_value$",
    r"_balance$",
    r"_revenue$",
    r"_profit$",
    r"_margin$",
    r"_weight$",
    r"_height$",
    r"_width$",
    r"_length$",
    r"_size$",
    r"_duration$",
    r"_distance$",
]

TEMPORAL_PATTERNS = [
    r"_date$",
    r"_dt$",
    r"_time$",
    r"_ts$",
    r"_timestamp$",
    r"^date_",
    r"_at$",
    r"^created",
    r"^modified",
    r"^updated",
    r"^deleted",
    r"_from$",
    r"_to$",
    r"_start$",
    r"_end$",
    r"^effective",
    r"^expiry",
    r"^valid_",
    r"_year$",
    r"_month$",
    r"_day$",
    r"_week$",
    r"_quarter$",
]

CLASSIFIER_PATTERNS = [
    r"_type$",
    r"^type_",
    r"_category$",
    r"_cat$",
    r"_class$",
    r"_classification$",
    r"_segment$",
    r"_tier$",
    r"_level$",
    r"_group$",
    r"_bucket$",
    r"_band$",
]

STATE_PATTERNS = [
    r"_status$",
    r"^status_",
    r"_state$",
    r"^state_",
    r"_phase$",
    r"_stage$",
    r"_step$",
    r"_lifecycle$",
]

FLAG_PATTERNS = [
    r"^is_",
    r"^has_",
    r"^can_",
    r"^should_",
    r"^was_",
    r"^will_",
    r"_flag$",
    r"_ind$",
    r"_indicator$",
    r"^active$",
    r"^enabled$",
    r"^deleted$",
    r"^verified$",
    r"^approved$",
]

RELATIONSHIP_PATTERNS = [
    r"_fk$",
    r"^fk_",
    r"_ref$",
    r"_parent",
    r"_child",
    r"_owner",
    r"_manager",
    r"_assigned",
    r"_created_by",
    r"_modified_by",
    r"_belongs_to",
]

QUALITY_PATTERNS = [
    r"_name$",
    r"^name$",
    r"_desc$",
    r"_description$",
    r"_title$",
    r"_label$",
    r"_note$",
    r"_notes$",
    r"_comment$",
    r"_comments$",
    r"_email$",
    r"_phone$",
    r"_address$",
    r"_url$",
    r"_path$",
]

SPATIAL_PATTERNS = [
    r"_address$",
    r"_city$",
    r"_state$",
    r"_province$",
    r"_country$",
    r"_zip$",
    r"_postal",
    r"_region$",
    r"_zone$",
    r"_latitude$",
    r"_lat$",
    r"_longitude$",
    r"_lon$",
    r"_lng$",
    r"_geo",
    r"_location$",
    r"_coord",
]

# Data type to dimension hints
NUMERIC_TYPES = {"INTEGER", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"}
TEMPORAL_TYPES = {"DATE", "DATETIME", "TIMESTAMP", "TIME"}
BOOLEAN_TYPES = {"BOOLEAN", "BOOL", "BIT"}


class AboutnessInferrer:
    """
    Infer semantic aboutness from patterns and data types.

    Example:
        inferrer = AboutnessInferrer(conn)

        # Infer all attributes for an entity
        inferred = inferrer.infer_entity("customer_order")

        # Infer single attribute
        aboutness = inferrer.infer_attribute("customer_order", "order_total", "DECIMAL")
    """

    def __init__(self, conn):
        """
        Initialize inferrer.

        Args:
            conn: Database connection.
        """
        self.conn = conn

    def infer_entity(
        self, entity_id: str, model_id: Optional[str] = None
    ) -> List[AttributeAboutness]:
        """
        Infer aboutness for all attributes of an entity.

        Args:
            entity_id: Entity ID.
            model_id: Optional model ID.

        Returns:
            List of inferred AttributeAboutness.
        """
        # Get attributes from database
        rows = self.conn.execute(
            """
            SELECT attribute_id, name, data_type, is_primary_key, is_nullable
            FROM metadata.attribute
            WHERE entity_id = ? AND (model_id = ? OR model_id IS NULL)
            ORDER BY ordinal_position
            """,
            [entity_id, model_id],
        ).fetchall()

        inferred = []
        for row in rows:
            attr_id = row[0]
            attr_name = row[1] or attr_id
            data_type = row[2] or "VARCHAR"
            is_pk = row[3]

            aboutness = self.infer_attribute(
                entity_id=entity_id,
                attribute_id=attr_id,
                attribute_name=attr_name,
                data_type=data_type,
                is_primary_key=is_pk,
            )

            if aboutness:
                aboutness.model_id = model_id
                inferred.append(aboutness)

        return inferred

    def infer_attribute(
        self,
        entity_id: str,
        attribute_id: str,
        attribute_name: Optional[str] = None,
        data_type: str = "VARCHAR",
        is_primary_key: bool = False,
    ) -> Optional[AttributeAboutness]:
        """
        Infer aboutness for a single attribute.

        Args:
            entity_id: Entity ID.
            attribute_id: Attribute ID.
            attribute_name: Attribute name (defaults to attribute_id).
            data_type: SQL data type.
            is_primary_key: Whether this is a primary key.

        Returns:
            Inferred AttributeAboutness or None.
        """
        name = (attribute_name or attribute_id).lower()
        base_type = self._normalize_type(data_type)

        # Primary keys are always identifiers
        if is_primary_key:
            return self._create_aboutness(
                entity_id,
                attribute_id,
                AboutnessDimension.IDENTIFIER,
                SemanticRole.JOINABLE,
                f"Primary key identifying {entity_id} records",
                identifies_what=entity_id,
                confidence=0.95,
            )

        # Check patterns in priority order
        dimension, role, intent_template, extra = self._match_patterns(name, base_type)

        if dimension:
            intent = self._generate_intent(intent_template, name, entity_id)
            return self._create_aboutness(
                entity_id,
                attribute_id,
                dimension,
                role,
                intent,
                confidence=0.7,
                **extra,
            )

        # Fall back to data type inference
        dimension, role = self._infer_from_type(base_type)
        if dimension:
            intent = self._generate_intent(
                f"Stores {dimension.value} data", name, entity_id
            )
            return self._create_aboutness(
                entity_id,
                attribute_id,
                dimension,
                role,
                intent,
                confidence=0.5,
            )

        # Default to quality (descriptive)
        return self._create_aboutness(
            entity_id,
            attribute_id,
            AboutnessDimension.QUALITY,
            SemanticRole.DISPLAYABLE,
            f"Describes {name.replace('_', ' ')} for {entity_id}",
            confidence=0.3,
        )

    def _match_patterns(
        self, name: str, data_type: str
    ) -> Tuple[Optional[AboutnessDimension], Optional[SemanticRole], str, Dict]:
        """Match attribute name against patterns."""

        # Check identifier patterns
        for pattern in IDENTIFIER_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return (
                    AboutnessDimension.IDENTIFIER,
                    SemanticRole.JOINABLE,
                    "Uniquely identifies {what}",
                    {"identifies_what": self._extract_what(name, pattern)},
                )

        # Check measure patterns
        for pattern in MEASURE_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return (
                    AboutnessDimension.MEASURE,
                    SemanticRole.AGGREGATABLE,
                    "Measures {what}",
                    {"measures_what": self._extract_what(name, pattern)},
                )

        # Check temporal patterns
        for pattern in TEMPORAL_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return (
                    AboutnessDimension.TEMPORAL,
                    SemanticRole.SLICEABLE,
                    "Records when {what}",
                    {},
                )

        # Check classifier patterns
        for pattern in CLASSIFIER_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return (
                    AboutnessDimension.CLASSIFIER,
                    SemanticRole.GROUPABLE,
                    "Categorizes {entity} by {what}",
                    {"classifies_what": self._extract_what(name, pattern)},
                )

        # Check state patterns
        for pattern in STATE_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return (
                    AboutnessDimension.STATE,
                    SemanticRole.FILTERABLE,
                    "Tracks lifecycle state of {entity}",
                    {},
                )

        # Check flag patterns
        for pattern in FLAG_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return (
                    AboutnessDimension.FLAG,
                    SemanticRole.FILTERABLE,
                    "Indicates whether {what}",
                    {},
                )

        # Check relationship patterns
        for pattern in RELATIONSHIP_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return (
                    AboutnessDimension.RELATIONSHIP,
                    SemanticRole.JOINABLE,
                    "References related {what}",
                    {"relates_to": self._extract_what(name, pattern)},
                )

        # Check spatial patterns
        for pattern in SPATIAL_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return (
                    AboutnessDimension.SPATIAL,
                    SemanticRole.GROUPABLE,
                    "Captures location/geography information",
                    {},
                )

        # Check quality patterns
        for pattern in QUALITY_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return (
                    AboutnessDimension.QUALITY,
                    SemanticRole.DISPLAYABLE,
                    "Describes {what}",
                    {},
                )

        return None, None, "", {}

    def _infer_from_type(
        self, data_type: str
    ) -> Tuple[Optional[AboutnessDimension], Optional[SemanticRole]]:
        """Infer dimension from data type."""
        if data_type in NUMERIC_TYPES:
            return AboutnessDimension.MEASURE, SemanticRole.AGGREGATABLE
        if data_type in TEMPORAL_TYPES:
            return AboutnessDimension.TEMPORAL, SemanticRole.SLICEABLE
        if data_type in BOOLEAN_TYPES:
            return AboutnessDimension.FLAG, SemanticRole.FILTERABLE
        return None, None

    def _normalize_type(self, data_type: str) -> str:
        """Normalize data type to base type."""
        if not data_type:
            return "VARCHAR"
        # Remove precision/scale
        base = re.sub(r"\(.*\)", "", data_type.upper())
        return base.strip()

    def _extract_what(self, name: str, pattern: str) -> str:
        """Extract the 'what' from attribute name."""
        # Remove pattern suffix/prefix
        cleaned = re.sub(pattern, "", name, flags=re.IGNORECASE)
        # Convert to readable form
        cleaned = cleaned.strip("_")
        return cleaned.replace("_", " ").title()

    def _generate_intent(self, template: str, name: str, entity_id: str) -> str:
        """Generate intent description from template."""
        what = name.replace("_", " ").title()
        return template.format(what=what, entity=entity_id)

    def _create_aboutness(
        self,
        entity_id: str,
        attribute_id: str,
        dimension: AboutnessDimension,
        role: SemanticRole,
        intent: str,
        confidence: float = 0.7,
        **kwargs,
    ) -> AttributeAboutness:
        """Create AttributeAboutness with inferred values."""
        return AttributeAboutness(
            entity_id=entity_id,
            attribute_id=attribute_id,
            intent=intent,
            aboutness_dimension=dimension,
            semantic_role=role,
            measures_what=kwargs.get("measures_what"),
            identifies_what=kwargs.get("identifies_what"),
            classifies_what=kwargs.get("classifies_what"),
            relates_to=kwargs.get("relates_to"),
            confidence_score=confidence,
            source="inferred",
        )

    def suggest_canonical_names(
        self, entity_id: str, model_id: Optional[str] = None
    ) -> Dict[str, List[str]]:
        """
        Suggest canonical names for attributes.

        Args:
            entity_id: Entity ID.
            model_id: Optional model ID.

        Returns:
            Dict mapping attribute_id to list of suggested canonical names.
        """
        rows = self.conn.execute(
            """
            SELECT attribute_id, name
            FROM metadata.attribute
            WHERE entity_id = ? AND (model_id = ? OR model_id IS NULL)
            """,
            [entity_id, model_id],
        ).fetchall()

        suggestions = {}
        for row in rows:
            attr_id = row[0]
            name = (row[1] or attr_id).lower()

            # Generate canonical name suggestions
            canonical = []

            # Remove common prefixes
            cleaned = re.sub(r"^(fk_|pk_|idx_|col_)", "", name)

            # Remove entity-specific prefixes
            cleaned = re.sub(f"^{entity_id}_", "", cleaned)

            # Add snake_case version
            canonical.append(cleaned)

            # Add standardized versions
            standardized = self._standardize_name(cleaned)
            if standardized != cleaned:
                canonical.append(standardized)

            suggestions[attr_id] = canonical

        return suggestions

    def _standardize_name(self, name: str) -> str:
        """Standardize attribute name to canonical form."""
        # Common abbreviation expansions
        expansions = {
            "amt": "amount",
            "qty": "quantity",
            "cnt": "count",
            "desc": "description",
            "dt": "date",
            "ts": "timestamp",
            "num": "number",
            "pct": "percent",
            "cat": "category",
            "addr": "address",
            "tel": "telephone",
            "ph": "phone",
            "fax": "facsimile",
            "msg": "message",
            "tx": "transaction",
            "acct": "account",
            "cust": "customer",
            "emp": "employee",
            "org": "organization",
            "dept": "department",
        }

        parts = name.split("_")
        expanded = [expansions.get(p, p) for p in parts]
        return "_".join(expanded)
