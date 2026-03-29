"""
AI Agent Context Builder for MDDE Semantic Layer.

Builds comprehensive knowledge planes for AI agent reasoning
by aggregating metadata from various MDDE sources.

ADR-246: AI Agent Context
Feb 2026
"""

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
import logging
import uuid
import json

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

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


def _generate_id(prefix: str = "") -> str:
    """Generate a unique ID."""
    return f"{prefix}{uuid.uuid4().hex[:12]}"


class ContextBuilder:
    """
    Builds knowledge planes for AI agent context.

    Aggregates information from:
    - Entity metadata
    - Relationships
    - Lineage
    - Business glossary
    - Metrics layer
    - Ontology concepts
    - Data domains
    - Data quality rules
    """

    def __init__(self, conn):
        """
        Initialize context builder.

        Args:
            conn: DuckDB connection to metadata database
        """
        self.conn = conn

    def build_context(self, request: ContextRequest) -> KnowledgePlane:
        """
        Build a knowledge plane based on context request.

        Args:
            request: Context request specification

        Returns:
            KnowledgePlane with aggregated context
        """
        plane = KnowledgePlane(request=request)

        # Build context based on requested types
        if not request.context_types or ContextType.ENTITY in request.context_types:
            plane.entities = self._build_entity_context(request)

        if not request.context_types or ContextType.RELATIONSHIP in request.context_types:
            plane.relationships = self._build_relationship_context(request, plane.entities)

        if ContextType.LINEAGE in request.context_types:
            plane.lineage = self._build_lineage_context(request, plane.entities)

        if ContextType.GLOSSARY in request.context_types:
            plane.glossary = self._build_glossary_context(request)

        if ContextType.METRIC in request.context_types:
            plane.metrics = self._build_metric_context(request)

        if ContextType.CONCEPT in request.context_types:
            plane.concepts = self._build_concept_context(request)

        if ContextType.DOMAIN in request.context_types:
            plane.domains = self._build_domain_context(request)

        if ContextType.QUALITY in request.context_types:
            plane.quality = self._build_quality_context(request, plane.entities)

        # Calculate statistics
        plane.total_entities = len(plane.entities)
        plane.total_relationships = len(plane.relationships)
        plane.total_attributes = sum(len(e.attributes) for e in plane.entities)

        return plane

    def _build_entity_context(self, request: ContextRequest) -> List[EntityContext]:
        """Build entity context."""
        entities = []

        # Build query based on request
        if request.entity_ids:
            placeholders = ", ".join("?" * len(request.entity_ids))
            query = f"SELECT * FROM metadata.entity WHERE entity_id IN ({placeholders})"
            params = request.entity_ids
        elif request.domain:
            query = "SELECT * FROM metadata.entity WHERE domain = ?"
            params = [request.domain]
        elif request.search_terms:
            search_conditions = " OR ".join(
                ["entity_name ILIKE ? OR description ILIKE ?"] * len(request.search_terms)
            )
            query = f"SELECT * FROM metadata.entity WHERE {search_conditions}"
            params = []
            for term in request.search_terms:
                params.extend([f"%{term}%", f"%{term}%"])
        else:
            query = "SELECT * FROM metadata.entity LIMIT 50"
            params = []

        try:
            rows = self.conn.execute(query, params).fetchall()
            col_names = [desc[0] for desc in self.conn.description or []]

            for row in rows:
                row_dict = dict(zip(col_names, row))
                entity_id = row_dict.get("entity_id")

                # Get attributes
                attributes = self._get_entity_attributes(entity_id)

                # Get primary key
                pk = self._get_primary_key(entity_id)

                # Get glossary terms
                glossary_terms = self._get_entity_glossary_terms(entity_id)

                entities.append(EntityContext(
                    entity_id=entity_id,
                    entity_name=row_dict.get("entity_name", ""),
                    display_name=row_dict.get("display_name") or row_dict.get("entity_name", ""),
                    description=row_dict.get("description", ""),
                    domain=row_dict.get("domain"),
                    stereotype=row_dict.get("stereotype"),
                    layer=row_dict.get("layer"),
                    attributes=attributes,
                    primary_key=pk,
                    business_owner=row_dict.get("business_owner"),
                    data_classification=row_dict.get("data_classification"),
                    tags=json.loads(row_dict.get("tags", "[]")) if row_dict.get("tags") else [],
                    glossary_terms=glossary_terms,
                ))

        except Exception as e:
            logger.error(f"Error building entity context: {e}")

        return entities

    def _get_entity_attributes(self, entity_id: str) -> List[Dict[str, Any]]:
        """Get attributes for an entity."""
        attributes = []
        try:
            rows = self.conn.execute(
                """
                SELECT attribute_id, attribute_name, display_name, description,
                       data_type, is_nullable, is_primary_key, is_business_key
                FROM metadata.attribute
                WHERE entity_id = ?
                ORDER BY ordinal_position
                """,
                [entity_id]
            ).fetchall()

            for row in rows:
                attributes.append({
                    "id": row[0],
                    "name": row[1],
                    "display_name": row[2] or row[1],
                    "description": row[3] or "",
                    "data_type": row[4],
                    "is_nullable": row[5],
                    "is_primary_key": row[6],
                    "is_business_key": row[7],
                })

        except Exception as e:
            logger.debug(f"Error getting attributes: {e}")

        return attributes

    def _get_primary_key(self, entity_id: str) -> List[str]:
        """Get primary key columns for an entity."""
        try:
            rows = self.conn.execute(
                """
                SELECT attribute_name FROM metadata.attribute
                WHERE entity_id = ? AND is_primary_key = TRUE
                ORDER BY ordinal_position
                """,
                [entity_id]
            ).fetchall()
            return [row[0] for row in rows]
        except Exception:
            return []

    def _get_entity_glossary_terms(self, entity_id: str) -> List[str]:
        """Get linked glossary terms for an entity."""
        try:
            rows = self.conn.execute(
                """
                SELECT g.term_name FROM metadata.glossary_term g
                JOIN metadata.glossary_entity_link gel ON g.term_id = gel.term_id
                WHERE gel.entity_id = ?
                """,
                [entity_id]
            ).fetchall()
            return [row[0] for row in rows]
        except Exception:
            return []

    def _build_relationship_context(
        self,
        request: ContextRequest,
        entities: List[EntityContext],
    ) -> List[RelationshipContext]:
        """Build relationship context."""
        relationships = []
        entity_ids = [e.entity_id for e in entities]

        if not entity_ids:
            return relationships

        try:
            placeholders = ", ".join("?" * len(entity_ids))
            rows = self.conn.execute(
                f"""
                SELECT from_entity_id, to_entity_id, relationship_type,
                       cardinality, description
                FROM metadata.relationship
                WHERE from_entity_id IN ({placeholders})
                   OR to_entity_id IN ({placeholders})
                """,
                entity_ids + entity_ids
            ).fetchall()

            # Get entity name mapping
            entity_names = {e.entity_id: e.entity_name for e in entities}

            for row in rows:
                from_name = entity_names.get(row[0], row[0])
                to_name = entity_names.get(row[1], row[1])

                relationships.append(RelationshipContext(
                    from_entity=from_name,
                    to_entity=to_name,
                    relationship_type=row[2] or "references",
                    cardinality=row[3] or "1:N",
                    description=row[4] or "",
                ))

        except Exception as e:
            logger.debug(f"Error building relationship context: {e}")

        return relationships

    def _build_lineage_context(
        self,
        request: ContextRequest,
        entities: List[EntityContext],
    ) -> List[LineageContext]:
        """Build lineage context."""
        lineage = []

        for entity in entities:
            upstream = self._get_upstream_lineage(entity.entity_id, request.max_depth)
            downstream = self._get_downstream_lineage(entity.entity_id, request.max_depth)

            if upstream or downstream:
                lineage.append(LineageContext(
                    entity_id=entity.entity_id,
                    entity_name=entity.entity_name,
                    upstream=upstream,
                    downstream=downstream,
                ))

        return lineage

    def _get_upstream_lineage(self, entity_id: str, max_depth: int) -> List[Dict[str, Any]]:
        """Get upstream lineage for an entity."""
        upstream = []
        try:
            rows = self.conn.execute(
                """
                SELECT source_entity_id, source_entity_name, transformation_type
                FROM metadata.lineage
                WHERE target_entity_id = ?
                """,
                [entity_id]
            ).fetchall()

            for row in rows:
                upstream.append({
                    "entity_id": row[0],
                    "entity_name": row[1],
                    "transformation": row[2],
                })

        except Exception:
            pass

        return upstream

    def _get_downstream_lineage(self, entity_id: str, max_depth: int) -> List[Dict[str, Any]]:
        """Get downstream lineage for an entity."""
        downstream = []
        try:
            rows = self.conn.execute(
                """
                SELECT target_entity_id, target_entity_name, transformation_type
                FROM metadata.lineage
                WHERE source_entity_id = ?
                """,
                [entity_id]
            ).fetchall()

            for row in rows:
                downstream.append({
                    "entity_id": row[0],
                    "entity_name": row[1],
                    "transformation": row[2],
                })

        except Exception:
            pass

        return downstream

    def _build_glossary_context(self, request: ContextRequest) -> List[GlossaryContext]:
        """Build glossary context."""
        glossary = []

        query = "SELECT * FROM metadata.glossary_term WHERE 1=1"
        params = []

        if request.domain:
            query += " AND domain = ?"
            params.append(request.domain)

        if request.search_terms:
            search_conditions = " OR ".join(
                ["term_name ILIKE ? OR definition ILIKE ?"] * len(request.search_terms)
            )
            query += f" AND ({search_conditions})"
            for term in request.search_terms:
                params.extend([f"%{term}%", f"%{term}%"])

        query += " LIMIT 50"

        try:
            rows = self.conn.execute(query, params).fetchall()
            col_names = [desc[0] for desc in self.conn.description or []]

            for row in rows:
                row_dict = dict(zip(col_names, row))
                glossary.append(GlossaryContext(
                    term_id=row_dict.get("term_id", ""),
                    term_name=row_dict.get("term_name", ""),
                    definition=row_dict.get("definition", ""),
                    synonyms=json.loads(row_dict.get("synonyms", "[]")) if row_dict.get("synonyms") else [],
                    domain=row_dict.get("domain"),
                ))

        except Exception as e:
            logger.debug(f"Error building glossary context: {e}")

        return glossary

    def _build_metric_context(self, request: ContextRequest) -> List[MetricContext]:
        """Build metric context."""
        metrics = []

        query = "SELECT * FROM metadata.metric_def WHERE 1=1"
        params = []

        if request.domain:
            query += " AND domain = ?"
            params.append(request.domain)

        query += " LIMIT 50"

        try:
            rows = self.conn.execute(query, params).fetchall()
            col_names = [desc[0] for desc in self.conn.description or []]

            for row in rows:
                row_dict = dict(zip(col_names, row))
                metrics.append(MetricContext(
                    metric_id=row_dict.get("metric_id", ""),
                    metric_name=row_dict.get("metric_name", ""),
                    display_name=row_dict.get("display_name") or row_dict.get("metric_name", ""),
                    description=row_dict.get("description", ""),
                    calculation=row_dict.get("expression", ""),
                    unit=row_dict.get("unit"),
                    domain=row_dict.get("domain"),
                    source_entity=row_dict.get("entity_id"),
                ))

        except Exception as e:
            logger.debug(f"Error building metric context: {e}")

        return metrics

    def _build_concept_context(self, request: ContextRequest) -> List[ConceptContext]:
        """Build ontology concept context."""
        concepts = []

        query = "SELECT * FROM metadata.ontology_concept WHERE 1=1"
        params = []

        query += " LIMIT 50"

        try:
            rows = self.conn.execute(query, params).fetchall()
            col_names = [desc[0] for desc in self.conn.description or []]

            for row in rows:
                row_dict = dict(zip(col_names, row))
                concept_id = row_dict.get("concept_id", "")

                # Get parent concepts
                parents = self._get_parent_concepts(concept_id)
                children = self._get_child_concepts(concept_id)

                concepts.append(ConceptContext(
                    concept_id=concept_id,
                    concept_name=row_dict.get("concept_name", ""),
                    definition=row_dict.get("definition", ""),
                    parent_concepts=parents,
                    child_concepts=children,
                ))

        except Exception as e:
            logger.debug(f"Error building concept context: {e}")

        return concepts

    def _get_parent_concepts(self, concept_id: str) -> List[str]:
        """Get parent concepts."""
        try:
            rows = self.conn.execute(
                """
                SELECT parent_concept_name FROM metadata.ontology_concept
                WHERE concept_id IN (
                    SELECT parent_concept_id FROM metadata.ontology_concept_hierarchy
                    WHERE child_concept_id = ?
                )
                """,
                [concept_id]
            ).fetchall()
            return [row[0] for row in rows]
        except Exception:
            return []

    def _get_child_concepts(self, concept_id: str) -> List[str]:
        """Get child concepts."""
        try:
            rows = self.conn.execute(
                """
                SELECT concept_name FROM metadata.ontology_concept
                WHERE concept_id IN (
                    SELECT child_concept_id FROM metadata.ontology_concept_hierarchy
                    WHERE parent_concept_id = ?
                )
                """,
                [concept_id]
            ).fetchall()
            return [row[0] for row in rows]
        except Exception:
            return []

    def _build_domain_context(self, request: ContextRequest) -> List[DomainContext]:
        """Build domain context."""
        domains = []

        query = "SELECT DISTINCT domain FROM metadata.entity WHERE domain IS NOT NULL"

        try:
            rows = self.conn.execute(query).fetchall()

            for row in rows:
                domain_name = row[0]
                if not domain_name:
                    continue

                # Get entities in domain
                entities = self.conn.execute(
                    "SELECT entity_name FROM metadata.entity WHERE domain = ?",
                    [domain_name]
                ).fetchall()

                domains.append(DomainContext(
                    domain_id=domain_name.lower().replace(" ", "_"),
                    domain_name=domain_name,
                    description=f"Data domain: {domain_name}",
                    entities=[e[0] for e in entities],
                ))

        except Exception as e:
            logger.debug(f"Error building domain context: {e}")

        return domains

    def _build_quality_context(
        self,
        request: ContextRequest,
        entities: List[EntityContext],
    ) -> List[QualityContext]:
        """Build quality context."""
        quality = []

        for entity in entities:
            try:
                # Get validation rules
                rules = self.conn.execute(
                    """
                    SELECT rule_id, rule_name, rule_expression, severity
                    FROM metadata.validation_rule
                    WHERE entity_id = ?
                    """,
                    [entity.entity_id]
                ).fetchall()

                if rules:
                    quality.append(QualityContext(
                        entity_id=entity.entity_id,
                        rules=[
                            {
                                "id": r[0],
                                "name": r[1],
                                "expression": r[2],
                                "severity": r[3],
                            }
                            for r in rules
                        ],
                    ))

            except Exception:
                pass

        return quality


class AgentContextProvider:
    """
    High-level provider for AI agent context.

    Provides simplified interface for AI integrations.
    """

    def __init__(self, conn):
        """Initialize provider."""
        self.conn = conn
        self.builder = ContextBuilder(conn)

    def get_context_for_query(
        self,
        query_text: str,
        intent: Optional[QueryIntent] = None,
        max_tokens: int = 8000,
    ) -> str:
        """
        Get context string for an AI query.

        Args:
            query_text: Natural language query
            intent: Query intent (auto-detected if not provided)
            max_tokens: Maximum context tokens

        Returns:
            Formatted context string
        """
        # Detect intent if not provided
        if not intent:
            intent = self._detect_intent(query_text)

        # Build context request based on intent
        request = self._build_request_for_intent(query_text, intent)

        # Build knowledge plane
        plane = self.builder.build_context(request)

        # Return prompt-friendly context
        return plane.to_prompt_context(max_tokens)

    def get_context_for_entity(
        self,
        entity_name: str,
        include_related: bool = True,
    ) -> KnowledgePlane:
        """
        Get context for a specific entity.

        Args:
            entity_name: Entity name
            include_related: Include related entities

        Returns:
            KnowledgePlane with entity context
        """
        # Find entity
        try:
            row = self.conn.execute(
                "SELECT entity_id FROM metadata.entity WHERE entity_name = ?",
                [entity_name]
            ).fetchone()

            if row:
                entity_id = row[0]
                context_types = [ContextType.ENTITY, ContextType.RELATIONSHIP]
                if include_related:
                    context_types.extend([ContextType.LINEAGE, ContextType.GLOSSARY])

                request = ContextRequest(
                    request_id=_generate_id("req_"),
                    context_types=context_types,
                    entity_ids=[entity_id],
                )

                return self.builder.build_context(request)

        except Exception as e:
            logger.error(f"Error getting entity context: {e}")

        return KnowledgePlane(request=ContextRequest(request_id=_generate_id("req_")))

    def get_context_for_domain(self, domain: str) -> KnowledgePlane:
        """
        Get context for a data domain.

        Args:
            domain: Domain name

        Returns:
            KnowledgePlane with domain context
        """
        request = ContextRequest(
            request_id=_generate_id("req_"),
            context_types=[
                ContextType.DOMAIN,
                ContextType.ENTITY,
                ContextType.GLOSSARY,
                ContextType.METRIC,
            ],
            domain=domain,
        )

        return self.builder.build_context(request)

    def search_context(
        self,
        search_terms: List[str],
        context_types: Optional[List[ContextType]] = None,
    ) -> KnowledgePlane:
        """
        Search for context across metadata.

        Args:
            search_terms: Terms to search for
            context_types: Types of context to include

        Returns:
            KnowledgePlane with search results
        """
        request = ContextRequest(
            request_id=_generate_id("req_"),
            context_types=context_types or [ContextType.ENTITY, ContextType.GLOSSARY],
            search_terms=search_terms,
        )

        return self.builder.build_context(request)

    def _detect_intent(self, query_text: str) -> QueryIntent:
        """Detect query intent from text."""
        query_lower = query_text.lower()

        if any(w in query_lower for w in ["what is", "explain", "describe", "meaning"]):
            return QueryIntent.UNDERSTAND
        elif any(w in query_lower for w in ["find", "search", "where", "which"]):
            return QueryIntent.FIND
        elif any(w in query_lower for w in ["relate", "connect", "lineage", "flow"]):
            return QueryIntent.ANALYZE
        elif any(w in query_lower for w in ["generate", "create", "write", "sql"]):
            return QueryIntent.GENERATE
        elif any(w in query_lower for w in ["validate", "check", "verify"]):
            return QueryIntent.VALIDATE
        elif any(w in query_lower for w in ["suggest", "recommend", "improve"]):
            return QueryIntent.SUGGEST
        else:
            return QueryIntent.EXPLORE

    def _build_request_for_intent(self, query_text: str, intent: QueryIntent) -> ContextRequest:
        """Build context request based on intent."""
        # Extract potential entity/term names from query
        search_terms = self._extract_search_terms(query_text)

        if intent == QueryIntent.EXPLORE:
            return ContextRequest(
                request_id=_generate_id("req_"),
                context_types=[ContextType.DOMAIN, ContextType.ENTITY],
                search_terms=search_terms,
            )
        elif intent == QueryIntent.UNDERSTAND:
            return ContextRequest(
                request_id=_generate_id("req_"),
                context_types=[ContextType.GLOSSARY, ContextType.CONCEPT, ContextType.ENTITY],
                search_terms=search_terms,
            )
        elif intent == QueryIntent.FIND:
            return ContextRequest(
                request_id=_generate_id("req_"),
                context_types=[ContextType.ENTITY],
                search_terms=search_terms,
            )
        elif intent == QueryIntent.ANALYZE:
            return ContextRequest(
                request_id=_generate_id("req_"),
                context_types=[ContextType.ENTITY, ContextType.RELATIONSHIP, ContextType.LINEAGE],
                search_terms=search_terms,
                max_depth=3,
            )
        elif intent == QueryIntent.GENERATE:
            return ContextRequest(
                request_id=_generate_id("req_"),
                context_types=[ContextType.ENTITY, ContextType.RELATIONSHIP, ContextType.METRIC],
                search_terms=search_terms,
            )
        elif intent == QueryIntent.VALIDATE:
            return ContextRequest(
                request_id=_generate_id("req_"),
                context_types=[ContextType.ENTITY, ContextType.QUALITY],
                search_terms=search_terms,
            )
        else:
            return ContextRequest(
                request_id=_generate_id("req_"),
                context_types=[ContextType.ENTITY, ContextType.GLOSSARY],
                search_terms=search_terms,
            )

    def _extract_search_terms(self, query_text: str) -> List[str]:
        """Extract potential search terms from query text."""
        import re

        # Remove common words
        stop_words = {
            "what", "is", "the", "a", "an", "and", "or", "how", "does",
            "can", "you", "me", "about", "of", "in", "to", "for", "with",
            "this", "that", "these", "those", "be", "are", "was", "were",
            "find", "show", "get", "tell", "explain", "describe",
        }

        # Extract words
        words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', query_text)

        # Filter and return
        terms = [w for w in words if w.lower() not in stop_words and len(w) > 2]

        return terms[:5]  # Limit to 5 terms
