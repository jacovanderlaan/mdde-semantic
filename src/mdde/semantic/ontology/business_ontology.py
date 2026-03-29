"""
Business Ontology Layer (ADR-359).

Extends structural ontology with causal relationships and
context-aware metric interpretation.

Inspired by dltHub's "Ontology driven Dimensional Modeling" article:
"Data Models tell you What; Ontologies tell you Why."

Feb 2026
"""

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC time."""
    return datetime.now(timezone.utc)


def _generate_id(prefix: str = "") -> str:
    """Generate a unique ID."""
    return f"{prefix}{uuid.uuid4().hex[:12]}"


class CausalType(Enum):
    """Types of causal relationships."""

    CAUSES = "causes"  # A directly causes B
    INDICATES = "indicates"  # A is an indicator of B
    PREDICTS = "predicts"  # A predicts future B
    CORRELATES = "correlates"  # A and B move together
    PREVENTS = "prevents"  # A prevents B
    ENABLES = "enables"  # A enables B to happen


class CausalDirection(Enum):
    """Direction of causal impact."""

    POSITIVE = "positive"  # More A -> More B
    NEGATIVE = "negative"  # More A -> Less B
    CONDITIONAL = "conditional"  # Depends on context


class Sentiment(Enum):
    """Interpretation sentiment."""

    POSITIVE = "positive"
    NEGATIVE = "negative"
    WARNING = "warning"
    NEUTRAL = "neutral"


@dataclass
class CausalRelationship:
    """
    A causal relationship between business concepts.

    Captures the "why" - why do these concepts relate?
    """

    relationship_id: str
    source_concept_id: str
    target_concept_id: str
    relationship_type: CausalType
    direction: CausalDirection
    context: str = ""  # When this relationship holds
    strength: float = 1.0  # 0.0 to 1.0
    evidence: str = ""  # Supporting evidence/rationale
    created_at: datetime = field(default_factory=_utc_now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "relationship_id": self.relationship_id,
            "source_concept_id": self.source_concept_id,
            "target_concept_id": self.target_concept_id,
            "relationship_type": self.relationship_type.value,
            "direction": self.direction.value,
            "context": self.context,
            "strength": self.strength,
            "evidence": self.evidence,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CausalRelationship":
        """Create from dictionary."""
        return cls(
            relationship_id=data.get("relationship_id", _generate_id("rel_")),
            source_concept_id=data["source_concept_id"],
            target_concept_id=data["target_concept_id"],
            relationship_type=CausalType(data["relationship_type"]),
            direction=CausalDirection(data["direction"]),
            context=data.get("context", ""),
            strength=data.get("strength", 1.0),
            evidence=data.get("evidence", ""),
        )


@dataclass
class BusinessConcept:
    """
    A business concept with causal context.

    Goes beyond structural ontology to capture business meaning.
    """

    concept_id: str
    name: str
    definition: str
    business_model: str = ""  # e.g., "white_glove_enterprise"
    domain: str = ""  # e.g., "customer_success"
    success_indicators: List[str] = field(default_factory=list)
    failure_indicators: List[str] = field(default_factory=list)
    key_metrics: List[str] = field(default_factory=list)
    stakeholders: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "concept_id": self.concept_id,
            "name": self.name,
            "definition": self.definition,
            "business_model": self.business_model,
            "domain": self.domain,
            "success_indicators": self.success_indicators,
            "failure_indicators": self.failure_indicators,
            "key_metrics": self.key_metrics,
            "stakeholders": self.stakeholders,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BusinessConcept":
        """Create from dictionary."""
        return cls(
            concept_id=data.get("concept_id", _generate_id("bc_")),
            name=data["name"],
            definition=data.get("definition", ""),
            business_model=data.get("business_model", ""),
            domain=data.get("domain", ""),
            success_indicators=data.get("success_indicators", []),
            failure_indicators=data.get("failure_indicators", []),
            key_metrics=data.get("key_metrics", []),
            stakeholders=data.get("stakeholders", []),
        )


@dataclass
class Threshold:
    """A named threshold for metric interpretation."""

    name: str
    value: float
    sentiment: Sentiment
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "sentiment": self.sentiment.value,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Threshold":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            value=data["value"],
            sentiment=Sentiment(data.get("sentiment", "neutral")),
            description=data.get("description", ""),
        )


@dataclass
class MetricInterpretation:
    """
    Context-aware metric interpretation rules.

    The same metric means different things in different contexts.
    """

    interpretation_id: str
    metric_id: str
    context: str  # Business context identifier
    increase_means: str  # What an increase indicates
    decrease_means: str  # What a decrease indicates
    thresholds: List[Threshold] = field(default_factory=list)
    recommended_actions: Dict[str, str] = field(default_factory=dict)
    confidence: float = 1.0
    created_at: datetime = field(default_factory=_utc_now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "interpretation_id": self.interpretation_id,
            "metric_id": self.metric_id,
            "context": self.context,
            "increase_means": self.increase_means,
            "decrease_means": self.decrease_means,
            "thresholds": [t.to_dict() for t in self.thresholds],
            "recommended_actions": self.recommended_actions,
            "confidence": self.confidence,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MetricInterpretation":
        """Create from dictionary."""
        return cls(
            interpretation_id=data.get("interpretation_id", _generate_id("mi_")),
            metric_id=data["metric_id"],
            context=data["context"],
            increase_means=data.get("increase_means", ""),
            decrease_means=data.get("decrease_means", ""),
            thresholds=[Threshold.from_dict(t) for t in data.get("thresholds", [])],
            recommended_actions=data.get("recommended_actions", {}),
            confidence=data.get("confidence", 1.0),
        )


@dataclass
class InterpretationResult:
    """Result of interpreting a metric change."""

    metric_id: str
    context: str
    change_percent: float
    sentiment: Sentiment
    explanation: str
    recommended_action: str
    confidence: float
    supporting_evidence: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "metric_id": self.metric_id,
            "context": self.context,
            "change_percent": self.change_percent,
            "sentiment": self.sentiment.value,
            "explanation": self.explanation,
            "recommended_action": self.recommended_action,
            "confidence": self.confidence,
            "supporting_evidence": self.supporting_evidence,
        }


@dataclass
class BusinessOntology:
    """
    A complete business ontology definition.

    Combines concepts, relationships, and interpretations.
    """

    ontology_id: str
    name: str
    version: str = "1.0"
    description: str = ""
    business_model: str = ""
    concepts: List[BusinessConcept] = field(default_factory=list)
    relationships: List[CausalRelationship] = field(default_factory=list)
    interpretations: List[MetricInterpretation] = field(default_factory=list)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "ontology_id": self.ontology_id,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "business_model": self.business_model,
            "concepts": [c.to_dict() for c in self.concepts],
            "relationships": [r.to_dict() for r in self.relationships],
            "interpretations": [i.to_dict() for i in self.interpretations],
        }

    def to_yaml(self) -> str:
        """Serialize to YAML."""
        return yaml.dump(
            self.to_dict(),
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BusinessOntology":
        """Create from dictionary."""
        return cls(
            ontology_id=data.get("ontology_id", _generate_id("bo_")),
            name=data["name"],
            version=data.get("version", "1.0"),
            description=data.get("description", ""),
            business_model=data.get("business_model", ""),
            concepts=[BusinessConcept.from_dict(c) for c in data.get("concepts", [])],
            relationships=[
                CausalRelationship.from_dict(r) for r in data.get("relationships", [])
            ],
            interpretations=[
                MetricInterpretation.from_dict(i)
                for i in data.get("interpretations", [])
            ],
        )

    @classmethod
    def from_yaml(cls, yaml_content: str) -> "BusinessOntology":
        """Load from YAML string."""
        data = yaml.safe_load(yaml_content)
        return cls.from_dict(data)

    @classmethod
    def from_file(cls, path: Path) -> "BusinessOntology":
        """Load from YAML file."""
        content = path.read_text(encoding="utf-8")
        return cls.from_yaml(content)

    def get_concept(self, concept_id: str) -> Optional[BusinessConcept]:
        """Get concept by ID."""
        for concept in self.concepts:
            if concept.concept_id == concept_id:
                return concept
        return None

    def get_interpretation(
        self,
        metric_id: str,
        context: str,
    ) -> Optional[MetricInterpretation]:
        """Get interpretation for metric in context."""
        for interp in self.interpretations:
            if interp.metric_id == metric_id and interp.context == context:
                return interp
        return None

    def get_causal_chain(
        self,
        source_concept_id: str,
        max_depth: int = 5,
    ) -> List[List[CausalRelationship]]:
        """Get all causal chains starting from a concept."""
        chains = []
        self._find_chains(source_concept_id, [], chains, max_depth)
        return chains

    def _find_chains(
        self,
        current_id: str,
        current_chain: List[CausalRelationship],
        all_chains: List[List[CausalRelationship]],
        max_depth: int,
    ) -> None:
        """Recursively find causal chains."""
        if len(current_chain) >= max_depth:
            return

        found_any = False
        for rel in self.relationships:
            if rel.source_concept_id == current_id:
                found_any = True
                new_chain = current_chain + [rel]
                all_chains.append(new_chain)
                self._find_chains(
                    rel.target_concept_id,
                    new_chain,
                    all_chains,
                    max_depth,
                )

        if not found_any and current_chain:
            # End of chain - already added
            pass


class BusinessOntologyManager:
    """
    Manage business ontologies and interpretations.

    Provides:
    - Ontology CRUD operations
    - Context-aware metric interpretation
    - Causal chain analysis
    - Reasoning for AI guidance
    """

    def __init__(self, conn=None):
        """
        Initialize business ontology manager.

        Args:
            conn: Optional DuckDB connection for persistence
        """
        self.conn = conn
        self._ontologies: Dict[str, BusinessOntology] = {}

        if conn:
            self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Ensure database tables exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS business_ontology (
                ontology_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                version VARCHAR DEFAULT '1.0',
                description TEXT,
                business_model VARCHAR,
                ontology_json JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS business_concept (
                concept_id VARCHAR PRIMARY KEY,
                ontology_id VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                definition TEXT,
                business_model VARCHAR,
                domain VARCHAR,
                concept_json JSON,
                FOREIGN KEY (ontology_id) REFERENCES business_ontology(ontology_id)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS causal_relationship (
                relationship_id VARCHAR PRIMARY KEY,
                ontology_id VARCHAR NOT NULL,
                source_concept_id VARCHAR NOT NULL,
                target_concept_id VARCHAR NOT NULL,
                relationship_type VARCHAR NOT NULL,
                direction VARCHAR NOT NULL,
                context TEXT,
                strength REAL DEFAULT 1.0,
                FOREIGN KEY (ontology_id) REFERENCES business_ontology(ontology_id)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metric_interpretation (
                interpretation_id VARCHAR PRIMARY KEY,
                ontology_id VARCHAR NOT NULL,
                metric_id VARCHAR NOT NULL,
                context VARCHAR NOT NULL,
                increase_means TEXT,
                decrease_means TEXT,
                interpretation_json JSON,
                confidence REAL DEFAULT 1.0,
                FOREIGN KEY (ontology_id) REFERENCES business_ontology(ontology_id)
            )
        """)

    def load_ontology(self, path: Path) -> BusinessOntology:
        """
        Load business ontology from file.

        Args:
            path: Path to YAML ontology file

        Returns:
            Loaded BusinessOntology
        """
        ontology = BusinessOntology.from_file(path)
        self._ontologies[ontology.ontology_id] = ontology
        logger.info(f"Loaded business ontology: {ontology.name}")
        return ontology

    def load_ontology_from_yaml(self, yaml_content: str) -> BusinessOntology:
        """
        Load business ontology from YAML string.

        Args:
            yaml_content: YAML content

        Returns:
            Loaded BusinessOntology
        """
        ontology = BusinessOntology.from_yaml(yaml_content)
        self._ontologies[ontology.ontology_id] = ontology
        return ontology

    def create_ontology(
        self,
        name: str,
        business_model: str = "",
        description: str = "",
    ) -> BusinessOntology:
        """
        Create a new business ontology.

        Args:
            name: Ontology name
            business_model: Business model identifier
            description: Description

        Returns:
            New BusinessOntology
        """
        ontology = BusinessOntology(
            ontology_id=_generate_id("bo_"),
            name=name,
            business_model=business_model,
            description=description,
        )
        self._ontologies[ontology.ontology_id] = ontology

        if self.conn:
            self._persist_ontology(ontology)

        return ontology

    def _persist_ontology(self, ontology: BusinessOntology) -> None:
        """Persist ontology to database."""
        self.conn.execute(
            """
            INSERT OR REPLACE INTO business_ontology
            (ontology_id, name, version, description, business_model, ontology_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ontology.ontology_id,
                ontology.name,
                ontology.version,
                ontology.description,
                ontology.business_model,
                json.dumps(ontology.to_dict()),
                _utc_now(),
            ],
        )

    def get_ontology(self, ontology_id: str) -> Optional[BusinessOntology]:
        """Get ontology by ID."""
        if ontology_id in self._ontologies:
            return self._ontologies[ontology_id]

        if self.conn:
            row = self.conn.execute(
                "SELECT ontology_json FROM business_ontology WHERE ontology_id = ?",
                [ontology_id],
            ).fetchone()

            if row:
                data = json.loads(row[0])
                ontology = BusinessOntology.from_dict(data)
                self._ontologies[ontology_id] = ontology
                return ontology

        return None

    def add_concept(
        self,
        ontology_id: str,
        name: str,
        definition: str,
        business_model: str = "",
        domain: str = "",
        success_indicators: Optional[List[str]] = None,
        failure_indicators: Optional[List[str]] = None,
    ) -> BusinessConcept:
        """
        Add a business concept to an ontology.

        Args:
            ontology_id: Ontology ID
            name: Concept name
            definition: Business definition
            business_model: Business model context
            domain: Business domain
            success_indicators: Indicators of success
            failure_indicators: Indicators of failure

        Returns:
            Created BusinessConcept
        """
        ontology = self.get_ontology(ontology_id)
        if not ontology:
            raise ValueError(f"Ontology not found: {ontology_id}")

        concept = BusinessConcept(
            concept_id=_generate_id("bc_"),
            name=name,
            definition=definition,
            business_model=business_model or ontology.business_model,
            domain=domain,
            success_indicators=success_indicators or [],
            failure_indicators=failure_indicators or [],
        )

        ontology.concepts.append(concept)
        ontology.updated_at = _utc_now()

        if self.conn:
            self._persist_ontology(ontology)

        return concept

    def add_causal_relationship(
        self,
        ontology_id: str,
        source_concept_id: str,
        target_concept_id: str,
        relationship_type: CausalType,
        direction: CausalDirection,
        context: str = "",
        strength: float = 1.0,
        evidence: str = "",
    ) -> CausalRelationship:
        """
        Add a causal relationship between concepts.

        Args:
            ontology_id: Ontology ID
            source_concept_id: Source concept
            target_concept_id: Target concept
            relationship_type: Type of causal relationship
            direction: Direction of impact
            context: When this relationship holds
            strength: Relationship strength (0-1)
            evidence: Supporting evidence

        Returns:
            Created CausalRelationship
        """
        ontology = self.get_ontology(ontology_id)
        if not ontology:
            raise ValueError(f"Ontology not found: {ontology_id}")

        relationship = CausalRelationship(
            relationship_id=_generate_id("rel_"),
            source_concept_id=source_concept_id,
            target_concept_id=target_concept_id,
            relationship_type=relationship_type,
            direction=direction,
            context=context,
            strength=strength,
            evidence=evidence,
        )

        ontology.relationships.append(relationship)
        ontology.updated_at = _utc_now()

        if self.conn:
            self._persist_ontology(ontology)

        return relationship

    def add_metric_interpretation(
        self,
        ontology_id: str,
        metric_id: str,
        context: str,
        increase_means: str,
        decrease_means: str,
        thresholds: Optional[List[Threshold]] = None,
        recommended_actions: Optional[Dict[str, str]] = None,
        confidence: float = 1.0,
    ) -> MetricInterpretation:
        """
        Add a context-aware metric interpretation.

        Args:
            ontology_id: Ontology ID
            metric_id: Metric identifier
            context: Business context
            increase_means: What increase indicates
            decrease_means: What decrease indicates
            thresholds: Named thresholds
            recommended_actions: Action recommendations
            confidence: Interpretation confidence

        Returns:
            Created MetricInterpretation
        """
        ontology = self.get_ontology(ontology_id)
        if not ontology:
            raise ValueError(f"Ontology not found: {ontology_id}")

        interpretation = MetricInterpretation(
            interpretation_id=_generate_id("mi_"),
            metric_id=metric_id,
            context=context,
            increase_means=increase_means,
            decrease_means=decrease_means,
            thresholds=thresholds or [],
            recommended_actions=recommended_actions or {},
            confidence=confidence,
        )

        ontology.interpretations.append(interpretation)
        ontology.updated_at = _utc_now()

        if self.conn:
            self._persist_ontology(ontology)

        return interpretation

    def interpret_metric_change(
        self,
        metric_id: str,
        change_percent: float,
        context: str,
        ontology_id: Optional[str] = None,
    ) -> InterpretationResult:
        """
        Interpret a metric change in business context.

        This is the key function that bridges data and business understanding.
        It answers: "What does this change MEAN for the business?"

        Args:
            metric_id: Metric identifier
            change_percent: Percentage change (e.g., -45 for 45% decrease)
            context: Business context identifier
            ontology_id: Optional specific ontology to use

        Returns:
            InterpretationResult with sentiment, explanation, and actions
        """
        # Find applicable interpretation
        interpretation = None

        if ontology_id:
            ontology = self.get_ontology(ontology_id)
            if ontology:
                interpretation = ontology.get_interpretation(metric_id, context)
        else:
            # Search all loaded ontologies
            for ontology in self._ontologies.values():
                interpretation = ontology.get_interpretation(metric_id, context)
                if interpretation:
                    break

        if not interpretation:
            # No specific interpretation found - provide generic
            return InterpretationResult(
                metric_id=metric_id,
                context=context,
                change_percent=change_percent,
                sentiment=Sentiment.NEUTRAL,
                explanation=f"No specific interpretation for {metric_id} in {context}",
                recommended_action="Review metric definition and business context",
                confidence=0.0,
                supporting_evidence=[],
            )

        # Determine sentiment based on change direction
        if change_percent > 0:
            explanation = interpretation.increase_means
        else:
            explanation = interpretation.decrease_means

        # Check thresholds
        sentiment = self._determine_sentiment(
            change_percent,
            interpretation.thresholds,
            interpretation.increase_means,
            interpretation.decrease_means,
        )

        # Get recommended action
        action_key = f"{'increase' if change_percent > 0 else 'decrease'}_{abs(change_percent) > 20}"
        recommended_action = interpretation.recommended_actions.get(
            action_key,
            interpretation.recommended_actions.get(
                "default", "Monitor and investigate"
            ),
        )

        return InterpretationResult(
            metric_id=metric_id,
            context=context,
            change_percent=change_percent,
            sentiment=sentiment,
            explanation=explanation,
            recommended_action=recommended_action,
            confidence=interpretation.confidence,
            supporting_evidence=[
                f"Interpretation from context: {context}",
                f"Confidence: {interpretation.confidence:.0%}",
            ],
        )

    def _determine_sentiment(
        self,
        change_percent: float,
        thresholds: List[Threshold],
        increase_means: str,
        decrease_means: str,
    ) -> Sentiment:
        """Determine sentiment based on change and thresholds."""
        # Check explicit thresholds
        for threshold in thresholds:
            if change_percent >= threshold.value:
                return threshold.sentiment

        # Infer from description keywords
        desc = increase_means.lower() if change_percent > 0 else decrease_means.lower()

        if any(word in desc for word in ["good", "positive", "success", "healthy", "great", "well", "excellent"]):
            return Sentiment.POSITIVE
        elif any(word in desc for word in ["bad", "negative", "failure", "unhealthy", "poor", "decline"]):
            return Sentiment.NEGATIVE
        elif any(word in desc for word in ["warning", "concern", "risk", "disengage", "issue", "problem"]):
            return Sentiment.WARNING
        else:
            return Sentiment.NEUTRAL

    def get_causal_explanation(
        self,
        source_concept_id: str,
        target_concept_id: str,
        ontology_id: str,
    ) -> List[str]:
        """
        Get causal explanation between two concepts.

        Traces the causal chain and provides human-readable explanation.

        Args:
            source_concept_id: Starting concept
            target_concept_id: Target concept
            ontology_id: Ontology ID

        Returns:
            List of explanation strings
        """
        ontology = self.get_ontology(ontology_id)
        if not ontology:
            return []

        chains = ontology.get_causal_chain(source_concept_id)
        explanations = []

        for chain in chains:
            if chain and chain[-1].target_concept_id == target_concept_id:
                # Found a path to target
                path_parts = []
                for rel in chain:
                    source = ontology.get_concept(rel.source_concept_id)
                    target = ontology.get_concept(rel.target_concept_id)
                    source_name = source.name if source else rel.source_concept_id
                    target_name = target.name if target else rel.target_concept_id

                    direction_word = (
                        "increases"
                        if rel.direction == CausalDirection.POSITIVE
                        else "decreases"
                        if rel.direction == CausalDirection.NEGATIVE
                        else "affects"
                    )

                    path_parts.append(
                        f"{source_name} {rel.relationship_type.value} {target_name} "
                        f"({direction_word})"
                    )

                explanations.append(" → ".join(path_parts))

        return explanations

    def list_ontologies(self) -> List[BusinessOntology]:
        """List all loaded ontologies."""
        if self.conn:
            rows = self.conn.execute(
                "SELECT ontology_json FROM business_ontology"
            ).fetchall()

            for row in rows:
                data = json.loads(row[0])
                ontology = BusinessOntology.from_dict(data)
                if ontology.ontology_id not in self._ontologies:
                    self._ontologies[ontology.ontology_id] = ontology

        return list(self._ontologies.values())


__all__ = [
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
]
