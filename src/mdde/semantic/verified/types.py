"""
Verified Query Repository Types.

Types for the Verified Query Repository (ADR-375).
Verified queries are question-SQL pairs attached to semantic models
that improve text-to-SQL accuracy through example-based learning.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class QuestionIntent(Enum):
    """Standard question intent categories for verified queries."""

    RANKING = "ranking"  # Top N, bottom N, best, worst
    TREND = "trend"  # Over time patterns, growth, decline
    COMPARISON = "comparison"  # A vs B, before/after
    AGGREGATION = "aggregation"  # Sum, count, avg, totals
    FILTERING = "filtering"  # Where conditions, segments
    LOOKUP = "lookup"  # Single record retrieval
    DISTRIBUTION = "distribution"  # Breakdown by category
    CORRELATION = "correlation"  # Relationship between metrics
    ANOMALY = "anomaly"  # Outliers, unusual patterns
    FORECAST = "forecast"  # Predictions, projections


class QueryComplexity(Enum):
    """Complexity levels for verified queries."""

    SIMPLE = "simple"  # Single table, basic aggregation
    MEDIUM = "medium"  # 2-3 tables, standard joins
    COMPLEX = "complex"  # Multiple joins, CTEs, window functions


class QuerySource(Enum):
    """Source of verified query."""

    MANUAL = "manual"  # Manually created by user
    AUTO_PROMOTED = "auto_promoted"  # Auto-promoted from query history
    IMPORTED = "imported"  # Imported from external source
    GENERATED = "generated"  # AI-generated and verified


class VerificationStatus(Enum):
    """Verification status for queries."""

    PENDING = "pending"  # Awaiting verification
    VERIFIED = "verified"  # Verified by human
    REJECTED = "rejected"  # Rejected during verification
    DEPRECATED = "deprecated"  # No longer valid


@dataclass
class VerifiedQuery:
    """
    A verified question-SQL pair attached to a semantic model.

    Verified queries improve text-to-SQL accuracy by providing
    concrete examples for the LLM to reason from.
    """

    query_id: str
    question: str
    sql: str
    intent: QuestionIntent
    semantic_model_id: str

    # Entity/metric references
    entities: List[str] = field(default_factory=list)
    metrics: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    filters: List[str] = field(default_factory=list)

    # Classification
    complexity: QueryComplexity = QueryComplexity.MEDIUM
    tags: List[str] = field(default_factory=list)

    # Verification
    status: VerificationStatus = VerificationStatus.VERIFIED
    verified_by: Optional[str] = None
    verified_at: Optional[datetime] = None
    source: QuerySource = QuerySource.MANUAL

    # Usage tracking
    usage_count: int = 0
    last_used_at: Optional[datetime] = None
    satisfaction_rate: Optional[float] = None  # 0.0 to 1.0
    feedback_count: int = 0

    # Similarity search
    embedding: Optional[List[float]] = None
    keywords: List[str] = field(default_factory=list)

    # Metadata
    description: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_at: Optional[datetime] = None

    # Variations
    question_variations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "query_id": self.query_id,
            "question": self.question,
            "sql": self.sql,
            "intent": self.intent.value,
            "semantic_model_id": self.semantic_model_id,
            "entities": self.entities,
            "metrics": self.metrics,
            "dimensions": self.dimensions,
            "filters": self.filters,
            "complexity": self.complexity.value,
            "tags": self.tags,
            "status": self.status.value,
            "verified_by": self.verified_by,
            "verified_at": self.verified_at.isoformat() if self.verified_at else None,
            "source": self.source.value,
            "usage_count": self.usage_count,
            "satisfaction_rate": self.satisfaction_rate,
            "description": self.description,
            "question_variations": self.question_variations,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VerifiedQuery":
        """Create from dictionary."""
        return cls(
            query_id=data["query_id"],
            question=data["question"],
            sql=data["sql"],
            intent=QuestionIntent(data.get("intent", "aggregation")),
            semantic_model_id=data.get("semantic_model_id", ""),
            entities=data.get("entities", []),
            metrics=data.get("metrics", []),
            dimensions=data.get("dimensions", []),
            filters=data.get("filters", []),
            complexity=QueryComplexity(data.get("complexity", "medium")),
            tags=data.get("tags", []),
            status=VerificationStatus(data.get("status", "verified")),
            verified_by=data.get("verified_by"),
            verified_at=(
                datetime.fromisoformat(data["verified_at"])
                if data.get("verified_at")
                else None
            ),
            source=QuerySource(data.get("source", "manual")),
            usage_count=data.get("usage_count", 0),
            satisfaction_rate=data.get("satisfaction_rate"),
            description=data.get("description"),
            question_variations=data.get("question_variations", []),
        )


@dataclass
class RetrievalResult:
    """Result from retrieving similar verified queries."""

    query: VerifiedQuery
    similarity_score: float  # 0.0 to 1.0
    match_reasons: List[str]  # Why this was matched


@dataclass
class PromotionCandidate:
    """A query candidate for auto-promotion to verified status."""

    question: str
    sql: str
    frequency: int  # How many times asked
    unique_users: int  # How many unique users
    satisfaction_rate: float  # User satisfaction
    first_asked: datetime
    last_asked: datetime
    suggested_intent: QuestionIntent
    suggested_entities: List[str]
    confidence: float  # Confidence in suggestion


@dataclass
class RepositoryStats:
    """Statistics about the verified query repository."""

    total_queries: int
    queries_by_intent: Dict[str, int]
    queries_by_complexity: Dict[str, int]
    entity_coverage: float  # % of entities with examples
    metric_coverage: float  # % of metrics with examples
    avg_usage_count: float
    avg_satisfaction_rate: float
    queries_by_source: Dict[str, int]
    last_updated: datetime
