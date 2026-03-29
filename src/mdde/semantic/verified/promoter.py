"""
Verified Query Auto-Promoter.

Automatically promotes high-quality queries from usage history
to verified status (ADR-375).
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .repository import VerifiedQueryRepository
from .types import (
    PromotionCandidate,
    QueryComplexity,
    QuerySource,
    QuestionIntent,
    VerifiedQuery,
)


@dataclass
class PromoterConfig:
    """Configuration for auto-promotion."""

    # Frequency thresholds
    min_query_count: int = 10  # Minimum times query was asked
    min_unique_users: int = 3  # Minimum unique users

    # Quality thresholds
    min_satisfaction_rate: float = 0.80  # 80% satisfaction
    min_success_rate: float = 0.90  # 90% successful executions

    # Time window
    lookback_days: int = 30

    # Limits
    max_promotions_per_run: int = 10

    # SQL validation
    validate_sql: bool = True

    # Require human approval
    require_approval: bool = True


class VerifiedQueryPromoter:
    """
    Auto-promote queries from history to verified status.

    Analyzes query logs to find frequently-asked questions with
    high satisfaction rates and promotes them to verified status.

    Usage:
        promoter = VerifiedQueryPromoter(repository, conn)

        # Find candidates
        candidates = promoter.find_candidates("sales_model")

        # Promote (with approval)
        promoted = promoter.promote_candidates(
            candidates[:5],
            semantic_model_id="sales_model",
            approved_by="data_team"
        )
    """

    def __init__(
        self,
        repository: VerifiedQueryRepository,
        conn: Any,
        config: Optional[PromoterConfig] = None,
    ):
        """
        Initialize promoter.

        Args:
            repository: Verified query repository
            conn: Database connection for query logs
            config: Promotion configuration
        """
        self.repository = repository
        self.conn = conn
        self.config = config or PromoterConfig()

    def find_candidates(
        self,
        semantic_model_id: str,
        limit: Optional[int] = None,
    ) -> List[PromotionCandidate]:
        """
        Find candidates for promotion from query history.

        Args:
            semantic_model_id: Semantic model to analyze
            limit: Maximum candidates to return

        Returns:
            List of promotion candidates sorted by score
        """
        limit = limit or self.config.max_promotions_per_run
        config = self.config
        lookback_date = datetime.utcnow() - timedelta(days=config.lookback_days)

        # Query for candidates from query audit log
        try:
            candidates_sql = """
                WITH query_stats AS (
                    SELECT
                        question,
                        generated_sql,
                        COUNT(*) as query_count,
                        COUNT(DISTINCT user_id) as unique_users,
                        SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as success_rate,
                        MIN(created_at) as first_asked,
                        MAX(created_at) as last_asked
                    FROM query_audit_log
                    WHERE semantic_model_id = ?
                      AND created_at >= ?
                      AND question IS NOT NULL
                      AND generated_sql IS NOT NULL
                    GROUP BY question, generated_sql
                    HAVING COUNT(*) >= ?
                       AND COUNT(DISTINCT user_id) >= ?
                ),
                feedback_stats AS (
                    SELECT
                        q.question,
                        SUM(CASE WHEN f.rating = 'positive' THEN 1 ELSE 0 END) * 1.0 /
                            NULLIF(COUNT(f.feedback_id), 0) as satisfaction_rate
                    FROM query_audit_log q
                    LEFT JOIN query_feedback f ON q.query_id = f.query_id
                    WHERE q.semantic_model_id = ?
                      AND q.created_at >= ?
                    GROUP BY q.question
                )
                SELECT
                    qs.question,
                    qs.generated_sql,
                    qs.query_count,
                    qs.unique_users,
                    COALESCE(fs.satisfaction_rate, 0.5) as satisfaction_rate,
                    qs.success_rate,
                    qs.first_asked,
                    qs.last_asked
                FROM query_stats qs
                LEFT JOIN feedback_stats fs ON qs.question = fs.question
                WHERE qs.success_rate >= ?
                  AND COALESCE(fs.satisfaction_rate, 0.5) >= ?
                  AND qs.question NOT IN (
                      SELECT question FROM metadata.verified_query_v2
                      WHERE semantic_model_id = ?
                        AND status = 'verified'
                  )
                ORDER BY qs.query_count DESC, fs.satisfaction_rate DESC
                LIMIT ?
            """

            rows = self.conn.execute(
                candidates_sql,
                [
                    semantic_model_id,
                    lookback_date,
                    config.min_query_count,
                    config.min_unique_users,
                    semantic_model_id,
                    lookback_date,
                    config.min_success_rate,
                    config.min_satisfaction_rate,
                    semantic_model_id,
                    limit * 2,  # Get extra to filter
                ],
            ).fetchall()

        except Exception:
            # Tables may not exist
            return []

        candidates = []
        for row in rows:
            question, sql, query_count, unique_users, satisfaction, success_rate, first_asked, last_asked = row

            # Classify intent
            intent = self._classify_intent(question)

            # Extract entities from SQL
            entities = self._extract_entities_from_sql(sql)

            # Calculate confidence
            confidence = self._calculate_confidence(
                query_count=query_count,
                unique_users=unique_users,
                satisfaction_rate=satisfaction,
                success_rate=success_rate,
            )

            candidates.append(PromotionCandidate(
                question=question,
                sql=sql,
                frequency=query_count,
                unique_users=unique_users,
                satisfaction_rate=satisfaction,
                first_asked=first_asked,
                last_asked=last_asked,
                suggested_intent=intent,
                suggested_entities=entities,
                confidence=confidence,
            ))

        # Sort by confidence
        candidates.sort(key=lambda c: c.confidence, reverse=True)
        return candidates[:limit]

    def promote_candidates(
        self,
        candidates: List[PromotionCandidate],
        semantic_model_id: str,
        approved_by: Optional[str] = None,
    ) -> List[VerifiedQuery]:
        """
        Promote candidates to verified queries.

        Args:
            candidates: Candidates to promote
            semantic_model_id: Target semantic model
            approved_by: User approving promotion (required if config.require_approval)

        Returns:
            List of created verified queries
        """
        if self.config.require_approval and not approved_by:
            raise ValueError("Approval required: set approved_by parameter")

        promoted = []
        now = datetime.utcnow()

        for candidate in candidates:
            # Validate SQL if configured
            if self.config.validate_sql:
                if not self._validate_sql(candidate.sql):
                    continue

            # Create verified query
            query = VerifiedQuery(
                query_id=f"vq_{uuid.uuid4().hex[:12]}",
                question=candidate.question,
                sql=candidate.sql,
                intent=candidate.suggested_intent,
                semantic_model_id=semantic_model_id,
                entities=candidate.suggested_entities,
                complexity=self._estimate_complexity(candidate.sql),
                source=QuerySource.AUTO_PROMOTED,
                verified_by=approved_by,
                verified_at=now,
                usage_count=candidate.frequency,
                satisfaction_rate=candidate.satisfaction_rate,
                tags=["auto-promoted"],
                description=(
                    f"Auto-promoted: {candidate.frequency} queries, "
                    f"{candidate.unique_users} users, "
                    f"{candidate.satisfaction_rate:.0%} satisfaction"
                ),
            )

            self.repository.add_query(query)
            promoted.append(query)

        return promoted

    def promote_single(
        self,
        question: str,
        sql: str,
        semantic_model_id: str,
        verified_by: str,
        intent: Optional[QuestionIntent] = None,
        entities: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> VerifiedQuery:
        """
        Manually promote a single query.

        Args:
            question: Natural language question
            sql: Verified SQL answer
            semantic_model_id: Target semantic model
            verified_by: User verifying
            intent: Question intent (auto-detected if not provided)
            entities: Referenced entities (extracted from SQL if not provided)
            tags: Optional tags

        Returns:
            Created verified query
        """
        query = VerifiedQuery(
            query_id=f"vq_{uuid.uuid4().hex[:12]}",
            question=question,
            sql=sql,
            intent=intent or self._classify_intent(question),
            semantic_model_id=semantic_model_id,
            entities=entities or self._extract_entities_from_sql(sql),
            complexity=self._estimate_complexity(sql),
            source=QuerySource.MANUAL,
            verified_by=verified_by,
            verified_at=datetime.utcnow(),
            tags=tags or [],
        )

        self.repository.add_query(query)
        return query

    def _classify_intent(self, question: str) -> QuestionIntent:
        """Classify question intent."""
        question_lower = question.lower()

        # Check for ranking keywords
        if any(kw in question_lower for kw in ["top", "bottom", "best", "worst", "highest", "lowest"]):
            return QuestionIntent.RANKING

        # Check for trend keywords
        if any(kw in question_lower for kw in ["trend", "over time", "monthly", "weekly", "growth"]):
            return QuestionIntent.TREND

        # Check for comparison keywords
        if any(kw in question_lower for kw in ["compare", "vs", "versus", "difference"]):
            return QuestionIntent.COMPARISON

        # Check for distribution keywords
        if any(kw in question_lower for kw in ["breakdown", "distribution", "by category"]):
            return QuestionIntent.DISTRIBUTION

        # Default to aggregation
        return QuestionIntent.AGGREGATION

    def _extract_entities_from_sql(self, sql: str) -> List[str]:
        """Extract entity/table names from SQL."""
        import re

        entities = []

        # Find FROM/JOIN clauses
        patterns = [
            r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, sql, re.IGNORECASE)
            entities.extend(matches)

        # Deduplicate and sort
        return sorted(set(entities))

    def _estimate_complexity(self, sql: str) -> QueryComplexity:
        """Estimate SQL complexity."""
        sql_upper = sql.upper()

        # Count complexity indicators
        join_count = sql_upper.count(" JOIN ")
        cte_count = sql_upper.count(" WITH ")
        subquery_count = sql_upper.count("SELECT") - 1
        window_count = sql_upper.count(" OVER ")

        complexity_score = join_count + cte_count * 2 + subquery_count * 2 + window_count

        if complexity_score <= 1:
            return QueryComplexity.SIMPLE
        elif complexity_score <= 4:
            return QueryComplexity.MEDIUM
        else:
            return QueryComplexity.COMPLEX

    def _calculate_confidence(
        self,
        query_count: int,
        unique_users: int,
        satisfaction_rate: float,
        success_rate: float,
    ) -> float:
        """Calculate confidence score for a candidate."""
        # Frequency component (log scale)
        import math
        freq_score = min(math.log10(query_count + 1) / 2, 1.0)

        # User diversity
        user_score = min(unique_users / 10, 1.0)

        # Quality
        quality_score = (satisfaction_rate + success_rate) / 2

        # Combined score
        return freq_score * 0.3 + user_score * 0.2 + quality_score * 0.5

    def _validate_sql(self, sql: str) -> bool:
        """Validate SQL syntax."""
        try:
            # Try to parse with sqlglot
            import sqlglot
            sqlglot.parse_one(sql)
            return True
        except Exception:
            return False

    def get_promotion_stats(
        self,
        semantic_model_id: str,
    ) -> Dict[str, Any]:
        """Get statistics about auto-promotion for a model."""
        # Count by source
        source_counts = self.conn.execute("""
            SELECT source, COUNT(*)
            FROM metadata.verified_query_v2
            WHERE semantic_model_id = ?
              AND status = 'verified'
            GROUP BY source
        """, [semantic_model_id]).fetchall()

        # Recent promotions
        recent = self.conn.execute("""
            SELECT COUNT(*)
            FROM metadata.verified_query_v2
            WHERE semantic_model_id = ?
              AND source = 'auto_promoted'
              AND created_at >= CURRENT_DATE - INTERVAL '7' DAY
        """, [semantic_model_id]).fetchone()[0]

        return {
            "by_source": {r[0]: r[1] for r in source_counts},
            "auto_promoted_last_7_days": recent,
            "total_auto_promoted": sum(
                r[1] for r in source_counts if r[0] == "auto_promoted"
            ),
        }
