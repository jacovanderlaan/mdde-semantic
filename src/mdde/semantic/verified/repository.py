"""
Verified Query Repository Manager.

Manages verified question-SQL pairs attached to semantic models (ADR-375).
Provides storage, retrieval, and lifecycle management for verified queries.
"""

import json
import uuid
from datetime import datetime
from typing import Any, List, Optional

from .types import (
    QueryComplexity,
    QuerySource,
    QuestionIntent,
    RepositoryStats,
    VerificationStatus,
    VerifiedQuery,
)


class VerifiedQueryRepository:
    """
    Manages verified queries for semantic models.

    Verified queries are question-SQL pairs that improve text-to-SQL
    accuracy by providing concrete examples for LLMs to reason from.

    Usage:
        repo = VerifiedQueryRepository(conn)

        # Add verified query
        repo.add_query(VerifiedQuery(
            query_id="vq_001",
            question="Top customers by revenue",
            sql="SELECT ...",
            intent=QuestionIntent.RANKING,
            semantic_model_id="sales_model",
            entities=["dim_customer", "fact_orders"],
        ))

        # Retrieve for generation
        examples = repo.retrieve_similar(
            question="Best selling products last month",
            semantic_model_id="sales_model",
            top_k=3
        )

        # Get stats
        stats = repo.get_stats("sales_model")
    """

    def __init__(self, conn: Any):
        """
        Initialize repository.

        Args:
            conn: DuckDB connection
        """
        self.conn = conn
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create tables if they don't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.verified_query_v2 (
                query_id VARCHAR PRIMARY KEY,
                question VARCHAR NOT NULL,
                sql VARCHAR NOT NULL,
                intent VARCHAR NOT NULL,
                semantic_model_id VARCHAR NOT NULL,
                entities JSON,
                metrics JSON,
                dimensions JSON,
                filters JSON,
                complexity VARCHAR DEFAULT 'medium',
                tags JSON,
                status VARCHAR DEFAULT 'verified',
                verified_by VARCHAR,
                verified_at TIMESTAMP,
                source VARCHAR DEFAULT 'manual',
                usage_count INTEGER DEFAULT 0,
                last_used_at TIMESTAMP,
                satisfaction_rate DOUBLE,
                feedback_count INTEGER DEFAULT 0,
                embedding JSON,
                keywords JSON,
                description VARCHAR,
                notes VARCHAR,
                question_variations JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR,
                updated_at TIMESTAMP
            )
        """)

        # Index for fast retrieval
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_verified_query_model
            ON metadata.verified_query_v2(semantic_model_id)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_verified_query_intent
            ON metadata.verified_query_v2(intent)
        """)

    # =========================================================================
    # CRUD Operations
    # =========================================================================

    def add_query(self, query: VerifiedQuery) -> str:
        """
        Add a verified query to the repository.

        Args:
            query: The verified query to add

        Returns:
            Query ID
        """
        if not query.query_id:
            query.query_id = f"vq_{uuid.uuid4().hex[:12]}"

        now = datetime.utcnow()
        if not query.created_at:
            query.created_at = now

        self.conn.execute(
            """
            INSERT INTO metadata.verified_query_v2 (
                query_id, question, sql, intent, semantic_model_id,
                entities, metrics, dimensions, filters,
                complexity, tags, status, verified_by, verified_at,
                source, usage_count, satisfaction_rate,
                embedding, keywords, description, notes,
                question_variations, created_at, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                query.query_id,
                query.question,
                query.sql,
                query.intent.value,
                query.semantic_model_id,
                json.dumps(query.entities),
                json.dumps(query.metrics),
                json.dumps(query.dimensions),
                json.dumps(query.filters),
                query.complexity.value,
                json.dumps(query.tags),
                query.status.value,
                query.verified_by,
                query.verified_at or now,
                query.source.value,
                query.usage_count,
                query.satisfaction_rate,
                json.dumps(query.embedding) if query.embedding else None,
                json.dumps(query.keywords),
                query.description,
                query.notes,
                json.dumps(query.question_variations),
                query.created_at,
                query.created_by,
            ],
        )

        return query.query_id

    def get_query(self, query_id: str) -> Optional[VerifiedQuery]:
        """Get a verified query by ID."""
        row = self.conn.execute(
            "SELECT * FROM metadata.verified_query_v2 WHERE query_id = ?",
            [query_id],
        ).fetchone()

        if not row:
            return None

        return self._row_to_query(row)

    def update_query(self, query: VerifiedQuery) -> None:
        """Update an existing verified query."""
        query.updated_at = datetime.utcnow()

        self.conn.execute(
            """
            UPDATE metadata.verified_query_v2 SET
                question = ?,
                sql = ?,
                intent = ?,
                entities = ?,
                metrics = ?,
                dimensions = ?,
                filters = ?,
                complexity = ?,
                tags = ?,
                status = ?,
                verified_by = ?,
                verified_at = ?,
                description = ?,
                notes = ?,
                question_variations = ?,
                updated_at = ?
            WHERE query_id = ?
            """,
            [
                query.question,
                query.sql,
                query.intent.value,
                json.dumps(query.entities),
                json.dumps(query.metrics),
                json.dumps(query.dimensions),
                json.dumps(query.filters),
                query.complexity.value,
                json.dumps(query.tags),
                query.status.value,
                query.verified_by,
                query.verified_at,
                query.description,
                query.notes,
                json.dumps(query.question_variations),
                query.updated_at,
                query.query_id,
            ],
        )

    def delete_query(self, query_id: str) -> bool:
        """Delete a verified query."""
        result = self.conn.execute(
            "DELETE FROM metadata.verified_query_v2 WHERE query_id = ?",
            [query_id],
        )
        return result.rowcount > 0

    def deprecate_query(self, query_id: str, reason: str) -> None:
        """Mark a query as deprecated."""
        self.conn.execute(
            """
            UPDATE metadata.verified_query_v2 SET
                status = 'deprecated',
                notes = ?,
                updated_at = ?
            WHERE query_id = ?
            """,
            [reason, datetime.utcnow(), query_id],
        )

    # =========================================================================
    # Query Listing
    # =========================================================================

    def list_queries(
        self,
        semantic_model_id: str,
        intent: Optional[QuestionIntent] = None,
        complexity: Optional[QueryComplexity] = None,
        tags: Optional[List[str]] = None,
        status: VerificationStatus = VerificationStatus.VERIFIED,
        limit: int = 100,
    ) -> List[VerifiedQuery]:
        """
        List verified queries for a semantic model.

        Args:
            semantic_model_id: Filter by semantic model
            intent: Filter by question intent
            complexity: Filter by complexity
            tags: Filter by tags (any match)
            status: Filter by verification status
            limit: Maximum results

        Returns:
            List of verified queries
        """
        query = """
            SELECT * FROM metadata.verified_query_v2
            WHERE semantic_model_id = ?
              AND status = ?
        """
        params: List[Any] = [semantic_model_id, status.value]

        if intent:
            query += " AND intent = ?"
            params.append(intent.value)

        if complexity:
            query += " AND complexity = ?"
            params.append(complexity.value)

        query += " ORDER BY usage_count DESC, created_at DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(query, params).fetchall()
        queries = [self._row_to_query(r) for r in rows]

        # Filter by tags if specified
        if tags:
            queries = [q for q in queries if any(t in q.tags for t in tags)]

        return queries

    def list_by_entity(
        self,
        semantic_model_id: str,
        entity_name: str,
        limit: int = 20,
    ) -> List[VerifiedQuery]:
        """List verified queries that reference a specific entity."""
        queries = self.list_queries(semantic_model_id, limit=500)
        return [q for q in queries if entity_name in q.entities][:limit]

    def list_by_metric(
        self,
        semantic_model_id: str,
        metric_name: str,
        limit: int = 20,
    ) -> List[VerifiedQuery]:
        """List verified queries that reference a specific metric."""
        queries = self.list_queries(semantic_model_id, limit=500)
        return [q for q in queries if metric_name in q.metrics][:limit]

    # =========================================================================
    # Usage Tracking
    # =========================================================================

    def record_usage(
        self,
        query_id: str,
        satisfied: Optional[bool] = None,
    ) -> None:
        """
        Record that a verified query was used.

        Args:
            query_id: Query that was used
            satisfied: User satisfaction (True/False/None)
        """
        now = datetime.utcnow()

        # Update usage count
        self.conn.execute(
            """
            UPDATE metadata.verified_query_v2 SET
                usage_count = usage_count + 1,
                last_used_at = ?
            WHERE query_id = ?
            """,
            [now, query_id],
        )

        # Update satisfaction rate if feedback provided
        if satisfied is not None:
            query = self.get_query(query_id)
            if query:
                new_count = query.feedback_count + 1
                current_rate = query.satisfaction_rate or 0.5
                # Running average
                new_rate = (
                    current_rate * query.feedback_count + (1.0 if satisfied else 0.0)
                ) / new_count

                self.conn.execute(
                    """
                    UPDATE metadata.verified_query_v2 SET
                        satisfaction_rate = ?,
                        feedback_count = ?
                    WHERE query_id = ?
                    """,
                    [new_rate, new_count, query_id],
                )

    # =========================================================================
    # Statistics
    # =========================================================================

    def get_stats(self, semantic_model_id: str) -> RepositoryStats:
        """Get statistics for a semantic model's verified queries."""
        # Total queries
        total = self.conn.execute(
            """
            SELECT COUNT(*) FROM metadata.verified_query_v2
            WHERE semantic_model_id = ? AND status = 'verified'
            """,
            [semantic_model_id],
        ).fetchone()[0]

        # By intent
        intent_rows = self.conn.execute(
            """
            SELECT intent, COUNT(*) FROM metadata.verified_query_v2
            WHERE semantic_model_id = ? AND status = 'verified'
            GROUP BY intent
            """,
            [semantic_model_id],
        ).fetchall()
        by_intent = {r[0]: r[1] for r in intent_rows}

        # By complexity
        complexity_rows = self.conn.execute(
            """
            SELECT complexity, COUNT(*) FROM metadata.verified_query_v2
            WHERE semantic_model_id = ? AND status = 'verified'
            GROUP BY complexity
            """,
            [semantic_model_id],
        ).fetchall()
        by_complexity = {r[0]: r[1] for r in complexity_rows}

        # By source
        source_rows = self.conn.execute(
            """
            SELECT source, COUNT(*) FROM metadata.verified_query_v2
            WHERE semantic_model_id = ? AND status = 'verified'
            GROUP BY source
            """,
            [semantic_model_id],
        ).fetchall()
        by_source = {r[0]: r[1] for r in source_rows}

        # Averages
        avg_row = self.conn.execute(
            """
            SELECT
                AVG(usage_count),
                AVG(satisfaction_rate)
            FROM metadata.verified_query_v2
            WHERE semantic_model_id = ? AND status = 'verified'
            """,
            [semantic_model_id],
        ).fetchone()

        # Entity/metric coverage (simplified - would need semantic model info)
        entity_coverage = 0.0
        metric_coverage = 0.0

        return RepositoryStats(
            total_queries=total,
            queries_by_intent=by_intent,
            queries_by_complexity=by_complexity,
            entity_coverage=entity_coverage,
            metric_coverage=metric_coverage,
            avg_usage_count=avg_row[0] or 0.0,
            avg_satisfaction_rate=avg_row[1] or 0.0,
            queries_by_source=by_source,
            last_updated=datetime.utcnow(),
        )

    # =========================================================================
    # Import/Export
    # =========================================================================

    def export_to_yaml(self, semantic_model_id: str) -> str:
        """Export verified queries to YAML format."""
        import yaml

        queries = self.list_queries(semantic_model_id, limit=1000)

        data = {
            "verified_queries": [
                {
                    "id": q.query_id,
                    "question": q.question,
                    "sql": q.sql,
                    "intent": q.intent.value,
                    "entities": q.entities,
                    "metrics": q.metrics,
                    "complexity": q.complexity.value,
                    "verified_by": q.verified_by,
                    "verified_at": (
                        q.verified_at.strftime("%Y-%m-%d") if q.verified_at else None
                    ),
                    "tags": q.tags,
                    "question_variations": q.question_variations,
                }
                for q in queries
            ]
        }

        return yaml.dump(data, default_flow_style=False, sort_keys=False)

    def import_from_yaml(
        self,
        yaml_content: str,
        semantic_model_id: str,
        created_by: Optional[str] = None,
    ) -> int:
        """
        Import verified queries from YAML.

        Args:
            yaml_content: YAML string
            semantic_model_id: Target semantic model
            created_by: User performing import

        Returns:
            Number of queries imported
        """
        import yaml

        data = yaml.safe_load(yaml_content)
        queries = data.get("verified_queries", [])

        imported = 0
        for q_data in queries:
            query = VerifiedQuery(
                query_id=q_data.get("id", f"vq_{uuid.uuid4().hex[:12]}"),
                question=q_data["question"],
                sql=q_data["sql"],
                intent=QuestionIntent(q_data.get("intent", "aggregation")),
                semantic_model_id=semantic_model_id,
                entities=q_data.get("entities", []),
                metrics=q_data.get("metrics", []),
                complexity=QueryComplexity(q_data.get("complexity", "medium")),
                tags=q_data.get("tags", []),
                verified_by=q_data.get("verified_by"),
                source=QuerySource.IMPORTED,
                created_by=created_by,
                question_variations=q_data.get("question_variations", []),
            )
            self.add_query(query)
            imported += 1

        return imported

    # =========================================================================
    # Helpers
    # =========================================================================

    def _row_to_query(self, row: tuple) -> VerifiedQuery:
        """Convert database row to VerifiedQuery."""
        return VerifiedQuery(
            query_id=row[0],
            question=row[1],
            sql=row[2],
            intent=QuestionIntent(row[3]) if row[3] else QuestionIntent.AGGREGATION,
            semantic_model_id=row[4],
            entities=json.loads(row[5]) if row[5] else [],
            metrics=json.loads(row[6]) if row[6] else [],
            dimensions=json.loads(row[7]) if row[7] else [],
            filters=json.loads(row[8]) if row[8] else [],
            complexity=(
                QueryComplexity(row[9]) if row[9] else QueryComplexity.MEDIUM
            ),
            tags=json.loads(row[10]) if row[10] else [],
            status=(
                VerificationStatus(row[11]) if row[11] else VerificationStatus.VERIFIED
            ),
            verified_by=row[12],
            verified_at=row[13],
            source=QuerySource(row[14]) if row[14] else QuerySource.MANUAL,
            usage_count=row[15] or 0,
            last_used_at=row[16],
            satisfaction_rate=row[17],
            feedback_count=row[18] or 0,
            embedding=json.loads(row[19]) if row[19] else None,
            keywords=json.loads(row[20]) if row[20] else [],
            description=row[21],
            notes=row[22],
            question_variations=json.loads(row[23]) if row[23] else [],
            created_at=row[24],
            created_by=row[25],
            updated_at=row[26],
        )
