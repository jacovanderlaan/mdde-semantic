"""
Metrics Layer Manager for MDDE Semantic Layer.

Manages business metric definitions, dimensions, and query generation.
Provides semantic query translation to SQL.

ADR-245: Metrics Layer
Feb 2026
"""

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import logging
import uuid
import re

from .models import (
    MetricDefinition,
    MetricDimension,
    MetricFilter,
    DerivedMetricFormula,
    MetricGoal,
    MetricAlert,
    MetricQuery,
    MetricQueryResult,
    MetricType,
    AggregationType,
    TimeGrain,
    DimensionRole,
    MetricStatus,
    # Enhanced metrics (v3.23.0)
    PeriodComparison,
    PeriodComparisonType,
    MetricLineage,
    MetricValidation,
    MetricValidationResult,
    MetricValidationLevel,
    SemanticModelExport,
    MetricCalculationContext,
    MetricAnnotation,
    MetricCatalogEntry,
)

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


def _generate_id(prefix: str = "") -> str:
    """Generate a unique ID."""
    return f"{prefix}{uuid.uuid4().hex[:12]}"


class MetricsManager:
    """
    Manages business metrics and semantic queries.

    Provides:
    - Metric CRUD operations
    - Dimension management
    - SQL query generation from semantic queries
    - Metric calculations
    """

    def __init__(self, conn):
        """
        Initialize metrics manager.

        Args:
            conn: DuckDB connection to metadata database
        """
        self.conn = conn
        self._ensure_tables()

    def _ensure_tables(self):
        """Ensure metrics tables exist."""
        try:
            self.conn.execute("SELECT 1 FROM metadata.metric_def LIMIT 1")
        except Exception:
            self._create_tables()

    def _create_tables(self):
        """Create metrics tables."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.metric_def (
                metric_id VARCHAR PRIMARY KEY,
                metric_name VARCHAR NOT NULL,
                display_name VARCHAR,
                description VARCHAR,
                metric_type VARCHAR DEFAULT 'simple',
                status VARCHAR DEFAULT 'active',
                entity_id VARCHAR,
                attribute_id VARCHAR,
                aggregation VARCHAR DEFAULT 'SUM',
                expression VARCHAR,
                filter_expression VARCHAR,
                time_grain VARCHAR,
                time_attribute_id VARCHAR,
                depends_on_metrics VARCHAR,  -- JSON array
                business_owner VARCHAR,
                technical_owner VARCHAR,
                domain VARCHAR,
                format_string VARCHAR,
                unit VARCHAR,
                tags VARCHAR,  -- JSON array
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.metric_dimension (
                dimension_id VARCHAR PRIMARY KEY,
                metric_id VARCHAR NOT NULL,
                attribute_id VARCHAR NOT NULL,
                dimension_name VARCHAR NOT NULL,
                display_name VARCHAR,
                description VARCHAR,
                role VARCHAR DEFAULT 'group_by',
                parent_dimension_id VARCHAR,
                hierarchy_level INTEGER DEFAULT 0,
                is_default BOOLEAN DEFAULT FALSE,
                is_required BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (metric_id) REFERENCES metadata.metric_def(metric_id)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.metric_filter (
                filter_id VARCHAR PRIMARY KEY,
                metric_id VARCHAR NOT NULL,
                filter_name VARCHAR NOT NULL,
                display_name VARCHAR,
                description VARCHAR,
                filter_expression VARCHAR,
                is_default BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (metric_id) REFERENCES metadata.metric_def(metric_id)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.metric_goal (
                goal_id VARCHAR PRIMARY KEY,
                metric_id VARCHAR NOT NULL,
                goal_name VARCHAR NOT NULL,
                target_value DECIMAL,
                comparison VARCHAR DEFAULT 'gte',
                time_period VARCHAR,
                dimension_filters VARCHAR,  -- JSON object
                FOREIGN KEY (metric_id) REFERENCES metadata.metric_def(metric_id)
            )
        """)

        logger.info("Metrics tables created")

    # ==================== Metric CRUD ====================

    def create_metric(self, metric: MetricDefinition) -> str:
        """
        Create a new metric.

        Args:
            metric: Metric definition to create

        Returns:
            Created metric ID
        """
        import json

        self.conn.execute(
            """
            INSERT INTO metadata.metric_def
            (metric_id, metric_name, display_name, description, metric_type, status,
             entity_id, attribute_id, aggregation, expression, filter_expression,
             time_grain, time_attribute_id, depends_on_metrics, business_owner,
             technical_owner, domain, format_string, unit, tags, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                metric.metric_id,
                metric.metric_name,
                metric.display_name,
                metric.description,
                metric.metric_type.value,
                metric.status.value,
                metric.entity_id,
                metric.attribute_id,
                metric.aggregation.value,
                metric.expression,
                metric.filter_expression,
                metric.time_grain.value if metric.time_grain else None,
                metric.time_attribute_id,
                json.dumps(metric.depends_on_metrics),
                metric.business_owner,
                metric.technical_owner,
                metric.domain,
                metric.format_string,
                metric.unit,
                json.dumps(metric.tags),
                metric.created_at,
                metric.updated_at,
            ]
        )

        logger.info(f"Created metric: {metric.metric_id}")
        return metric.metric_id

    def get_metric(self, metric_id: str) -> Optional[MetricDefinition]:
        """Get a metric by ID."""
        import json

        row = self.conn.execute(
            "SELECT * FROM metadata.metric_def WHERE metric_id = ?",
            [metric_id]
        ).fetchone()

        if not row:
            return None

        return self._row_to_metric(row)

    def get_metric_by_name(self, metric_name: str) -> Optional[MetricDefinition]:
        """Get a metric by name."""
        import json

        row = self.conn.execute(
            "SELECT * FROM metadata.metric_def WHERE metric_name = ?",
            [metric_name]
        ).fetchone()

        if not row:
            return None

        return self._row_to_metric(row)

    def _row_to_metric(self, row) -> MetricDefinition:
        """Convert database row to MetricDefinition."""
        import json

        return MetricDefinition(
            metric_id=row[0],
            metric_name=row[1],
            display_name=row[2] or row[1],
            description=row[3] or "",
            metric_type=MetricType(row[4]) if row[4] else MetricType.SIMPLE,
            status=MetricStatus(row[5]) if row[5] else MetricStatus.ACTIVE,
            entity_id=row[6] or "",
            attribute_id=row[7],
            aggregation=AggregationType(row[8]) if row[8] else AggregationType.SUM,
            expression=row[9] or "",
            filter_expression=row[10],
            time_grain=TimeGrain(row[11]) if row[11] else None,
            time_attribute_id=row[12],
            depends_on_metrics=json.loads(row[13]) if row[13] else [],
            business_owner=row[14],
            technical_owner=row[15],
            domain=row[16],
            format_string=row[17],
            unit=row[18],
            tags=json.loads(row[19]) if row[19] else [],
            created_at=row[20],
            updated_at=row[21],
        )

    def list_metrics(
        self,
        domain: Optional[str] = None,
        status: Optional[MetricStatus] = None,
    ) -> List[MetricDefinition]:
        """List metrics with optional filters."""
        query = "SELECT * FROM metadata.metric_def WHERE 1=1"
        params = []

        if domain:
            query += " AND domain = ?"
            params.append(domain)

        if status:
            query += " AND status = ?"
            params.append(status.value)

        query += " ORDER BY metric_name"

        rows = self.conn.execute(query, params).fetchall()
        return [self._row_to_metric(row) for row in rows]

    def update_metric(self, metric: MetricDefinition) -> bool:
        """Update an existing metric."""
        import json

        metric.updated_at = _utc_now()

        self.conn.execute(
            """
            UPDATE metadata.metric_def
            SET metric_name = ?, display_name = ?, description = ?, metric_type = ?,
                status = ?, entity_id = ?, attribute_id = ?, aggregation = ?,
                expression = ?, filter_expression = ?, time_grain = ?,
                time_attribute_id = ?, depends_on_metrics = ?, business_owner = ?,
                technical_owner = ?, domain = ?, format_string = ?, unit = ?,
                tags = ?, updated_at = ?
            WHERE metric_id = ?
            """,
            [
                metric.metric_name,
                metric.display_name,
                metric.description,
                metric.metric_type.value,
                metric.status.value,
                metric.entity_id,
                metric.attribute_id,
                metric.aggregation.value,
                metric.expression,
                metric.filter_expression,
                metric.time_grain.value if metric.time_grain else None,
                metric.time_attribute_id,
                json.dumps(metric.depends_on_metrics),
                metric.business_owner,
                metric.technical_owner,
                metric.domain,
                metric.format_string,
                metric.unit,
                json.dumps(metric.tags),
                metric.updated_at,
                metric.metric_id,
            ]
        )

        return True

    def delete_metric(self, metric_id: str) -> bool:
        """Delete a metric and its related data."""
        self.conn.execute(
            "DELETE FROM metadata.metric_dimension WHERE metric_id = ?",
            [metric_id]
        )
        self.conn.execute(
            "DELETE FROM metadata.metric_filter WHERE metric_id = ?",
            [metric_id]
        )
        self.conn.execute(
            "DELETE FROM metadata.metric_goal WHERE metric_id = ?",
            [metric_id]
        )
        self.conn.execute(
            "DELETE FROM metadata.metric_def WHERE metric_id = ?",
            [metric_id]
        )

        return True

    # ==================== Dimension Management ====================

    def add_dimension(self, dimension: MetricDimension) -> str:
        """Add a dimension to a metric."""
        self.conn.execute(
            """
            INSERT INTO metadata.metric_dimension
            (dimension_id, metric_id, attribute_id, dimension_name, display_name,
             description, role, parent_dimension_id, hierarchy_level, is_default, is_required)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                dimension.dimension_id,
                dimension.metric_id,
                dimension.attribute_id,
                dimension.dimension_name,
                dimension.display_name or dimension.dimension_name,
                dimension.description,
                dimension.role.value,
                dimension.parent_dimension_id,
                dimension.hierarchy_level,
                dimension.is_default,
                dimension.is_required,
            ]
        )

        return dimension.dimension_id

    def get_metric_dimensions(self, metric_id: str) -> List[MetricDimension]:
        """Get all dimensions for a metric."""
        rows = self.conn.execute(
            "SELECT * FROM metadata.metric_dimension WHERE metric_id = ? ORDER BY dimension_name",
            [metric_id]
        ).fetchall()

        return [
            MetricDimension(
                dimension_id=row[0],
                metric_id=row[1],
                attribute_id=row[2],
                dimension_name=row[3],
                display_name=row[4] or row[3],
                description=row[5] or "",
                role=DimensionRole(row[6]) if row[6] else DimensionRole.GROUP_BY,
                parent_dimension_id=row[7],
                hierarchy_level=row[8] or 0,
                is_default=row[9] or False,
                is_required=row[10] or False,
            )
            for row in rows
        ]

    # ==================== Filter Management ====================

    def add_filter(self, filter_def: MetricFilter) -> str:
        """Add a predefined filter to a metric."""
        self.conn.execute(
            """
            INSERT INTO metadata.metric_filter
            (filter_id, metric_id, filter_name, display_name, description, filter_expression, is_default)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                filter_def.filter_id,
                filter_def.metric_id,
                filter_def.filter_name,
                filter_def.display_name or filter_def.filter_name,
                filter_def.description,
                filter_def.filter_expression,
                filter_def.is_default,
            ]
        )

        return filter_def.filter_id

    def get_metric_filters(self, metric_id: str) -> List[MetricFilter]:
        """Get all predefined filters for a metric."""
        rows = self.conn.execute(
            "SELECT * FROM metadata.metric_filter WHERE metric_id = ? ORDER BY filter_name",
            [metric_id]
        ).fetchall()

        return [
            MetricFilter(
                filter_id=row[0],
                metric_id=row[1],
                filter_name=row[2],
                display_name=row[3] or row[2],
                description=row[4] or "",
                filter_expression=row[5] or "",
                is_default=row[6] or False,
            )
            for row in rows
        ]

    # ==================== Query Generation ====================

    def generate_sql(
        self,
        query: MetricQuery,
        dialect: str = "duckdb",
    ) -> str:
        """
        Generate SQL from a semantic metric query.

        Args:
            query: Semantic query specification
            dialect: SQL dialect (duckdb, snowflake, databricks, etc.)

        Returns:
            Generated SQL query
        """
        if not query.metric_ids:
            raise ValueError("At least one metric is required")

        # Get metric definitions
        metrics = [self.get_metric(mid) for mid in query.metric_ids]
        metrics = [m for m in metrics if m]

        if not metrics:
            raise ValueError("No valid metrics found")

        # Build SELECT clause
        select_parts = []
        from_entities = set()
        join_conditions = []

        # Add dimension columns
        for dim_name in query.dimensions:
            # Find the dimension definition
            for metric in metrics:
                dims = self.get_metric_dimensions(metric.metric_id)
                for dim in dims:
                    if dim.dimension_name == dim_name:
                        attr_info = self._get_attribute_info(dim.attribute_id)
                        if attr_info:
                            select_parts.append(f"{attr_info['table_alias']}.{attr_info['column_name']} AS {dim_name}")
                            from_entities.add(attr_info['entity_id'])
                        break

        # Add metric columns
        for metric in metrics:
            metric_sql = self._build_metric_expression(metric)
            select_parts.append(f"{metric_sql} AS {metric.metric_name}")

            # Add entity to FROM
            if metric.entity_id:
                from_entities.add(metric.entity_id)

        # Build FROM clause
        from_clause = self._build_from_clause(list(from_entities))

        # Build WHERE clause
        where_parts = []

        # Add metric filters
        for metric in metrics:
            if metric.filter_expression:
                where_parts.append(f"({metric.filter_expression})")

        # Add query filters
        for field, value in query.filters.items():
            if isinstance(value, list):
                values_str = ", ".join(f"'{v}'" for v in value)
                where_parts.append(f"{field} IN ({values_str})")
            elif isinstance(value, str):
                where_parts.append(f"{field} = '{value}'")
            else:
                where_parts.append(f"{field} = {value}")

        # Add time range filter
        if query.time_range and metrics[0].time_attribute_id:
            time_attr = self._get_attribute_info(metrics[0].time_attribute_id)
            if time_attr:
                time_col = f"{time_attr['table_alias']}.{time_attr['column_name']}"
                if query.time_range.get("start"):
                    where_parts.append(f"{time_col} >= '{query.time_range['start']}'")
                if query.time_range.get("end"):
                    where_parts.append(f"{time_col} < '{query.time_range['end']}'")

        # Build GROUP BY clause
        group_by_parts = []
        for i, dim_name in enumerate(query.dimensions):
            group_by_parts.append(str(i + 1))  # Use column position

        # Assemble query
        sql_parts = [
            "SELECT",
            "    " + ",\n    ".join(select_parts),
            from_clause,
        ]

        if where_parts:
            sql_parts.append("WHERE " + " AND ".join(where_parts))

        if group_by_parts:
            sql_parts.append("GROUP BY " + ", ".join(group_by_parts))

        if query.order_by:
            sql_parts.append("ORDER BY " + ", ".join(query.order_by))

        if query.limit:
            sql_parts.append(f"LIMIT {query.limit}")

        return "\n".join(sql_parts)

    def _build_metric_expression(self, metric: MetricDefinition) -> str:
        """Build SQL expression for a metric."""
        if metric.expression:
            return metric.expression

        if metric.metric_type == MetricType.SIMPLE:
            if metric.attribute_id:
                attr_info = self._get_attribute_info(metric.attribute_id)
                if attr_info:
                    col = f"{attr_info['table_alias']}.{attr_info['column_name']}"
                    return f"{metric.aggregation.value}({col})"

            return f"{metric.aggregation.value}(*)"

        elif metric.metric_type == MetricType.DERIVED:
            # Substitute dependent metrics
            expr = metric.expression
            for dep_id in metric.depends_on_metrics:
                dep_metric = self.get_metric(dep_id)
                if dep_metric:
                    dep_expr = self._build_metric_expression(dep_metric)
                    expr = expr.replace(dep_metric.metric_name, f"({dep_expr})")
            return expr

        return metric.expression or "NULL"

    def _build_from_clause(self, entity_ids: List[str]) -> str:
        """Build FROM clause with necessary joins."""
        if not entity_ids:
            return "FROM dual"

        # Get entity info
        entities = []
        for eid in entity_ids:
            info = self._get_entity_info(eid)
            if info:
                entities.append(info)

        if not entities:
            return "FROM dual"

        # Start with first entity
        from_parts = [f"FROM {entities[0]['schema_name']}.{entities[0]['table_name']} {entities[0]['alias']}"]

        # Add joins for additional entities
        for entity in entities[1:]:
            # Try to find join condition
            join_cond = self._find_join_condition(entities[0]['entity_id'], entity['entity_id'])
            if join_cond:
                from_parts.append(
                    f"JOIN {entity['schema_name']}.{entity['table_name']} {entity['alias']} ON {join_cond}"
                )
            else:
                # Cross join if no relationship found
                from_parts.append(
                    f"CROSS JOIN {entity['schema_name']}.{entity['table_name']} {entity['alias']}"
                )

        return "\n".join(from_parts)

    def _get_attribute_info(self, attribute_id: str) -> Optional[Dict[str, Any]]:
        """Get attribute information."""
        try:
            row = self.conn.execute(
                """
                SELECT a.attribute_id, a.attribute_name, e.entity_id, e.entity_name, e.schema_name
                FROM metadata.attribute a
                JOIN metadata.entity e ON a.entity_id = e.entity_id
                WHERE a.attribute_id = ?
                """,
                [attribute_id]
            ).fetchone()

            if row:
                return {
                    "attribute_id": row[0],
                    "column_name": row[1],
                    "entity_id": row[2],
                    "table_name": row[3],
                    "schema_name": row[4] or "public",
                    "table_alias": row[3][:3].lower(),
                }
        except Exception as e:
            logger.debug(f"Error getting attribute info: {e}")

        return None

    def _get_entity_info(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get entity information."""
        try:
            row = self.conn.execute(
                "SELECT entity_id, entity_name, schema_name FROM metadata.entity WHERE entity_id = ?",
                [entity_id]
            ).fetchone()

            if row:
                return {
                    "entity_id": row[0],
                    "table_name": row[1],
                    "schema_name": row[2] or "public",
                    "alias": row[1][:3].lower(),
                }
        except Exception as e:
            logger.debug(f"Error getting entity info: {e}")

        return None

    def _find_join_condition(self, entity1_id: str, entity2_id: str) -> Optional[str]:
        """Find join condition between two entities."""
        try:
            row = self.conn.execute(
                """
                SELECT from_attribute_id, to_attribute_id
                FROM metadata.relationship
                WHERE (from_entity_id = ? AND to_entity_id = ?)
                   OR (from_entity_id = ? AND to_entity_id = ?)
                LIMIT 1
                """,
                [entity1_id, entity2_id, entity2_id, entity1_id]
            ).fetchone()

            if row:
                from_attr = self._get_attribute_info(row[0])
                to_attr = self._get_attribute_info(row[1])
                if from_attr and to_attr:
                    return f"{from_attr['table_alias']}.{from_attr['column_name']} = {to_attr['table_alias']}.{to_attr['column_name']}"

        except Exception as e:
            logger.debug(f"Error finding join condition: {e}")

        return None

    def execute_query(self, query: MetricQuery) -> MetricQueryResult:
        """
        Execute a semantic metric query.

        Args:
            query: Semantic query specification

        Returns:
            Query results with data
        """
        import time

        start_time = time.time()

        sql = self.generate_sql(query)

        try:
            result = self.conn.execute(sql).fetchall()
            columns = [desc[0] for desc in self.conn.description or []]

            data = [dict(zip(columns, row)) for row in result]

            execution_time = (time.time() - start_time) * 1000

            return MetricQueryResult(
                query=query,
                columns=columns,
                data=data,
                row_count=len(data),
                generated_sql=sql,
                execution_time_ms=execution_time,
            )

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return MetricQueryResult(
                query=query,
                columns=[],
                data=[],
                row_count=0,
                generated_sql=sql,
                execution_time_ms=0,
            )

    # ==================== Period-over-Period (v3.23.0) ====================

    def add_period_comparison(self, comparison: PeriodComparison) -> str:
        """Add a period comparison to a metric."""
        self._ensure_comparison_table()

        self.conn.execute(
            """
            INSERT INTO metadata.metric_period_comparison
            (comparison_id, metric_id, comparison_type, periods_back, label,
             show_absolute, show_percentage)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                comparison.comparison_id,
                comparison.metric_id,
                comparison.comparison_type.value,
                comparison.periods_back,
                comparison.label,
                comparison.show_absolute,
                comparison.show_percentage,
            ]
        )

        return comparison.comparison_id

    def _ensure_comparison_table(self):
        """Ensure period comparison table exists."""
        try:
            self.conn.execute("SELECT 1 FROM metadata.metric_period_comparison LIMIT 1")
        except Exception:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata.metric_period_comparison (
                    comparison_id VARCHAR PRIMARY KEY,
                    metric_id VARCHAR NOT NULL,
                    comparison_type VARCHAR,
                    periods_back INTEGER DEFAULT 1,
                    label VARCHAR,
                    show_absolute BOOLEAN DEFAULT TRUE,
                    show_percentage BOOLEAN DEFAULT TRUE
                )
            """)

    def generate_period_comparison_sql(
        self,
        metric_id: str,
        comparison: PeriodComparison,
        base_query: MetricQuery,
        dialect: str = "duckdb",
    ) -> str:
        """
        Generate SQL for period-over-period comparison.

        Args:
            metric_id: Metric to compare
            comparison: Comparison configuration
            base_query: Base query for current period
            dialect: SQL dialect

        Returns:
            SQL query with comparison columns
        """
        metric = self.get_metric(metric_id)
        if not metric:
            raise ValueError(f"Metric not found: {metric_id}")

        base_sql = self.generate_sql(base_query, dialect)

        # Build comparison expression based on type
        if comparison.comparison_type == PeriodComparisonType.PREVIOUS_PERIOD:
            offset_expr = self._get_time_offset_expression(
                metric.time_grain or TimeGrain.DAY,
                comparison.periods_back,
                dialect,
            )
        elif comparison.comparison_type == PeriodComparisonType.SAME_PERIOD_LAST_YEAR:
            offset_expr = "INTERVAL '1 year'" if dialect == "duckdb" else "INTERVAL 1 YEAR"
        elif comparison.comparison_type == PeriodComparisonType.SAME_PERIOD_LAST_MONTH:
            offset_expr = "INTERVAL '1 month'" if dialect == "duckdb" else "INTERVAL 1 MONTH"
        else:
            offset_expr = "INTERVAL '7 days'" if dialect == "duckdb" else "INTERVAL 7 DAY"

        # Generate comparison query using LAG or self-join
        comparison_sql = f"""
WITH base_data AS (
    {base_sql}
),
comparison_data AS (
    SELECT
        *,
        LAG({metric.metric_name}, {comparison.periods_back}) OVER (ORDER BY time_period) AS {metric.metric_name}_prev
    FROM base_data
)
SELECT
    *,
    {metric.metric_name} - {metric.metric_name}_prev AS {metric.metric_name}_change,
    CASE
        WHEN {metric.metric_name}_prev = 0 THEN NULL
        ELSE ({metric.metric_name} - {metric.metric_name}_prev) / {metric.metric_name}_prev * 100
    END AS {metric.metric_name}_pct_change
FROM comparison_data
"""

        return comparison_sql

    def _get_time_offset_expression(
        self,
        grain: TimeGrain,
        periods: int,
        dialect: str,
    ) -> str:
        """Get time offset expression for a grain."""
        unit_map = {
            TimeGrain.DAY: "day",
            TimeGrain.WEEK: "week",
            TimeGrain.MONTH: "month",
            TimeGrain.QUARTER: "quarter",
            TimeGrain.YEAR: "year",
        }

        unit = unit_map.get(grain, "day")

        if dialect == "duckdb":
            return f"INTERVAL '{periods} {unit}'"
        elif dialect == "snowflake":
            return f"INTERVAL '{periods} {unit.upper()}'"
        else:
            return f"INTERVAL {periods} {unit.upper()}"

    # ==================== Metric Lineage (v3.23.0) ====================

    def add_metric_lineage(self, lineage: MetricLineage) -> str:
        """Track metric lineage/dependency."""
        self._ensure_lineage_table()

        self.conn.execute(
            """
            INSERT INTO metadata.metric_lineage
            (lineage_id, metric_id, source_type, source_id, source_name, transformation, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                lineage.lineage_id,
                lineage.metric_id,
                lineage.source_type,
                lineage.source_id,
                lineage.source_name,
                lineage.transformation,
                lineage.created_at,
            ]
        )

        return lineage.lineage_id

    def _ensure_lineage_table(self):
        """Ensure metric lineage table exists."""
        try:
            self.conn.execute("SELECT 1 FROM metadata.metric_lineage LIMIT 1")
        except Exception:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata.metric_lineage (
                    lineage_id VARCHAR PRIMARY KEY,
                    metric_id VARCHAR NOT NULL,
                    source_type VARCHAR,
                    source_id VARCHAR,
                    source_name VARCHAR,
                    transformation VARCHAR,
                    created_at TIMESTAMP
                )
            """)

    def get_metric_lineage(self, metric_id: str) -> List[MetricLineage]:
        """Get all lineage entries for a metric."""
        self._ensure_lineage_table()

        rows = self.conn.execute(
            "SELECT * FROM metadata.metric_lineage WHERE metric_id = ?",
            [metric_id]
        ).fetchall()

        return [
            MetricLineage(
                lineage_id=row[0],
                metric_id=row[1],
                source_type=row[2] or "",
                source_id=row[3] or "",
                source_name=row[4] or "",
                transformation=row[5] or "",
                created_at=row[6],
            )
            for row in rows
        ]

    def get_dependent_metrics(self, metric_id: str) -> List[MetricDefinition]:
        """Get metrics that depend on this metric."""
        all_metrics = self.list_metrics()
        dependents = []

        for metric in all_metrics:
            if metric_id in metric.depends_on_metrics:
                dependents.append(metric)

        return dependents

    # ==================== Metric Validation (v3.23.0) ====================

    def add_validation(self, validation: MetricValidation) -> str:
        """Add a validation rule to a metric."""
        import json

        self._ensure_validation_table()

        self.conn.execute(
            """
            INSERT INTO metadata.metric_validation
            (validation_id, metric_id, validation_name, validation_type, parameters, severity, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                validation.validation_id,
                validation.metric_id,
                validation.validation_name,
                validation.validation_type,
                json.dumps(validation.parameters),
                validation.severity,
                validation.is_active,
            ]
        )

        return validation.validation_id

    def _ensure_validation_table(self):
        """Ensure validation table exists."""
        try:
            self.conn.execute("SELECT 1 FROM metadata.metric_validation LIMIT 1")
        except Exception:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata.metric_validation (
                    validation_id VARCHAR PRIMARY KEY,
                    metric_id VARCHAR NOT NULL,
                    validation_name VARCHAR,
                    validation_type VARCHAR,
                    parameters VARCHAR,
                    severity VARCHAR DEFAULT 'warning',
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)

    def validate_metric(
        self,
        metric_id: str,
        value: float,
        context: Optional[MetricCalculationContext] = None,
    ) -> List[MetricValidationResult]:
        """
        Validate a metric value against all active validations.

        Args:
            metric_id: Metric to validate
            value: Current metric value
            context: Calculation context

        Returns:
            List of validation results
        """
        import json

        self._ensure_validation_table()

        rows = self.conn.execute(
            "SELECT * FROM metadata.metric_validation WHERE metric_id = ? AND is_active = TRUE",
            [metric_id]
        ).fetchall()

        results = []

        for row in rows:
            validation_id = row[0]
            validation_type = row[3]
            params = json.loads(row[4]) if row[4] else {}
            severity = row[5]

            passed = True
            message = "Validation passed"
            expected_range = None

            if validation_type == "not_null":
                passed = value is not None
                message = "Value is null" if not passed else "Value is not null"

            elif validation_type == "positive":
                passed = value is not None and value >= 0
                message = "Value is negative" if not passed else "Value is non-negative"

            elif validation_type == "range":
                min_val = params.get("min")
                max_val = params.get("max")
                expected_range = (min_val, max_val)

                if min_val is not None and value < min_val:
                    passed = False
                    message = f"Value {value} is below minimum {min_val}"
                elif max_val is not None and value > max_val:
                    passed = False
                    message = f"Value {value} is above maximum {max_val}"
                else:
                    message = f"Value {value} is within range [{min_val}, {max_val}]"

            elif validation_type == "threshold":
                threshold = params.get("threshold", 0)
                comparison = params.get("comparison", "gte")

                if comparison == "gte":
                    passed = value >= threshold
                elif comparison == "lte":
                    passed = value <= threshold
                elif comparison == "gt":
                    passed = value > threshold
                elif comparison == "lt":
                    passed = value < threshold

                message = f"Value {value} {'meets' if passed else 'fails'} threshold {comparison} {threshold}"

            results.append(MetricValidationResult(
                metric_id=metric_id,
                validation_id=validation_id,
                passed=passed,
                message=message,
                actual_value=value,
                expected_range=expected_range,
            ))

        return results

    # ==================== dbt Semantic Layer Export (v3.23.0) ====================

    def export_to_dbt_semantic_layer(
        self,
        metric_ids: Optional[List[str]] = None,
        model_name: str = "semantic_model",
    ) -> str:
        """
        Export metrics to dbt semantic layer YAML format.

        Args:
            metric_ids: Specific metrics to export (None = all)
            model_name: Name for the semantic model

        Returns:
            dbt semantic layer YAML string
        """
        if metric_ids:
            metrics = [self.get_metric(mid) for mid in metric_ids]
            metrics = [m for m in metrics if m]
        else:
            metrics = self.list_metrics(status=MetricStatus.ACTIVE)

        yaml_lines = [
            "version: 2",
            "",
            "semantic_models:",
            f"  - name: {model_name}",
            "    defaults:",
            "      agg_time_dimension: metric_time",
            "",
            "    entities:",
        ]

        # Collect unique entities
        entities = set()
        for metric in metrics:
            if metric.entity_id:
                entities.add(metric.entity_id)

        for entity_id in entities:
            entity_info = self._get_entity_info(entity_id)
            if entity_info:
                yaml_lines.append(f"      - name: {entity_info['table_name']}")
                yaml_lines.append(f"        type: primary")
                yaml_lines.append(f"        expr: {entity_info['table_name']}_id")
                yaml_lines.append("")

        yaml_lines.append("    dimensions:")

        # Collect dimensions from all metrics
        all_dimensions = {}
        for metric in metrics:
            dims = self.get_metric_dimensions(metric.metric_id)
            for dim in dims:
                if dim.dimension_name not in all_dimensions:
                    all_dimensions[dim.dimension_name] = dim

        for dim_name, dim in all_dimensions.items():
            dim_type = "categorical"  # Default
            attr_info = self._get_attribute_info(dim.attribute_id)

            if attr_info:
                yaml_lines.append(f"      - name: {dim_name}")
                yaml_lines.append(f"        type: {dim_type}")
                yaml_lines.append(f"        expr: {attr_info['column_name']}")
                if dim.description:
                    yaml_lines.append(f"        description: {dim.description}")
                yaml_lines.append("")

        yaml_lines.append("    measures:")

        for metric in metrics:
            yaml_lines.append(f"      - name: {metric.metric_name}")
            yaml_lines.append(f"        agg: {metric.aggregation.value.lower()}")

            if metric.attribute_id:
                attr_info = self._get_attribute_info(metric.attribute_id)
                if attr_info:
                    yaml_lines.append(f"        expr: {attr_info['column_name']}")
            elif metric.expression:
                yaml_lines.append(f"        expr: \"{metric.expression}\"")

            if metric.description:
                yaml_lines.append(f"        description: {metric.description}")

            yaml_lines.append("")

        yaml_lines.append("    metrics:")

        for metric in metrics:
            yaml_lines.append(f"      - name: {metric.metric_name}")
            yaml_lines.append(f"        type: {self._map_metric_type_to_dbt(metric.metric_type)}")

            if metric.metric_type == MetricType.SIMPLE:
                yaml_lines.append(f"        type_params:")
                yaml_lines.append(f"          measure: {metric.metric_name}")
            elif metric.metric_type == MetricType.DERIVED:
                yaml_lines.append(f"        type_params:")
                yaml_lines.append(f"          expr: \"{metric.expression}\"")

            if metric.filter_expression:
                yaml_lines.append(f"        filter: \"{metric.filter_expression}\"")

            if metric.description:
                yaml_lines.append(f"        description: {metric.description}")

            yaml_lines.append("")

        return "\n".join(yaml_lines)

    def _map_metric_type_to_dbt(self, metric_type: MetricType) -> str:
        """Map MDDE metric type to dbt metric type."""
        mapping = {
            MetricType.SIMPLE: "simple",
            MetricType.DERIVED: "derived",
            MetricType.RATIO: "ratio",
            MetricType.CUMULATIVE: "cumulative",
        }
        return mapping.get(metric_type, "simple")

    # ==================== Metric Catalog (v3.23.0) ====================

    def get_metric_catalog(
        self,
        domain: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> List[MetricCatalogEntry]:
        """
        Get metric catalog entries for discovery.

        Args:
            domain: Filter by domain
            tags: Filter by tags

        Returns:
            List of catalog entries
        """
        metrics = self.list_metrics(domain=domain)

        entries = []
        for metric in metrics:
            # Filter by tags if specified
            if tags:
                if not any(t in metric.tags for t in tags):
                    continue

            entries.append(MetricCatalogEntry(
                metric_id=metric.metric_id,
                metric_name=metric.metric_name,
                display_name=metric.display_name,
                description=metric.description,
                domain=metric.domain or "",
                category=metric.metric_type.value,
                tags=metric.tags,
                business_owner=metric.business_owner or "",
                status=metric.status,
            ))

        return entries

    def search_metrics(
        self,
        query: str,
        search_descriptions: bool = True,
        search_tags: bool = True,
    ) -> List[MetricCatalogEntry]:
        """
        Search metrics by text query.

        Args:
            query: Search text
            search_descriptions: Include descriptions in search
            search_tags: Include tags in search

        Returns:
            Matching catalog entries
        """
        query_lower = query.lower()
        all_metrics = self.list_metrics()

        matches = []
        for metric in all_metrics:
            score = 0

            # Name match (highest weight)
            if query_lower in metric.metric_name.lower():
                score += 10
            if query_lower in metric.display_name.lower():
                score += 8

            # Description match
            if search_descriptions and metric.description:
                if query_lower in metric.description.lower():
                    score += 5

            # Tag match
            if search_tags:
                for tag in metric.tags:
                    if query_lower in tag.lower():
                        score += 3
                        break

            if score > 0:
                matches.append((score, MetricCatalogEntry(
                    metric_id=metric.metric_id,
                    metric_name=metric.metric_name,
                    display_name=metric.display_name,
                    description=metric.description,
                    domain=metric.domain or "",
                    category=metric.metric_type.value,
                    tags=metric.tags,
                    business_owner=metric.business_owner or "",
                    status=metric.status,
                    popularity_score=score,
                )))

        # Sort by score descending
        matches.sort(key=lambda x: x[0], reverse=True)

        return [entry for _, entry in matches]
