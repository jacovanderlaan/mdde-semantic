"""
Semantic Layer Manager (ADR-301).

CRUD operations for semantic models, metrics, dimensions, and hierarchies.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .types import (
    Metric,
    Dimension,
    Hierarchy,
    HierarchyLevel,
    MetricFilter,
    MetricType,
    AggregationType,
    DimensionType,
    HierarchyType,
    TimeGrain,
    generate_id,
)
from .model import SemanticModel

logger = logging.getLogger(__name__)


class SemanticLayerManager:
    """
    Manager for semantic layer operations.

    Provides CRUD for semantic models, metrics, dimensions, and hierarchies.
    Implements Patrick Okare's vision of the semantic layer as the "translator"
    between raw data and business insights.
    """

    def __init__(self, conn):
        """
        Initialize manager.

        Args:
            conn: Database connection (DuckDB)
        """
        self.conn = conn
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Ensure semantic layer tables exist."""
        # Semantic models
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_model (
                model_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                description VARCHAR,
                owner VARCHAR,
                domain VARCHAR,
                default_time_dimension VARCHAR,
                default_granularity VARCHAR,
                version VARCHAR DEFAULT '1.0.0',
                mdde_model_id VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Metrics
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_metric (
                metric_id VARCHAR PRIMARY KEY,
                semantic_model_id VARCHAR REFERENCES semantic_model(model_id),
                name VARCHAR NOT NULL,
                description VARCHAR,
                metric_type VARCHAR NOT NULL,
                expression VARCHAR NOT NULL,
                entity_id VARCHAR,
                attribute_id VARCHAR,
                aggregation VARCHAR,
                unit VARCHAR,
                format VARCHAR,
                owner VARCHAR,
                certified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Metric filters
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_metric_filter (
                filter_id VARCHAR PRIMARY KEY,
                metric_id VARCHAR REFERENCES semantic_metric(metric_id),
                expression VARCHAR NOT NULL,
                description VARCHAR
            )
        """)

        # Metric time grains
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_metric_time_grain (
                metric_id VARCHAR,
                time_grain VARCHAR NOT NULL,
                PRIMARY KEY (metric_id, time_grain)
            )
        """)

        # Metric tags
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_metric_tag (
                metric_id VARCHAR,
                tag VARCHAR NOT NULL,
                PRIMARY KEY (metric_id, tag)
            )
        """)

        # Dimensions
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_dimension (
                dimension_id VARCHAR PRIMARY KEY,
                semantic_model_id VARCHAR REFERENCES semantic_model(model_id),
                name VARCHAR NOT NULL,
                description VARCHAR,
                entity_id VARCHAR,
                attribute_id VARCHAR,
                dimension_type VARCHAR NOT NULL,
                time_granularity VARCHAR,
                geo_type VARCHAR,
                label VARCHAR,
                format VARCHAR
            )
        """)

        # Dimension allowed values
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_dimension_value (
                dimension_id VARCHAR,
                value VARCHAR NOT NULL,
                PRIMARY KEY (dimension_id, value)
            )
        """)

        # Hierarchies
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_hierarchy (
                hierarchy_id VARCHAR PRIMARY KEY,
                semantic_model_id VARCHAR REFERENCES semantic_model(model_id),
                name VARCHAR NOT NULL,
                description VARCHAR,
                hierarchy_type VARCHAR
            )
        """)

        # Hierarchy levels
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_hierarchy_level (
                level_id VARCHAR PRIMARY KEY,
                hierarchy_id VARCHAR REFERENCES semantic_hierarchy(hierarchy_id),
                name VARCHAR NOT NULL,
                dimension_id VARCHAR,
                level_order INTEGER NOT NULL
            )
        """)

        # Metric dependencies (for derived metrics)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS semantic_metric_dependency (
                parent_metric_id VARCHAR,
                child_metric_id VARCHAR,
                PRIMARY KEY (parent_metric_id, child_metric_id)
            )
        """)

    # --- Semantic Model CRUD ---

    def create_model(
        self,
        name: str,
        description: Optional[str] = None,
        owner: Optional[str] = None,
        domain: Optional[str] = None,
        default_time_dimension: Optional[str] = None,
        default_granularity: Optional[str] = None,
        mdde_model_id: Optional[str] = None,
    ) -> SemanticModel:
        """
        Create a new semantic model.

        Args:
            name: Model name
            description: Model description
            owner: Owner (team/person)
            domain: Business domain (sales, finance, etc.)
            default_time_dimension: Default time dimension ID
            default_granularity: Default time grain (day, week, month)
            mdde_model_id: Link to MDDE model

        Returns:
            Created SemanticModel
        """
        model = SemanticModel(
            name=name,
            description=description,
            owner=owner,
            domain=domain,
            default_time_dimension=default_time_dimension,
            default_granularity=TimeGrain(default_granularity) if default_granularity else None,
            mdde_model_id=mdde_model_id,
        )

        self.conn.execute(
            """
            INSERT INTO semantic_model
            (model_id, name, description, owner, domain, default_time_dimension,
             default_granularity, version, mdde_model_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                model.model_id,
                model.name,
                model.description,
                model.owner,
                model.domain,
                model.default_time_dimension,
                model.default_granularity.value if model.default_granularity else None,
                model.version,
                model.mdde_model_id,
                model.created_at,
                model.updated_at,
            ],
        )

        logger.info(f"Created semantic model: {model.name} ({model.model_id})")
        return model

    def get_model(self, model_id: str) -> Optional[SemanticModel]:
        """Get a semantic model by ID with all components."""
        result = self.conn.execute(
            "SELECT * FROM semantic_model WHERE model_id = ?",
            [model_id],
        ).fetchone()

        if not result:
            return None

        model = SemanticModel(
            model_id=result[0],
            name=result[1],
            description=result[2],
            owner=result[3],
            domain=result[4],
            default_time_dimension=result[5],
            default_granularity=TimeGrain(result[6]) if result[6] else None,
            version=result[7],
            mdde_model_id=result[8],
            created_at=result[9],
            updated_at=result[10],
        )

        # Load metrics
        model.metrics = self._load_metrics(model_id)

        # Load dimensions
        model.dimensions = self._load_dimensions(model_id)

        # Load hierarchies
        model.hierarchies = self._load_hierarchies(model_id)

        return model

    def list_models(
        self,
        domain: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List semantic models with optional filtering."""
        query = "SELECT model_id, name, description, domain, owner, version, created_at FROM semantic_model WHERE 1=1"
        params = []

        if domain:
            query += " AND domain = ?"
            params.append(domain)
        if owner:
            query += " AND owner = ?"
            params.append(owner)

        query += " ORDER BY name"

        results = self.conn.execute(query, params).fetchall()
        return [
            {
                "model_id": r[0],
                "name": r[1],
                "description": r[2],
                "domain": r[3],
                "owner": r[4],
                "version": r[5],
                "created_at": r[6],
            }
            for r in results
        ]

    def update_model(
        self,
        model_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        owner: Optional[str] = None,
        domain: Optional[str] = None,
    ) -> bool:
        """Update semantic model properties."""
        updates = []
        params = []

        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if description is not None:
            updates.append("description = ?")
            params.append(description)
        if owner is not None:
            updates.append("owner = ?")
            params.append(owner)
        if domain is not None:
            updates.append("domain = ?")
            params.append(domain)

        if not updates:
            return False

        updates.append("updated_at = ?")
        params.append(datetime.now(timezone.utc))
        params.append(model_id)

        self.conn.execute(
            f"UPDATE semantic_model SET {', '.join(updates)} WHERE model_id = ?",
            params,
        )
        return True

    def delete_model(self, model_id: str) -> bool:
        """Delete a semantic model and all its components."""
        # Delete in order due to foreign keys
        self.conn.execute(
            "DELETE FROM semantic_hierarchy_level WHERE hierarchy_id IN (SELECT hierarchy_id FROM semantic_hierarchy WHERE semantic_model_id = ?)",
            [model_id],
        )
        self.conn.execute(
            "DELETE FROM semantic_hierarchy WHERE semantic_model_id = ?",
            [model_id],
        )
        self.conn.execute(
            "DELETE FROM semantic_dimension_value WHERE dimension_id IN (SELECT dimension_id FROM semantic_dimension WHERE semantic_model_id = ?)",
            [model_id],
        )
        self.conn.execute(
            "DELETE FROM semantic_dimension WHERE semantic_model_id = ?",
            [model_id],
        )
        self.conn.execute(
            "DELETE FROM semantic_metric_tag WHERE metric_id IN (SELECT metric_id FROM semantic_metric WHERE semantic_model_id = ?)",
            [model_id],
        )
        self.conn.execute(
            "DELETE FROM semantic_metric_time_grain WHERE metric_id IN (SELECT metric_id FROM semantic_metric WHERE semantic_model_id = ?)",
            [model_id],
        )
        self.conn.execute(
            "DELETE FROM semantic_metric_filter WHERE metric_id IN (SELECT metric_id FROM semantic_metric WHERE semantic_model_id = ?)",
            [model_id],
        )
        self.conn.execute(
            "DELETE FROM semantic_metric_dependency WHERE parent_metric_id IN (SELECT metric_id FROM semantic_metric WHERE semantic_model_id = ?) OR child_metric_id IN (SELECT metric_id FROM semantic_metric WHERE semantic_model_id = ?)",
            [model_id, model_id],
        )
        self.conn.execute(
            "DELETE FROM semantic_metric WHERE semantic_model_id = ?",
            [model_id],
        )
        self.conn.execute(
            "DELETE FROM semantic_model WHERE model_id = ?",
            [model_id],
        )

        logger.info(f"Deleted semantic model: {model_id}")
        return True

    # --- Metric CRUD ---

    def add_metric(
        self,
        model_id: str,
        name: str,
        description: str,
        metric_type: str,
        expression: str,
        entity_id: str,
        attribute_id: Optional[str] = None,
        aggregation: Optional[str] = None,
        unit: Optional[str] = None,
        format: Optional[str] = None,
        owner: Optional[str] = None,
        certified: bool = False,
        time_grains: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        filters: Optional[List[Dict[str, str]]] = None,
    ) -> Metric:
        """Add a metric to a semantic model."""
        metric_id = generate_id("met")

        metric = Metric(
            metric_id=metric_id,
            name=name,
            description=description,
            metric_type=MetricType(metric_type),
            expression=expression,
            entity_id=entity_id,
            attribute_id=attribute_id,
            aggregation=AggregationType(aggregation) if aggregation else None,
            unit=unit,
            format=format,
            owner=owner,
            certified=certified,
            time_grains=[TimeGrain(g) for g in (time_grains or [])],
            tags=tags or [],
            semantic_model_id=model_id,
        )

        # Insert metric
        self.conn.execute(
            """
            INSERT INTO semantic_metric
            (metric_id, semantic_model_id, name, description, metric_type, expression,
             entity_id, attribute_id, aggregation, unit, format, owner, certified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                metric.metric_id,
                model_id,
                metric.name,
                metric.description,
                metric.metric_type.value,
                metric.expression,
                metric.entity_id,
                metric.attribute_id,
                metric.aggregation.value if metric.aggregation else None,
                metric.unit,
                metric.format,
                metric.owner,
                metric.certified,
            ],
        )

        # Insert time grains
        for grain in metric.time_grains:
            self.conn.execute(
                "INSERT INTO semantic_metric_time_grain (metric_id, time_grain) VALUES (?, ?)",
                [metric_id, grain.value],
            )

        # Insert tags
        for tag in metric.tags:
            self.conn.execute(
                "INSERT INTO semantic_metric_tag (metric_id, tag) VALUES (?, ?)",
                [metric_id, tag],
            )

        # Insert filters
        if filters:
            for f in filters:
                filter_id = generate_id("flt")
                self.conn.execute(
                    "INSERT INTO semantic_metric_filter (filter_id, metric_id, expression, description) VALUES (?, ?, ?, ?)",
                    [filter_id, metric_id, f.get("expression"), f.get("description")],
                )
                metric.filters.append(MetricFilter(
                    filter_id=filter_id,
                    expression=f.get("expression", ""),
                    description=f.get("description"),
                ))

        logger.info(f"Added metric: {metric.name} to model {model_id}")
        return metric

    def _load_metrics(self, model_id: str) -> List[Metric]:
        """Load all metrics for a model."""
        results = self.conn.execute(
            "SELECT * FROM semantic_metric WHERE semantic_model_id = ?",
            [model_id],
        ).fetchall()

        metrics = []
        for r in results:
            metric_id = r[0]

            # Load time grains
            time_grains = self.conn.execute(
                "SELECT time_grain FROM semantic_metric_time_grain WHERE metric_id = ?",
                [metric_id],
            ).fetchall()

            # Load tags
            tags = self.conn.execute(
                "SELECT tag FROM semantic_metric_tag WHERE metric_id = ?",
                [metric_id],
            ).fetchall()

            # Load filters
            filters_result = self.conn.execute(
                "SELECT filter_id, expression, description FROM semantic_metric_filter WHERE metric_id = ?",
                [metric_id],
            ).fetchall()

            metric = Metric(
                metric_id=r[0],
                semantic_model_id=r[1],
                name=r[2],
                description=r[3] or "",
                metric_type=MetricType(r[4]),
                expression=r[5],
                entity_id=r[6] or "",
                attribute_id=r[7],
                aggregation=AggregationType(r[8]) if r[8] else None,
                unit=r[9],
                format=r[10],
                owner=r[11],
                certified=r[12] or False,
                time_grains=[TimeGrain(g[0]) for g in time_grains],
                tags=[t[0] for t in tags],
                filters=[MetricFilter(f[0], f[1], f[2]) for f in filters_result],
            )
            metrics.append(metric)

        return metrics

    # --- Dimension CRUD ---

    def add_dimension(
        self,
        model_id: str,
        name: str,
        entity_id: str,
        attribute_id: str,
        dimension_type: str,
        description: Optional[str] = None,
        time_granularity: Optional[str] = None,
        geo_type: Optional[str] = None,
        label: Optional[str] = None,
        format: Optional[str] = None,
        allowed_values: Optional[List[str]] = None,
    ) -> Dimension:
        """Add a dimension to a semantic model."""
        dimension_id = generate_id("dim")

        dimension = Dimension(
            dimension_id=dimension_id,
            name=name,
            description=description,
            entity_id=entity_id,
            attribute_id=attribute_id,
            dimension_type=DimensionType(dimension_type),
            time_granularity=TimeGrain(time_granularity) if time_granularity else None,
            geo_type=geo_type,
            label=label,
            format=format,
            allowed_values=allowed_values,
            semantic_model_id=model_id,
        )

        self.conn.execute(
            """
            INSERT INTO semantic_dimension
            (dimension_id, semantic_model_id, name, description, entity_id, attribute_id,
             dimension_type, time_granularity, geo_type, label, format)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                dimension.dimension_id,
                model_id,
                dimension.name,
                dimension.description,
                dimension.entity_id,
                dimension.attribute_id,
                dimension.dimension_type.value,
                dimension.time_granularity.value if dimension.time_granularity else None,
                dimension.geo_type,
                dimension.label,
                dimension.format,
            ],
        )

        # Insert allowed values
        if allowed_values:
            for val in allowed_values:
                self.conn.execute(
                    "INSERT INTO semantic_dimension_value (dimension_id, value) VALUES (?, ?)",
                    [dimension_id, val],
                )

        logger.info(f"Added dimension: {dimension.name} to model {model_id}")
        return dimension

    def _load_dimensions(self, model_id: str) -> List[Dimension]:
        """Load all dimensions for a model."""
        results = self.conn.execute(
            "SELECT * FROM semantic_dimension WHERE semantic_model_id = ?",
            [model_id],
        ).fetchall()

        dimensions = []
        for r in results:
            dimension_id = r[0]

            # Load allowed values
            values = self.conn.execute(
                "SELECT value FROM semantic_dimension_value WHERE dimension_id = ?",
                [dimension_id],
            ).fetchall()

            dimension = Dimension(
                dimension_id=r[0],
                semantic_model_id=r[1],
                name=r[2],
                description=r[3],
                entity_id=r[4] or "",
                attribute_id=r[5] or "",
                dimension_type=DimensionType(r[6]),
                time_granularity=TimeGrain(r[7]) if r[7] else None,
                geo_type=r[8],
                label=r[9],
                format=r[10],
                allowed_values=[v[0] for v in values] if values else None,
            )
            dimensions.append(dimension)

        return dimensions

    # --- Hierarchy CRUD ---

    def add_hierarchy(
        self,
        model_id: str,
        name: str,
        hierarchy_type: str,
        levels: List[Dict[str, Any]],
        description: Optional[str] = None,
    ) -> Hierarchy:
        """Add a hierarchy to a semantic model."""
        hierarchy_id = generate_id("hier")

        hierarchy = Hierarchy(
            hierarchy_id=hierarchy_id,
            name=name,
            description=description,
            hierarchy_type=HierarchyType(hierarchy_type),
            semantic_model_id=model_id,
        )

        self.conn.execute(
            """
            INSERT INTO semantic_hierarchy
            (hierarchy_id, semantic_model_id, name, description, hierarchy_type)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                hierarchy.hierarchy_id,
                model_id,
                hierarchy.name,
                hierarchy.description,
                hierarchy.hierarchy_type.value,
            ],
        )

        # Insert levels
        for i, level_data in enumerate(levels):
            level_id = generate_id("lvl")
            level = HierarchyLevel(
                level_id=level_id,
                name=level_data["name"],
                dimension_id=level_data.get("dimension_id", ""),
                order=i,
            )
            hierarchy.levels.append(level)

            self.conn.execute(
                """
                INSERT INTO semantic_hierarchy_level
                (level_id, hierarchy_id, name, dimension_id, level_order)
                VALUES (?, ?, ?, ?, ?)
                """,
                [level.level_id, hierarchy_id, level.name, level.dimension_id, level.order],
            )

        logger.info(f"Added hierarchy: {hierarchy.name} to model {model_id}")
        return hierarchy

    def _load_hierarchies(self, model_id: str) -> List[Hierarchy]:
        """Load all hierarchies for a model."""
        results = self.conn.execute(
            "SELECT * FROM semantic_hierarchy WHERE semantic_model_id = ?",
            [model_id],
        ).fetchall()

        hierarchies = []
        for r in results:
            hierarchy_id = r[0]

            # Load levels
            levels_result = self.conn.execute(
                "SELECT * FROM semantic_hierarchy_level WHERE hierarchy_id = ? ORDER BY level_order",
                [hierarchy_id],
            ).fetchall()

            levels = [
                HierarchyLevel(
                    level_id=l[0],
                    name=l[2],
                    dimension_id=l[3] or "",
                    order=l[4],
                )
                for l in levels_result
            ]

            hierarchy = Hierarchy(
                hierarchy_id=r[0],
                semantic_model_id=r[1],
                name=r[2],
                description=r[3],
                hierarchy_type=HierarchyType(r[4]) if r[4] else HierarchyType.CUSTOM,
                levels=levels,
            )
            hierarchies.append(hierarchy)

        return hierarchies

    # --- Utility Methods ---

    def get_metrics_by_entity(self, entity_id: str) -> List[Dict[str, Any]]:
        """Get all metrics that reference a specific entity."""
        results = self.conn.execute(
            """
            SELECT m.metric_id, m.name, m.description, m.metric_type, m.certified,
                   sm.model_id, sm.name as model_name
            FROM semantic_metric m
            JOIN semantic_model sm ON m.semantic_model_id = sm.model_id
            WHERE m.entity_id = ?
            """,
            [entity_id],
        ).fetchall()

        return [
            {
                "metric_id": r[0],
                "name": r[1],
                "description": r[2],
                "metric_type": r[3],
                "certified": r[4],
                "model_id": r[5],
                "model_name": r[6],
            }
            for r in results
        ]

    def get_certified_metrics(self, model_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all certified metrics, optionally filtered by model."""
        query = """
            SELECT m.metric_id, m.name, m.description, m.owner,
                   sm.model_id, sm.name as model_name, sm.domain
            FROM semantic_metric m
            JOIN semantic_model sm ON m.semantic_model_id = sm.model_id
            WHERE m.certified = TRUE
        """
        params = []

        if model_id:
            query += " AND sm.model_id = ?"
            params.append(model_id)

        query += " ORDER BY sm.name, m.name"

        results = self.conn.execute(query, params).fetchall()

        return [
            {
                "metric_id": r[0],
                "name": r[1],
                "description": r[2],
                "owner": r[3],
                "model_id": r[4],
                "model_name": r[5],
                "domain": r[6],
            }
            for r in results
        ]
