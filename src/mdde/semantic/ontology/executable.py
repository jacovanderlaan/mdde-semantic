"""
SQL-Executable Ontology (ADR-364).

Extends MDDE's ontology with execution capabilities.
Metrics and relationships are both semantic AND executable.

Key insight from Timbr.ai:
"The shift isn't from semantic layer to ontology.
It's from description to execution."

Feb 2026
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import re

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC time."""
    return datetime.now(timezone.utc)


class RelationshipType(Enum):
    """Semantic relationship types."""
    HAS = "has"  # Customer HAS orders
    BELONGS_TO = "belongs_to"  # Order BELONGS_TO customer
    REFERENCES = "references"  # Invoice REFERENCES order
    AGGREGATES = "aggregates"  # Summary AGGREGATES detail
    INHERITS = "inherits"  # Enterprise Customer INHERITS Customer
    ASSOCIATES = "associates"  # Product ASSOCIATES with Category


class JoinType(Enum):
    """SQL join types for relationships."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class Cardinality(Enum):
    """Relationship cardinality."""
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"
    MANY_TO_MANY = "N:M"


class FilterCompositionMode(Enum):
    """How to compose multiple filters."""
    AND = "AND"
    OR = "OR"


@dataclass
class ComposedFilter:
    """
    A composable filter expression.

    Supports combining multiple filter conditions with AND/OR logic.
    """
    name: str
    sql_expression: str
    description: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)

    def compose_with(
        self,
        other: "ComposedFilter",
        mode: FilterCompositionMode = FilterCompositionMode.AND,
    ) -> "ComposedFilter":
        """Compose this filter with another."""
        combined_sql = f"({self.sql_expression}) {mode.value} ({other.sql_expression})"
        combined_params = {**self.parameters, **other.parameters}
        return ComposedFilter(
            name=f"{self.name}_{mode.value.lower()}_{other.name}",
            sql_expression=combined_sql,
            description=f"{self.description} {mode.value} {other.description}",
            parameters=combined_params,
        )

    def negate(self) -> "ComposedFilter":
        """Return negation of this filter."""
        return ComposedFilter(
            name=f"not_{self.name}",
            sql_expression=f"NOT ({self.sql_expression})",
            description=f"NOT ({self.description})",
            parameters=self.parameters,
        )


@dataclass
class OptimizationHint:
    """
    Hints for query optimization.

    Guides the query planner for better performance.
    """
    hint_type: str  # e.g., "index", "partition", "broadcast", "materialize"
    target: str  # entity/table this applies to
    parameters: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0  # Higher = more important

    def to_sql_comment(self, dialect: str = "ansi") -> str:
        """Generate SQL comment hint."""
        if dialect == "databricks":
            if self.hint_type == "broadcast":
                return f"/*+ BROADCAST({self.target}) */"
            elif self.hint_type == "repartition":
                cols = self.parameters.get("columns", [])
                return f"/*+ REPARTITION({', '.join(cols)}) */"
        elif dialect == "snowflake":
            if self.hint_type == "cluster":
                cols = self.parameters.get("columns", [])
                return f"-- CLUSTER BY ({', '.join(cols)})"
        return f"-- Hint: {self.hint_type} on {self.target}"


@dataclass
class QueryContext:
    """
    Context for query generation.

    Provides runtime information for metric execution.
    """
    time_range: Optional[Tuple[datetime, datetime]] = None
    filters: Dict[str, Any] = field(default_factory=dict)
    dimensions: List[str] = field(default_factory=list)
    grain: Optional[str] = None  # day, week, month
    limit: Optional[int] = None
    dialect: str = "ansi"  # SQL dialect


@dataclass
class ExecutableMetric:
    """
    A metric that is both defined AND executable.

    Captures both the semantic meaning and the SQL implementation.

    Supports inheritance: child metrics can extend parent metrics,
    inheriting dimensions, filters, and applying transformations.
    """
    metric_id: str
    name: str
    definition: str  # Human description

    # Execution
    sql_expression: str  # The actual SQL aggregation
    grain_entity_id: str  # What entity this aggregates over (e.g., "fact_orders")

    # Semantic context
    compatible_dimensions: List[str] = field(default_factory=list)
    named_filters: Dict[str, str] = field(default_factory=dict)  # name -> SQL expression

    # Inheritance
    parent_metric_id: Optional[str] = None  # For metric hierarchies
    inherit_filters: bool = True  # Inherit parent's named_filters
    inherit_dimensions: bool = True  # Inherit parent's compatible_dimensions
    transformation: Optional[str] = None  # SQL transformation to apply to parent, e.g., "* 100" for percentage

    # Governance
    owner: Optional[str] = None
    certified: bool = False
    tags: List[str] = field(default_factory=list)

    # Optimization hints
    optimization_hints: List["OptimizationHint"] = field(default_factory=list)
    preferred_aggregation_strategy: Optional[str] = None  # "hash", "sort", "partial"
    estimated_cardinality: Optional[int] = None  # For query planning

    # Metadata
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)

    def resolve_inheritance(
        self,
        parent: Optional["ExecutableMetric"],
    ) -> "ExecutableMetric":
        """
        Resolve inheritance from parent metric.

        Creates a new metric with inherited properties merged.

        Args:
            parent: Parent metric to inherit from

        Returns:
            New metric with inherited properties
        """
        if not parent:
            return self

        # Start with self's values
        resolved = ExecutableMetric(
            metric_id=self.metric_id,
            name=self.name,
            definition=self.definition,
            sql_expression=self.sql_expression,
            grain_entity_id=self.grain_entity_id or parent.grain_entity_id,
            owner=self.owner or parent.owner,
            certified=self.certified,
            tags=list(set(self.tags + parent.tags)),
        )

        # Inherit dimensions if enabled
        if self.inherit_dimensions:
            inherited_dims = set(parent.compatible_dimensions)
            inherited_dims.update(self.compatible_dimensions)
            resolved.compatible_dimensions = list(inherited_dims)
        else:
            resolved.compatible_dimensions = list(self.compatible_dimensions)

        # Inherit filters if enabled
        if self.inherit_filters:
            resolved.named_filters = {**parent.named_filters, **self.named_filters}
        else:
            resolved.named_filters = dict(self.named_filters)

        # Apply transformation to SQL expression
        if self.transformation:
            resolved.sql_expression = f"({parent.sql_expression}) {self.transformation}"

        # Merge optimization hints
        resolved.optimization_hints = parent.optimization_hints + self.optimization_hints

        return resolved

    def to_sql(
        self,
        context: Optional[QueryContext] = None,
        alias: Optional[str] = None,
    ) -> str:
        """
        Generate SQL expression for this metric.

        Args:
            context: Query context for filters/dimensions
            alias: Optional column alias

        Returns:
            SQL expression string
        """
        expr = self.sql_expression

        # Apply named filters from context
        if context and context.filters:
            for filter_name, filter_value in context.filters.items():
                if filter_name in self.named_filters:
                    filter_sql = self.named_filters[filter_name]
                    # Simple placeholder replacement
                    filter_sql = filter_sql.replace("{value}", str(filter_value))
                    expr = f"CASE WHEN {filter_sql} THEN {expr} END"

        if alias:
            return f"{expr} AS {alias}"
        return expr

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "metric_id": self.metric_id,
            "name": self.name,
            "definition": self.definition,
            "sql_expression": self.sql_expression,
            "grain_entity_id": self.grain_entity_id,
            "compatible_dimensions": self.compatible_dimensions,
            "named_filters": self.named_filters,
            "parent_metric_id": self.parent_metric_id,
            "inherit_filters": self.inherit_filters,
            "inherit_dimensions": self.inherit_dimensions,
            "transformation": self.transformation,
            "owner": self.owner,
            "certified": self.certified,
            "tags": self.tags,
            "preferred_aggregation_strategy": self.preferred_aggregation_strategy,
            "estimated_cardinality": self.estimated_cardinality,
        }
        if self.optimization_hints:
            result["optimization_hints"] = [
                {"hint_type": h.hint_type, "target": h.target, "parameters": h.parameters, "priority": h.priority}
                for h in self.optimization_hints
            ]
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutableMetric":
        """Create from dictionary."""
        hints = []
        for h in data.get("optimization_hints", []):
            hints.append(OptimizationHint(
                hint_type=h["hint_type"],
                target=h["target"],
                parameters=h.get("parameters", {}),
                priority=h.get("priority", 0),
            ))

        return cls(
            metric_id=data["metric_id"],
            name=data["name"],
            definition=data.get("definition", ""),
            sql_expression=data["sql_expression"],
            grain_entity_id=data.get("grain_entity_id", ""),
            compatible_dimensions=data.get("compatible_dimensions", []),
            named_filters=data.get("named_filters", {}),
            parent_metric_id=data.get("parent_metric_id"),
            inherit_filters=data.get("inherit_filters", True),
            inherit_dimensions=data.get("inherit_dimensions", True),
            transformation=data.get("transformation"),
            owner=data.get("owner"),
            certified=data.get("certified", False),
            tags=data.get("tags", []),
            optimization_hints=hints,
            preferred_aggregation_strategy=data.get("preferred_aggregation_strategy"),
            estimated_cardinality=data.get("estimated_cardinality"),
        )


@dataclass
class SemanticRelationship:
    """
    A relationship that is both semantic AND queryable.

    Captures both the business meaning and SQL implementation.
    """
    relationship_id: str
    source_entity_id: str
    target_entity_id: str

    # Semantic
    relationship_type: RelationshipType
    cardinality: Cardinality
    name: str = ""  # Human-readable name
    description: str = ""

    # Execution
    join_type: JoinType = JoinType.LEFT
    join_condition: str = ""  # The actual JOIN ON expression
    traversal_cost: float = 1.0  # For query optimization (lower = cheaper)

    # Metadata
    created_at: datetime = field(default_factory=_utc_now)

    def to_join_clause(
        self,
        source_alias: Optional[str] = None,
        target_alias: Optional[str] = None,
    ) -> str:
        """
        Generate SQL JOIN clause.

        Args:
            source_alias: Optional alias for source table
            target_alias: Optional alias for target table

        Returns:
            SQL JOIN clause
        """
        condition = self.join_condition

        # Replace table references with aliases if provided
        if source_alias:
            condition = condition.replace(
                f"{self.source_entity_id}.",
                f"{source_alias}."
            )
        if target_alias:
            condition = condition.replace(
                f"{self.target_entity_id}.",
                f"{target_alias}."
            )

        target = target_alias or self.target_entity_id
        return f"{self.join_type.value} {target} ON {condition}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "relationship_id": self.relationship_id,
            "source_entity_id": self.source_entity_id,
            "target_entity_id": self.target_entity_id,
            "relationship_type": self.relationship_type.value,
            "cardinality": self.cardinality.value,
            "name": self.name,
            "description": self.description,
            "join_type": self.join_type.value,
            "join_condition": self.join_condition,
            "traversal_cost": self.traversal_cost,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SemanticRelationship":
        """Create from dictionary."""
        return cls(
            relationship_id=data["relationship_id"],
            source_entity_id=data["source_entity_id"],
            target_entity_id=data["target_entity_id"],
            relationship_type=RelationshipType(data["relationship_type"]),
            cardinality=Cardinality(data["cardinality"]),
            name=data.get("name", ""),
            description=data.get("description", ""),
            join_type=JoinType(data.get("join_type", "LEFT JOIN")),
            join_condition=data.get("join_condition", ""),
            traversal_cost=data.get("traversal_cost", 1.0),
        )


@dataclass
class ExecutableQuery:
    """
    Result of translating a query through the ontology.

    Contains both the executable SQL and the semantic context.
    """
    sql: str
    metrics_used: List[str] = field(default_factory=list)
    dimensions_used: List[str] = field(default_factory=list)
    relationships_traversed: List[str] = field(default_factory=list)
    filters_applied: List[str] = field(default_factory=list)
    explanation: str = ""  # Human-readable explanation


class ExecutableOntology:
    """
    SQL-Executable Ontology.

    Combines semantic meaning with execution capability.
    """

    def __init__(self):
        """Initialize ontology."""
        self._metrics: Dict[str, ExecutableMetric] = {}
        self._relationships: Dict[str, SemanticRelationship] = {}
        self._entities: Dict[str, Dict[str, Any]] = {}  # entity_id -> entity info
        self._dimension_mappings: Dict[str, str] = {}  # dimension name -> entity.column

    def add_metric(self, metric: ExecutableMetric) -> None:
        """Add an executable metric."""
        self._metrics[metric.metric_id] = metric

    def add_relationship(self, relationship: SemanticRelationship) -> None:
        """Add a semantic relationship."""
        self._relationships[relationship.relationship_id] = relationship

    def register_entity(
        self,
        entity_id: str,
        table_name: str,
        primary_key: str,
        schema: Optional[str] = None,
    ) -> None:
        """Register an entity with its physical location."""
        self._entities[entity_id] = {
            "table_name": table_name,
            "primary_key": primary_key,
            "schema": schema,
            "qualified_name": f"{schema}.{table_name}" if schema else table_name,
        }

    def register_dimension(
        self,
        dimension_name: str,
        entity_id: str,
        column_name: str,
    ) -> None:
        """Register a dimension mapping."""
        self._dimension_mappings[dimension_name] = f"{entity_id}.{column_name}"

    def get_metric(self, metric_id: str) -> Optional[ExecutableMetric]:
        """Get a metric by ID."""
        return self._metrics.get(metric_id)

    def get_resolved_metric(self, metric_id: str) -> Optional[ExecutableMetric]:
        """
        Get a metric with inheritance resolved.

        Traverses the inheritance chain and returns a fully resolved metric
        with all inherited properties merged.
        """
        metric = self._metrics.get(metric_id)
        if not metric:
            return None

        if not metric.parent_metric_id:
            return metric

        # Resolve parent first (recursive)
        parent = self.get_resolved_metric(metric.parent_metric_id)
        if not parent:
            logger.warning(f"Parent metric not found: {metric.parent_metric_id}")
            return metric

        return metric.resolve_inheritance(parent)

    def get_relationship(self, rel_id: str) -> Optional[SemanticRelationship]:
        """Get a relationship by ID."""
        return self._relationships.get(rel_id)

    def create_composed_filter(
        self,
        filter_names: List[str],
        mode: FilterCompositionMode = FilterCompositionMode.AND,
    ) -> Optional[ComposedFilter]:
        """
        Create a composed filter from multiple named filters.

        Searches all metrics for the named filters and composes them.

        Args:
            filter_names: List of filter names to compose
            mode: How to combine filters (AND/OR)

        Returns:
            ComposedFilter or None if no filters found
        """
        filters: List[ComposedFilter] = []

        for name in filter_names:
            for metric in self._metrics.values():
                if name in metric.named_filters:
                    filters.append(ComposedFilter(
                        name=name,
                        sql_expression=metric.named_filters[name],
                    ))
                    break

        if not filters:
            return None

        result = filters[0]
        for f in filters[1:]:
            result = result.compose_with(f, mode)

        return result

    def get_optimization_hints(
        self,
        metric_id: str,
        dialect: str = "ansi",
    ) -> List[str]:
        """
        Get SQL optimization hints for a metric.

        Returns dialect-specific SQL hint comments.
        """
        metric = self.get_resolved_metric(metric_id)
        if not metric:
            return []

        return [h.to_sql_comment(dialect) for h in metric.optimization_hints]

    def find_path(
        self,
        from_entity: str,
        to_entity: str,
        max_hops: int = 5,
    ) -> List[SemanticRelationship]:
        """
        Find a path between two entities through relationships.

        Uses BFS to find shortest path, preferring lower traversal cost.
        """
        if from_entity == to_entity:
            return []

        # Build adjacency list
        edges: Dict[str, List[Tuple[str, SemanticRelationship]]] = {}
        for rel in self._relationships.values():
            if rel.source_entity_id not in edges:
                edges[rel.source_entity_id] = []
            edges[rel.source_entity_id].append((rel.target_entity_id, rel))

            # Also add reverse direction for bidirectional traversal
            if rel.target_entity_id not in edges:
                edges[rel.target_entity_id] = []
            edges[rel.target_entity_id].append((rel.source_entity_id, rel))

        # BFS
        from collections import deque
        queue: deque = deque([(from_entity, [])])
        visited: Set[str] = {from_entity}

        while queue:
            current, path = queue.popleft()

            if len(path) >= max_hops:
                continue

            for neighbor, rel in edges.get(current, []):
                if neighbor == to_entity:
                    return path + [rel]
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [rel]))

        return []  # No path found

    def traverse(
        self,
        start_entity: str,
        relationship_path: List[str],
    ) -> str:
        """
        Generate SQL JOINs for relationship traversal.

        Args:
            start_entity: Starting entity
            relationship_path: List of relationship IDs to traverse

        Returns:
            SQL JOIN clauses
        """
        joins = []
        current_entity = start_entity
        alias_counter = 0

        for rel_id in relationship_path:
            rel = self._relationships.get(rel_id)
            if not rel:
                logger.warning(f"Relationship not found: {rel_id}")
                continue

            # Determine direction
            if rel.source_entity_id == current_entity:
                target = rel.target_entity_id
            else:
                target = rel.source_entity_id

            alias_counter += 1
            alias = f"t{alias_counter}"

            join_clause = rel.to_join_clause(
                source_alias=f"t{alias_counter-1}" if alias_counter > 1 else None,
                target_alias=alias,
            )
            joins.append(join_clause)
            current_entity = target

        return "\n".join(joins)

    def execute_metric(
        self,
        metric_id: str,
        dimensions: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        context: Optional[QueryContext] = None,
    ) -> ExecutableQuery:
        """
        Generate executable SQL for a metric with dimensions.

        Args:
            metric_id: Metric to execute
            dimensions: Dimensions to group by
            filters: Filters to apply
            context: Additional query context

        Returns:
            ExecutableQuery with SQL and metadata
        """
        metric = self._metrics.get(metric_id)
        if not metric:
            raise ValueError(f"Metric not found: {metric_id}")

        ctx = context or QueryContext()
        if filters:
            ctx.filters.update(filters)
        if dimensions:
            ctx.dimensions.extend(dimensions)

        # Build query
        select_parts = []
        join_parts = []
        group_by_parts = []
        from_clause = ""

        # Get base entity
        base_entity = self._entities.get(metric.grain_entity_id, {})
        from_clause = base_entity.get("qualified_name", metric.grain_entity_id)

        # Add dimensions
        for dim in ctx.dimensions:
            if dim in metric.compatible_dimensions:
                dim_ref = self._dimension_mappings.get(dim, dim)
                select_parts.append(dim_ref)
                group_by_parts.append(dim_ref)

                # Find and add JOINs for dimensions from other entities
                entity_id = dim_ref.split('.')[0] if '.' in dim_ref else None
                if entity_id and entity_id != metric.grain_entity_id:
                    path = self.find_path(metric.grain_entity_id, entity_id)
                    for rel in path:
                        join_parts.append(rel.to_join_clause())

        # Add metric
        metric_sql = metric.to_sql(ctx, alias=metric.name)
        select_parts.append(metric_sql)

        # Build SQL
        sql_parts = ["SELECT"]
        sql_parts.append("    " + ",\n    ".join(select_parts))
        sql_parts.append(f"FROM {from_clause}")

        if join_parts:
            sql_parts.extend(join_parts)

        if group_by_parts:
            sql_parts.append("GROUP BY " + ", ".join(group_by_parts))

        if ctx.limit:
            sql_parts.append(f"LIMIT {ctx.limit}")

        sql = "\n".join(sql_parts)

        return ExecutableQuery(
            sql=sql,
            metrics_used=[metric_id],
            dimensions_used=ctx.dimensions,
            relationships_traversed=[r.relationship_id for r in path] if 'path' in dir() else [],
            explanation=f"Metric '{metric.name}' aggregated over {metric.grain_entity_id}",
        )

    def to_dict(self) -> Dict[str, Any]:
        """Export ontology to dictionary."""
        return {
            "metrics": {k: v.to_dict() for k, v in self._metrics.items()},
            "relationships": {k: v.to_dict() for k, v in self._relationships.items()},
            "entities": self._entities,
            "dimension_mappings": self._dimension_mappings,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutableOntology":
        """Create ontology from dictionary."""
        ontology = cls()

        for metric_data in data.get("metrics", {}).values():
            ontology.add_metric(ExecutableMetric.from_dict(metric_data))

        for rel_data in data.get("relationships", {}).values():
            ontology.add_relationship(SemanticRelationship.from_dict(rel_data))

        ontology._entities = data.get("entities", {})
        ontology._dimension_mappings = data.get("dimension_mappings", {})

        return ontology


class OntologyQuery:
    """
    Query the ontology using semantic terms.

    Translates business questions into executable SQL.
    """

    def __init__(self, ontology: ExecutableOntology):
        """Initialize with ontology."""
        self.ontology = ontology

    def query(
        self,
        metrics: List[str],
        dimensions: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> ExecutableQuery:
        """
        Execute a semantic query.

        Args:
            metrics: List of metric IDs
            dimensions: Optional dimensions to group by
            filters: Optional filters

        Returns:
            ExecutableQuery with SQL
        """
        if len(metrics) == 1:
            return self.ontology.execute_metric(
                metrics[0],
                dimensions=dimensions,
                filters=filters,
            )

        # Multiple metrics - combine them
        context = QueryContext(
            dimensions=dimensions or [],
            filters=filters or {},
        )

        select_parts = []
        all_joins = []

        for metric_id in metrics:
            metric = self.ontology.get_metric(metric_id)
            if metric:
                select_parts.append(metric.to_sql(context, alias=metric.name))

        # Build combined query
        # (simplified - would need proper join logic)
        sql = f"""SELECT
    {', '.join(select_parts)}
FROM combined_base
"""

        return ExecutableQuery(
            sql=sql,
            metrics_used=metrics,
            dimensions_used=dimensions or [],
        )

    def natural_language_to_sql(
        self,
        question: str,
    ) -> Optional[ExecutableQuery]:
        """
        Convert natural language to SQL using ontology.

        Note: This is a simplified implementation.
        Full NL→SQL would require LLM integration.

        Args:
            question: Natural language question

        Returns:
            ExecutableQuery or None if not parseable
        """
        question_lower = question.lower()

        # Simple pattern matching
        metrics_found = []
        dimensions_found = []

        for metric_id, metric in self.ontology._metrics.items():
            if metric.name.lower() in question_lower:
                metrics_found.append(metric_id)

        for dim in self.ontology._dimension_mappings.keys():
            if dim.lower() in question_lower:
                dimensions_found.append(dim)

        if metrics_found:
            return self.query(
                metrics=metrics_found,
                dimensions=dimensions_found if dimensions_found else None,
            )

        return None
