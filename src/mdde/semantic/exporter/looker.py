"""
Looker LookML Exporter (ADR-301).

Exports MDDE semantic models to Looker LookML format.
Generates explores, views, measures, and dimensions.

Reference: https://cloud.google.com/looker/docs/lookml-overview
"""

import logging
from typing import Any, Dict, List, Optional

from ..manager import SemanticLayerManager
from ..model import SemanticModel
from ..types import MetricType, AggregationType, DimensionType, TimeGrain

logger = logging.getLogger(__name__)


class LookerSemanticExporter:
    """
    Export MDDE semantic models to Looker LookML format.

    Generates:
    - Explore definitions
    - View files with dimensions and measures
    """

    def __init__(self, conn):
        """
        Initialize exporter.

        Args:
            conn: Database connection
        """
        self.conn = conn
        self.manager = SemanticLayerManager(conn)

    def export_model(self, model_id: str) -> str:
        """
        Export semantic model to LookML format.

        Args:
            model_id: Semantic model ID

        Returns:
            LookML string
        """
        model = self.manager.get_model(model_id)
        if not model:
            raise ValueError(f"Semantic model not found: {model_id}")

        return self._generate_lookml(model)

    def export_explore(self, model_id: str) -> str:
        """
        Export just the explore definition.

        Args:
            model_id: Semantic model ID

        Returns:
            LookML explore block
        """
        model = self.manager.get_model(model_id)
        if not model:
            raise ValueError(f"Semantic model not found: {model_id}")

        return self._generate_explore(model)

    def export_views(self, model_id: str) -> Dict[str, str]:
        """
        Export view definitions (one per entity).

        Args:
            model_id: Semantic model ID

        Returns:
            Dict mapping view name to LookML content
        """
        model = self.manager.get_model(model_id)
        if not model:
            raise ValueError(f"Semantic model not found: {model_id}")

        return self._generate_views(model)

    def _generate_lookml(self, model: SemanticModel) -> str:
        """Generate complete LookML file."""
        lines = []

        # Header comment
        lines.append(f"# LookML generated from MDDE Semantic Model: {model.name}")
        lines.append(f"# Domain: {model.domain or 'N/A'}")
        lines.append(f"# Owner: {model.owner or 'N/A'}")
        lines.append(f"# Version: {model.version}")
        lines.append("")

        # Include views
        entity_ids = self._get_unique_entities(model)
        for entity_id in entity_ids:
            view_name = self._to_lookml_name(entity_id)
            lines.append(f'include: "/views/{view_name}.view.lkml"')
        lines.append("")

        # Explore definition
        lines.append(self._generate_explore(model))
        lines.append("")

        # View definitions
        views = self._generate_views(model)
        for view_name, view_content in views.items():
            lines.append(view_content)
            lines.append("")

        return "\n".join(lines)

    def _generate_explore(self, model: SemanticModel) -> str:
        """Generate LookML explore block."""
        lines = []
        explore_name = self._to_lookml_name(model.name)

        lines.append(f"explore: {explore_name} {{")

        if model.description:
            lines.append(f'  description: "{self._escape_lookml(model.description)}"')

        # Label
        lines.append(f'  label: "{model.name}"')

        # Primary view
        entity_ids = self._get_unique_entities(model)
        if entity_ids:
            primary_view = self._to_lookml_name(entity_ids[0])
            lines.append(f"  from: {primary_view}")

            # Joins for additional views
            for entity_id in entity_ids[1:]:
                view_name = self._to_lookml_name(entity_id)
                lines.append("")
                lines.append(f"  join: {view_name} {{")
                lines.append(f"    type: left_outer")
                lines.append(f"    relationship: many_to_one")
                lines.append(f"    sql_on: ${{primary_view}}.{view_name}_id = ${{{view_name}}}.id ;;")
                lines.append("  }")

        lines.append("}")

        return "\n".join(lines)

    def _generate_views(self, model: SemanticModel) -> Dict[str, str]:
        """Generate LookML view definitions."""
        views = {}
        entity_ids = self._get_unique_entities(model)

        for entity_id in entity_ids:
            view_name = self._to_lookml_name(entity_id)
            view_lines = []

            view_lines.append(f"view: {view_name} {{")
            view_lines.append(f'  sql_table_name: ${{schema}}.{entity_id} ;;')
            view_lines.append("")

            # Dimensions from this entity
            for dim in model.dimensions:
                if dim.entity_id == entity_id:
                    view_lines.extend(self._generate_dimension(dim))
                    view_lines.append("")

            # Measures from metrics for this entity
            for metric in model.metrics:
                if metric.entity_id == entity_id:
                    view_lines.extend(self._generate_measure(metric))
                    view_lines.append("")

            view_lines.append("}")

            views[view_name] = "\n".join(view_lines)

        return views

    def _generate_dimension(self, dim) -> List[str]:
        """Generate LookML dimension block."""
        lines = []
        dim_name = self._to_lookml_name(dim.name)

        # Determine dimension type
        if dim.dimension_type == DimensionType.TIME:
            lines.append(f"  dimension_group: {dim_name} {{")
            lines.append(f"    type: time")
            lines.append(f"    timeframes: [raw, date, week, month, quarter, year]")
            lines.append(f"    sql: ${{TABLE}}.{dim.attribute_id or dim.name} ;;")
        else:
            lines.append(f"  dimension: {dim_name} {{")
            lines.append(f"    type: {self._map_dimension_type(dim.dimension_type)}")
            lines.append(f"    sql: ${{TABLE}}.{dim.attribute_id or dim.name} ;;")

        if dim.description:
            lines.append(f'    description: "{self._escape_lookml(dim.description)}"')

        if dim.label:
            lines.append(f'    label: "{dim.label}"')

        # Geographic type
        if dim.dimension_type == DimensionType.GEOGRAPHIC and dim.geo_type:
            lines.append(f"    map_layer_name: {dim.geo_type}")

        lines.append("  }")

        return lines

    def _generate_measure(self, metric) -> List[str]:
        """Generate LookML measure block."""
        lines = []
        measure_name = self._to_lookml_name(metric.name)

        lines.append(f"  measure: {measure_name} {{")

        # Type based on metric type
        if metric.metric_type == MetricType.SIMPLE:
            lines.append(f"    type: {self._map_aggregation_to_lookml(metric.aggregation)}")
            sql_field = metric.attribute_id or metric.expression
            lines.append(f"    sql: ${{TABLE}}.{sql_field} ;;")

        elif metric.metric_type == MetricType.DERIVED:
            lines.append(f"    type: number")
            lines.append(f"    sql: {metric.expression} ;;")

        elif metric.metric_type == MetricType.RATIO:
            lines.append(f"    type: number")
            lines.append(f"    sql: {metric.expression} ;;")
            lines.append(f"    value_format_name: percent_2")

        elif metric.metric_type == MetricType.CUMULATIVE:
            lines.append(f"    type: running_total")
            sql_field = metric.attribute_id or metric.expression
            lines.append(f"    sql: ${{TABLE}}.{sql_field} ;;")

        elif metric.metric_type == MetricType.CONVERSION:
            lines.append(f"    type: number")
            lines.append(f"    sql: {metric.expression} ;;")
            lines.append(f"    value_format_name: percent_2")

        else:
            lines.append(f"    type: number")
            lines.append(f"    sql: {metric.expression or '0'} ;;")

        # Description
        if metric.description:
            lines.append(f'    description: "{self._escape_lookml(metric.description)}"')

        # Label
        lines.append(f'    label: "{metric.name}"')

        # Format
        if metric.format:
            lines.append(f'    value_format: "{metric.format}"')
        elif metric.unit == "$":
            lines.append(f"    value_format_name: usd")
        elif metric.unit == "%":
            lines.append(f"    value_format_name: percent_2")

        # Drill fields (from related dimensions)
        drill_fields = []
        for dim in self._get_dimensions_for_entity(metric.entity_id):
            drill_fields.append(self._to_lookml_name(dim.name))
        if drill_fields:
            lines.append(f"    drill_fields: [{', '.join(drill_fields)}]")

        lines.append("  }")

        return lines

    def _get_unique_entities(self, model: SemanticModel) -> List[str]:
        """Get unique entity IDs from model."""
        entities = set()
        for metric in model.metrics:
            entities.add(metric.entity_id)
        for dim in model.dimensions:
            if dim.entity_id:
                entities.add(dim.entity_id)
        return sorted(list(entities))

    def _get_dimensions_for_entity(self, entity_id: str) -> List:
        """Get dimensions for a specific entity (placeholder)."""
        # This would need access to the model to work properly
        return []

    def _map_dimension_type(self, dim_type: DimensionType) -> str:
        """Map dimension type to LookML type."""
        mapping = {
            DimensionType.CATEGORICAL: "string",
            DimensionType.TIME: "time",
            DimensionType.GEOGRAPHIC: "string",
            DimensionType.NUMERIC: "number",
        }
        return mapping.get(dim_type, "string")

    def _map_aggregation_to_lookml(self, agg: Optional[AggregationType]) -> str:
        """Map aggregation type to LookML measure type."""
        if not agg:
            return "sum"
        mapping = {
            AggregationType.SUM: "sum",
            AggregationType.COUNT: "count",
            AggregationType.COUNT_DISTINCT: "count_distinct",
            AggregationType.AVG: "average",
            AggregationType.MIN: "min",
            AggregationType.MAX: "max",
            AggregationType.MEDIAN: "median",
            AggregationType.PERCENTILE: "percentile",
        }
        return mapping.get(agg, "sum")

    def _to_lookml_name(self, name: str) -> str:
        """Convert to LookML-compatible name."""
        import re
        # Replace spaces and hyphens with underscores
        name = re.sub(r'[\s\-]+', '_', name)
        # Remove special characters
        name = re.sub(r'[^\w]', '', name)
        return name.lower()

    def _escape_lookml(self, text: str) -> str:
        """Escape text for LookML strings."""
        if not text:
            return ""
        return text.replace('"', '\\"').replace('\n', ' ')
