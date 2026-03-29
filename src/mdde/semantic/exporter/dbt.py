"""
dbt Semantic Layer Exporter (ADR-301).

Exports MDDE semantic models to dbt Semantic Layer YAML format (MetricFlow).
Generates semantic_models and metrics definitions compatible with dbt v1.6+.

Reference: https://docs.getdbt.com/docs/build/semantic-models
"""

import logging
from typing import Any, Dict, List, Optional
import yaml

from ..manager import SemanticLayerManager
from ..model import SemanticModel
from ..types import MetricType, AggregationType, DimensionType, TimeGrain

logger = logging.getLogger(__name__)


class DbtSemanticExporter:
    """
    Export MDDE semantic models to dbt Semantic Layer format.

    Generates:
    - semantic_models/*.yml - Semantic model definitions
    - metrics/*.yml - Metric definitions
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
        Export a semantic model to dbt YAML format.

        Args:
            model_id: Semantic model ID

        Returns:
            YAML string for dbt semantic_models
        """
        model = self.manager.get_model(model_id)
        if not model:
            raise ValueError(f"Semantic model not found: {model_id}")

        return self._generate_semantic_model_yaml(model)

    def export_metrics(self, model_id: str) -> str:
        """
        Export metrics to dbt metrics YAML format.

        Args:
            model_id: Semantic model ID

        Returns:
            YAML string for dbt metrics
        """
        model = self.manager.get_model(model_id)
        if not model:
            raise ValueError(f"Semantic model not found: {model_id}")

        return self._generate_metrics_yaml(model)

    def export_all(self, model_id: str) -> Dict[str, str]:
        """
        Export both semantic model and metrics.

        Args:
            model_id: Semantic model ID

        Returns:
            Dict with 'semantic_model' and 'metrics' YAML strings
        """
        return {
            "semantic_model": self.export_model(model_id),
            "metrics": self.export_metrics(model_id),
        }

    def _generate_semantic_model_yaml(self, model: SemanticModel) -> str:
        """Generate dbt semantic_models YAML."""
        semantic_model = {
            "name": self._to_snake_case(model.name),
            "description": model.description or f"Semantic model: {model.name}",
            "model": f"ref('{self._get_primary_entity_name(model)}')",
        }

        # Default time dimension
        if model.default_time_dimension:
            semantic_model["defaults"] = {
                "agg_time_dimension": model.default_time_dimension
            }

        # Entities (primary and foreign keys)
        entities = self._generate_entities(model)
        if entities:
            semantic_model["entities"] = entities

        # Measures (from metrics)
        measures = self._generate_measures(model)
        if measures:
            semantic_model["measures"] = measures

        # Dimensions
        dimensions = self._generate_dimensions(model)
        if dimensions:
            semantic_model["dimensions"] = dimensions

        output = {"semantic_models": [semantic_model]}
        return yaml.dump(output, default_flow_style=False, sort_keys=False, allow_unicode=True)

    def _generate_metrics_yaml(self, model: SemanticModel) -> str:
        """Generate dbt metrics YAML."""
        metrics = []

        for metric in model.metrics:
            dbt_metric = {
                "name": self._to_snake_case(metric.name),
                "description": metric.description,
                "type": self._map_metric_type(metric.metric_type),
            }

            # Type-specific params
            if metric.metric_type == MetricType.SIMPLE:
                dbt_metric["type_params"] = {
                    "measure": self._to_snake_case(metric.name) + "_measure"
                }
            elif metric.metric_type == MetricType.DERIVED:
                dbt_metric["type_params"] = {
                    "expr": metric.expression
                }
            elif metric.metric_type == MetricType.RATIO:
                dbt_metric["type_params"] = {
                    "numerator": {"name": "numerator_metric"},
                    "denominator": {"name": "denominator_metric"},
                }

            # Filters
            if metric.filters:
                dbt_metric["filter"] = " AND ".join(
                    f.expression for f in metric.filters
                )

            # Label and meta
            dbt_metric["label"] = metric.name
            meta = {}
            if metric.owner:
                meta["owner"] = metric.owner
            if metric.certified:
                meta["certified"] = True
            if metric.unit:
                meta["unit"] = metric.unit
            if metric.tags:
                meta["tags"] = metric.tags
            if meta:
                dbt_metric["meta"] = meta

            metrics.append(dbt_metric)

        output = {"metrics": metrics}
        return yaml.dump(output, default_flow_style=False, sort_keys=False, allow_unicode=True)

    def _generate_entities(self, model: SemanticModel) -> List[Dict[str, Any]]:
        """Generate dbt entities (primary/foreign keys)."""
        entities = []

        # Get unique entity IDs from metrics and dimensions
        entity_ids = set()
        for metric in model.metrics:
            entity_ids.add(metric.entity_id)
        for dim in model.dimensions:
            if dim.entity_id:
                entity_ids.add(dim.entity_id)

        primary_set = False
        for entity_id in sorted(entity_ids):
            entity = {
                "name": self._to_snake_case(entity_id),
                "type": "primary" if not primary_set else "foreign",
            }
            if not primary_set:
                primary_set = True
            entities.append(entity)

        return entities

    def _generate_measures(self, model: SemanticModel) -> List[Dict[str, Any]]:
        """Generate dbt measures from metrics."""
        measures = []

        for metric in model.metrics:
            if metric.metric_type != MetricType.SIMPLE:
                continue

            measure = {
                "name": self._to_snake_case(metric.name) + "_measure",
                "agg": self._map_aggregation(metric.aggregation),
                "expr": metric.expression if metric.expression else metric.attribute_id,
            }

            if metric.description:
                measure["description"] = metric.description

            # Create metric from measure
            measure["create_metric"] = True

            measures.append(measure)

        return measures

    def _generate_dimensions(self, model: SemanticModel) -> List[Dict[str, Any]]:
        """Generate dbt dimensions."""
        dimensions = []

        for dim in model.dimensions:
            dbt_dim = {
                "name": self._to_snake_case(dim.name),
                "type": self._map_dimension_type(dim.dimension_type),
            }

            if dim.description:
                dbt_dim["description"] = dim.description

            # Expression (column reference)
            dbt_dim["expr"] = dim.attribute_id or dim.name

            # Time dimension specifics
            if dim.dimension_type == DimensionType.TIME:
                dbt_dim["type_params"] = {
                    "time_granularity": dim.time_granularity.value if dim.time_granularity else "day"
                }

            # Label
            if dim.label:
                dbt_dim["label"] = dim.label

            dimensions.append(dbt_dim)

        return dimensions

    def _get_primary_entity_name(self, model: SemanticModel) -> str:
        """Get primary entity name for model ref."""
        if model.metrics:
            return self._to_snake_case(model.metrics[0].entity_id)
        return self._to_snake_case(model.name)

    def _map_metric_type(self, metric_type: MetricType) -> str:
        """Map MDDE metric type to dbt metric type."""
        mapping = {
            MetricType.SIMPLE: "simple",
            MetricType.DERIVED: "derived",
            MetricType.CUMULATIVE: "cumulative",
            MetricType.RATIO: "ratio",
            MetricType.CONVERSION: "conversion",
        }
        return mapping.get(metric_type, "simple")

    def _map_aggregation(self, agg: Optional[AggregationType]) -> str:
        """Map MDDE aggregation to dbt aggregation."""
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

    def _map_dimension_type(self, dim_type: DimensionType) -> str:
        """Map MDDE dimension type to dbt dimension type."""
        mapping = {
            DimensionType.CATEGORICAL: "categorical",
            DimensionType.TIME: "time",
            DimensionType.GEOGRAPHIC: "categorical",  # dbt doesn't have geo type
            DimensionType.NUMERIC: "categorical",
        }
        return mapping.get(dim_type, "categorical")

    def _to_snake_case(self, name: str) -> str:
        """Convert name to snake_case."""
        import re
        # Replace spaces and hyphens with underscores
        name = re.sub(r'[\s\-]+', '_', name)
        # Insert underscore before uppercase letters
        name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
        return name.lower()
