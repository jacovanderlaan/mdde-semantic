"""
Power BI Semantic Exporter (ADR-301).

Exports MDDE semantic models to Power BI formats:
- TMDL (Tabular Model Definition Language) - Power BI's modern format
- DAX measures

Reference: https://learn.microsoft.com/en-us/power-bi/transform-model/desktop-relationship-view
"""

import logging
from typing import Any, Dict, List, Optional
import json

from ..manager import SemanticLayerManager
from ..model import SemanticModel
from ..types import MetricType, AggregationType, DimensionType

logger = logging.getLogger(__name__)


class PowerBISemanticExporter:
    """
    Export MDDE semantic models to Power BI format.

    Generates:
    - TMDL files for semantic model structure
    - DAX measure definitions
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
        Export semantic model to TMDL format.

        Args:
            model_id: Semantic model ID

        Returns:
            TMDL string for Power BI
        """
        model = self.manager.get_model(model_id)
        if not model:
            raise ValueError(f"Semantic model not found: {model_id}")

        return self._generate_tmdl(model)

    def export_dax_measures(self, model_id: str) -> str:
        """
        Export metrics as DAX measure definitions.

        Args:
            model_id: Semantic model ID

        Returns:
            DAX measures as text
        """
        model = self.manager.get_model(model_id)
        if not model:
            raise ValueError(f"Semantic model not found: {model_id}")

        return self._generate_dax_measures(model)

    def export_pbix_dataset(self, model_id: str) -> Dict[str, Any]:
        """
        Export as Power BI dataset JSON structure.

        Args:
            model_id: Semantic model ID

        Returns:
            Dict representing Power BI dataset
        """
        model = self.manager.get_model(model_id)
        if not model:
            raise ValueError(f"Semantic model not found: {model_id}")

        return self._generate_dataset_json(model)

    def _generate_tmdl(self, model: SemanticModel) -> str:
        """Generate Power BI TMDL format."""
        lines = []

        # Model header
        lines.append(f"model Model")
        lines.append(f"  name: {self._escape_tmdl(model.name)}")
        if model.description:
            lines.append(f"  description: {self._escape_tmdl(model.description)}")
        lines.append("")

        # Tables (from unique entities)
        entity_ids = self._get_unique_entities(model)
        for entity_id in entity_ids:
            lines.append(f"  table {self._to_pbi_name(entity_id)}")
            lines.append("")

            # Columns (dimensions linked to this entity)
            for dim in model.dimensions:
                if dim.entity_id == entity_id:
                    lines.append(f"    column {self._to_pbi_name(dim.name)}")
                    lines.append(f"      dataType: {self._map_dimension_data_type(dim.dimension_type)}")
                    if dim.description:
                        lines.append(f"      description: {self._escape_tmdl(dim.description)}")
                    lines.append("")

            # Measures (metrics linked to this entity)
            for metric in model.metrics:
                if metric.entity_id == entity_id:
                    lines.append(f"    measure {self._to_pbi_name(metric.name)}")
                    lines.append(f"      expression: {self._metric_to_dax(metric)}")
                    if metric.description:
                        lines.append(f"      description: {self._escape_tmdl(metric.description)}")
                    if metric.format:
                        lines.append(f"      formatString: {metric.format}")
                    lines.append("")

        # Hierarchies
        for hierarchy in model.hierarchies:
            if hierarchy.levels:
                # Find entity for first level's dimension
                first_level = hierarchy.levels[0]
                entity_id = self._get_dimension_entity(model, first_level.dimension_id)
                if entity_id:
                    lines.append(f"  table {self._to_pbi_name(entity_id)}")
                    lines.append(f"    hierarchy {self._to_pbi_name(hierarchy.name)}")
                    for level in hierarchy.levels:
                        lines.append(f"      level {self._to_pbi_name(level.name)}")
                        lines.append(f"        column: {level.dimension_id}")
                    lines.append("")

        return "\n".join(lines)

    def _generate_dax_measures(self, model: SemanticModel) -> str:
        """Generate DAX measure definitions."""
        lines = []
        lines.append(f"// DAX Measures for {model.name}")
        lines.append(f"// Generated from MDDE Semantic Layer")
        lines.append(f"// Domain: {model.domain or 'N/A'}")
        lines.append("")

        for metric in model.metrics:
            # Comment with metadata
            lines.append(f"// {metric.name}")
            if metric.description:
                lines.append(f"// Description: {metric.description}")
            if metric.certified:
                lines.append("// Status: CERTIFIED")
            if metric.owner:
                lines.append(f"// Owner: {metric.owner}")

            # DAX expression
            dax = self._metric_to_dax(metric)
            lines.append(f"{self._to_pbi_name(metric.name)} = {dax}")
            lines.append("")

        return "\n".join(lines)

    def _generate_dataset_json(self, model: SemanticModel) -> Dict[str, Any]:
        """Generate Power BI dataset JSON structure."""
        dataset = {
            "name": model.name,
            "description": model.description,
            "tables": [],
            "relationships": [],
        }

        # Build tables
        entity_ids = self._get_unique_entities(model)
        for entity_id in entity_ids:
            table = {
                "name": self._to_pbi_name(entity_id),
                "columns": [],
                "measures": [],
                "hierarchies": [],
            }

            # Add columns from dimensions
            for dim in model.dimensions:
                if dim.entity_id == entity_id:
                    column = {
                        "name": self._to_pbi_name(dim.name),
                        "dataType": self._map_dimension_data_type(dim.dimension_type),
                        "description": dim.description,
                    }
                    table["columns"].append(column)

            # Add measures from metrics
            for metric in model.metrics:
                if metric.entity_id == entity_id:
                    measure = {
                        "name": self._to_pbi_name(metric.name),
                        "expression": self._metric_to_dax(metric),
                        "description": metric.description,
                        "formatString": metric.format,
                        "annotations": [],
                    }
                    if metric.certified:
                        measure["annotations"].append({
                            "name": "certified",
                            "value": "true"
                        })
                    if metric.owner:
                        measure["annotations"].append({
                            "name": "owner",
                            "value": metric.owner
                        })
                    table["measures"].append(measure)

            # Add hierarchies
            for hierarchy in model.hierarchies:
                if hierarchy.levels:
                    first_entity = self._get_dimension_entity(model, hierarchy.levels[0].dimension_id)
                    if first_entity == entity_id:
                        hier = {
                            "name": self._to_pbi_name(hierarchy.name),
                            "levels": [
                                {"name": level.name, "column": level.dimension_id}
                                for level in hierarchy.levels
                            ]
                        }
                        table["hierarchies"].append(hier)

            dataset["tables"].append(table)

        return dataset

    def _metric_to_dax(self, metric) -> str:
        """Convert metric to DAX expression."""
        if metric.metric_type == MetricType.SIMPLE:
            agg_func = self._map_aggregation_to_dax(metric.aggregation)
            column = f"'{metric.entity_id}'[{metric.attribute_id or metric.expression}]"
            return f"{agg_func}({column})"

        elif metric.metric_type == MetricType.DERIVED:
            # Derived metrics use the expression directly
            return metric.expression

        elif metric.metric_type == MetricType.RATIO:
            # Ratio: assume expression contains numerator/denominator
            return f"DIVIDE({metric.expression}, 0)"

        elif metric.metric_type == MetricType.CUMULATIVE:
            agg_func = self._map_aggregation_to_dax(metric.aggregation)
            column = f"'{metric.entity_id}'[{metric.attribute_id or metric.expression}]"
            return f"CALCULATE({agg_func}({column}), FILTER(ALL(Dates), Dates[Date] <= MAX(Dates[Date])))"

        else:
            return metric.expression or "0"

    def _map_aggregation_to_dax(self, agg: Optional[AggregationType]) -> str:
        """Map aggregation type to DAX function."""
        if not agg:
            return "SUM"
        mapping = {
            AggregationType.SUM: "SUM",
            AggregationType.COUNT: "COUNT",
            AggregationType.COUNT_DISTINCT: "DISTINCTCOUNT",
            AggregationType.AVG: "AVERAGE",
            AggregationType.MIN: "MIN",
            AggregationType.MAX: "MAX",
            AggregationType.MEDIAN: "MEDIAN",
            AggregationType.PERCENTILE: "PERCENTILEX.INC",
        }
        return mapping.get(agg, "SUM")

    def _map_dimension_data_type(self, dim_type: DimensionType) -> str:
        """Map dimension type to Power BI data type."""
        mapping = {
            DimensionType.CATEGORICAL: "string",
            DimensionType.TIME: "dateTime",
            DimensionType.GEOGRAPHIC: "string",
            DimensionType.NUMERIC: "double",
        }
        return mapping.get(dim_type, "string")

    def _get_unique_entities(self, model: SemanticModel) -> List[str]:
        """Get unique entity IDs from model."""
        entities = set()
        for metric in model.metrics:
            entities.add(metric.entity_id)
        for dim in model.dimensions:
            if dim.entity_id:
                entities.add(dim.entity_id)
        return sorted(list(entities))

    def _get_dimension_entity(self, model: SemanticModel, dimension_id: str) -> Optional[str]:
        """Get entity ID for a dimension."""
        for dim in model.dimensions:
            if dim.dimension_id == dimension_id:
                return dim.entity_id
        return None

    def _to_pbi_name(self, name: str) -> str:
        """Convert to Power BI compatible name."""
        # Replace invalid characters
        import re
        name = re.sub(r'[^\w\s]', '', name)
        return name.replace(' ', '_')

    def _escape_tmdl(self, text: str) -> str:
        """Escape text for TMDL format."""
        if not text:
            return '""'
        # Simple escape - wrap in quotes if contains special chars
        if any(c in text for c in ['"', '\n', ':']):
            return f'"{text}"'
        return text
