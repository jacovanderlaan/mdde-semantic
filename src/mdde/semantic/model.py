"""
Semantic Model (ADR-301).

The SemanticModel class represents a complete semantic layer definition,
containing metrics, dimensions, and hierarchies.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import json
import yaml

from .types import (
    Metric,
    Dimension,
    Hierarchy,
    TimeGrain,
    generate_id,
)


@dataclass
class SemanticModel:
    """
    A semantic layer definition.

    A SemanticModel is analogous to:
    - dbt semantic_model
    - Looker Explore
    - Power BI Semantic Model
    - Cube.js cube

    It provides a consistent, unified view of business metrics across
    the organization - the "translator" between raw data and business insights.
    """

    # Identity
    model_id: str = field(default_factory=lambda: generate_id("sem"))
    name: str = ""
    description: Optional[str] = None

    # Ownership
    owner: Optional[str] = None
    domain: Optional[str] = None  # e.g., "sales", "finance", "marketing"

    # Components
    metrics: List[Metric] = field(default_factory=list)
    dimensions: List[Dimension] = field(default_factory=list)
    hierarchies: List[Hierarchy] = field(default_factory=list)

    # Configuration
    default_time_dimension: Optional[str] = None
    default_granularity: Optional[TimeGrain] = None

    # Version
    version: str = "1.0.0"

    # Link to MDDE model
    mdde_model_id: Optional[str] = None

    # Timestamps
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self):
        """Convert string enums to enum types."""
        if isinstance(self.default_granularity, str):
            self.default_granularity = TimeGrain(self.default_granularity)

    # --- Metric operations ---

    def add_metric(self, metric: Metric) -> None:
        """Add a metric to the model."""
        metric.semantic_model_id = self.model_id
        self.metrics.append(metric)
        self.updated_at = datetime.now(timezone.utc)

    def get_metric(self, metric_id: str) -> Optional[Metric]:
        """Get a metric by ID."""
        for m in self.metrics:
            if m.metric_id == metric_id:
                return m
        return None

    def get_metric_by_name(self, name: str) -> Optional[Metric]:
        """Get a metric by name."""
        for m in self.metrics:
            if m.name == name:
                return m
        return None

    def remove_metric(self, metric_id: str) -> bool:
        """Remove a metric by ID."""
        for i, m in enumerate(self.metrics):
            if m.metric_id == metric_id:
                self.metrics.pop(i)
                self.updated_at = datetime.now(timezone.utc)
                return True
        return False

    # --- Dimension operations ---

    def add_dimension(self, dimension: Dimension) -> None:
        """Add a dimension to the model."""
        dimension.semantic_model_id = self.model_id
        self.dimensions.append(dimension)
        self.updated_at = datetime.now(timezone.utc)

    def get_dimension(self, dimension_id: str) -> Optional[Dimension]:
        """Get a dimension by ID."""
        for d in self.dimensions:
            if d.dimension_id == dimension_id:
                return d
        return None

    def get_dimension_by_name(self, name: str) -> Optional[Dimension]:
        """Get a dimension by name."""
        for d in self.dimensions:
            if d.name == name:
                return d
        return None

    def remove_dimension(self, dimension_id: str) -> bool:
        """Remove a dimension by ID."""
        for i, d in enumerate(self.dimensions):
            if d.dimension_id == dimension_id:
                self.dimensions.pop(i)
                self.updated_at = datetime.now(timezone.utc)
                return True
        return False

    # --- Hierarchy operations ---

    def add_hierarchy(self, hierarchy: Hierarchy) -> None:
        """Add a hierarchy to the model."""
        hierarchy.semantic_model_id = self.model_id
        self.hierarchies.append(hierarchy)
        self.updated_at = datetime.now(timezone.utc)

    def get_hierarchy(self, hierarchy_id: str) -> Optional[Hierarchy]:
        """Get a hierarchy by ID."""
        for h in self.hierarchies:
            if h.hierarchy_id == hierarchy_id:
                return h
        return None

    def remove_hierarchy(self, hierarchy_id: str) -> bool:
        """Remove a hierarchy by ID."""
        for i, h in enumerate(self.hierarchies):
            if h.hierarchy_id == hierarchy_id:
                self.hierarchies.pop(i)
                self.updated_at = datetime.now(timezone.utc)
                return True
        return False

    # --- Statistics ---

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the semantic model."""
        certified_metrics = [m for m in self.metrics if m.certified]
        time_dimensions = [d for d in self.dimensions if d.dimension_type.value == "time"]

        return {
            "total_metrics": len(self.metrics),
            "certified_metrics": len(certified_metrics),
            "total_dimensions": len(self.dimensions),
            "time_dimensions": len(time_dimensions),
            "total_hierarchies": len(self.hierarchies),
            "unique_entities": len(self._get_unique_entities()),
            "tags": self._get_all_tags(),
        }

    def _get_unique_entities(self) -> List[str]:
        """Get list of unique entity IDs referenced by metrics and dimensions."""
        entities = set()
        for m in self.metrics:
            entities.add(m.entity_id)
        for d in self.dimensions:
            if d.entity_id:
                entities.add(d.entity_id)
        return sorted(list(entities))

    def _get_all_tags(self) -> List[str]:
        """Get all unique tags across metrics."""
        tags = set()
        for m in self.metrics:
            tags.update(m.tags)
        return sorted(list(tags))

    # --- Serialization ---

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "model_id": self.model_id,
            "name": self.name,
            "description": self.description,
            "owner": self.owner,
            "domain": self.domain,
            "metrics": [m.to_dict() for m in self.metrics],
            "dimensions": [d.to_dict() for d in self.dimensions],
            "hierarchies": [h.to_dict() for h in self.hierarchies],
            "default_time_dimension": self.default_time_dimension,
            "default_granularity": self.default_granularity.value if self.default_granularity else None,
            "version": self.version,
            "mdde_model_id": self.mdde_model_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SemanticModel":
        """Create from dictionary."""
        data = data.copy()

        # Convert nested objects
        if "metrics" in data and data["metrics"]:
            data["metrics"] = [Metric.from_dict(m) for m in data["metrics"]]
        if "dimensions" in data and data["dimensions"]:
            data["dimensions"] = [Dimension.from_dict(d) for d in data["dimensions"]]
        if "hierarchies" in data and data["hierarchies"]:
            data["hierarchies"] = [Hierarchy.from_dict(h) for h in data["hierarchies"]]

        # Convert timestamps
        if isinstance(data.get("created_at"), str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        if isinstance(data.get("updated_at"), str):
            data["updated_at"] = datetime.fromisoformat(data["updated_at"])

        return cls(**data)

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    @classmethod
    def from_json(cls, json_str: str) -> "SemanticModel":
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))

    def to_yaml(self) -> str:
        """Convert to YAML string."""
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "SemanticModel":
        """Create from YAML string."""
        return cls.from_dict(yaml.safe_load(yaml_str))

    # --- Summary ---

    def get_summary(self) -> str:
        """Get a human-readable summary."""
        stats = self.get_statistics()
        lines = [
            f"Semantic Model: {self.name}",
            "=" * 50,
            f"ID: {self.model_id}",
            f"Domain: {self.domain or 'N/A'}",
            f"Owner: {self.owner or 'N/A'}",
            f"Version: {self.version}",
            "",
            "Components:",
            f"  Metrics: {stats['total_metrics']} ({stats['certified_metrics']} certified)",
            f"  Dimensions: {stats['total_dimensions']} ({stats['time_dimensions']} time)",
            f"  Hierarchies: {stats['total_hierarchies']}",
            "",
            f"Entities Used: {stats['unique_entities']}",
        ]

        if stats["tags"]:
            lines.append(f"Tags: {', '.join(stats['tags'])}")

        return "\n".join(lines)
