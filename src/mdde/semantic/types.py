"""
Semantic Layer Types (ADR-301).

Enums and dataclasses for semantic layer definitions.
Inspired by Patrick Okare's "Five Must-Have Layers" - Analytics & Consumption Layer.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
import hashlib


class MetricType(Enum):
    """Type of metric calculation."""

    SIMPLE = "simple"  # Direct aggregation: SUM(revenue)
    DERIVED = "derived"  # Calculated from other metrics: revenue / orders
    CUMULATIVE = "cumulative"  # Running total
    RATIO = "ratio"  # Ratio of two metrics
    CONVERSION = "conversion"  # Funnel conversion rate


class AggregationType(Enum):
    """Aggregation function for metrics."""

    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    PERCENTILE = "percentile"


class DimensionType(Enum):
    """Type of dimension."""

    CATEGORICAL = "categorical"
    TIME = "time"
    GEOGRAPHIC = "geographic"
    NUMERIC = "numeric"  # For numeric ranges/bins


class HierarchyType(Enum):
    """Type of drill-down hierarchy."""

    TIME = "time"
    GEOGRAPHIC = "geographic"
    PRODUCT = "product"
    ORGANIZATIONAL = "organizational"
    CUSTOM = "custom"


class TimeGrain(Enum):
    """Supported time granularities."""

    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


@dataclass
class MetricFilter:
    """A filter condition for a metric."""

    filter_id: str
    expression: str
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "filter_id": self.filter_id,
            "expression": self.expression,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MetricFilter":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class Metric:
    """
    A business metric definition.

    Metrics are the core of the semantic layer - they define how
    business measures are calculated consistently across the organization.
    """

    metric_id: str
    name: str
    description: str

    # Type and expression
    metric_type: MetricType
    expression: str  # SQL expression or formula

    # Source entity
    entity_id: str
    attribute_id: Optional[str] = None  # For simple metrics

    # Aggregation
    aggregation: Optional[AggregationType] = None

    # Filters
    filters: List[MetricFilter] = field(default_factory=list)

    # Time grain support
    time_grains: List[TimeGrain] = field(default_factory=list)

    # Business metadata
    unit: Optional[str] = None  # "$", "%", "orders"
    format: Optional[str] = None  # "#,##0.00"

    # Governance
    owner: Optional[str] = None
    certified: bool = False
    tags: List[str] = field(default_factory=list)

    # Parent model
    semantic_model_id: Optional[str] = None

    # Timestamps
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self):
        """Convert string enums to enum types."""
        if isinstance(self.metric_type, str):
            self.metric_type = MetricType(self.metric_type)
        if isinstance(self.aggregation, str):
            self.aggregation = AggregationType(self.aggregation)
        if self.time_grains:
            self.time_grains = [
                TimeGrain(g) if isinstance(g, str) else g
                for g in self.time_grains
            ]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "metric_id": self.metric_id,
            "name": self.name,
            "description": self.description,
            "metric_type": self.metric_type.value,
            "expression": self.expression,
            "entity_id": self.entity_id,
            "attribute_id": self.attribute_id,
            "aggregation": self.aggregation.value if self.aggregation else None,
            "filters": [f.to_dict() for f in self.filters],
            "time_grains": [g.value for g in self.time_grains],
            "unit": self.unit,
            "format": self.format,
            "owner": self.owner,
            "certified": self.certified,
            "tags": self.tags,
            "semantic_model_id": self.semantic_model_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Metric":
        """Create from dictionary."""
        data = data.copy()

        # Convert filters
        if "filters" in data and data["filters"]:
            data["filters"] = [MetricFilter.from_dict(f) for f in data["filters"]]

        # Convert timestamps
        if isinstance(data.get("created_at"), str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        if isinstance(data.get("updated_at"), str):
            data["updated_at"] = datetime.fromisoformat(data["updated_at"])

        return cls(**data)


@dataclass
class Dimension:
    """
    A dimension for slicing metrics.

    Dimensions define the axes along which metrics can be analyzed -
    time, geography, product categories, customer segments, etc.
    """

    dimension_id: str
    name: str
    description: Optional[str] = None

    # Source
    entity_id: str = ""
    attribute_id: str = ""

    # Type
    dimension_type: DimensionType = DimensionType.CATEGORICAL

    # For time dimensions
    time_granularity: Optional[TimeGrain] = None

    # For geographic dimensions
    geo_type: Optional[str] = None  # country, region, city

    # Allowed values (optional constraint)
    allowed_values: Optional[List[str]] = None

    # Display
    label: Optional[str] = None
    format: Optional[str] = None

    # Parent model
    semantic_model_id: Optional[str] = None

    def __post_init__(self):
        """Convert string enums to enum types."""
        if isinstance(self.dimension_type, str):
            self.dimension_type = DimensionType(self.dimension_type)
        if isinstance(self.time_granularity, str):
            self.time_granularity = TimeGrain(self.time_granularity)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dimension_id": self.dimension_id,
            "name": self.name,
            "description": self.description,
            "entity_id": self.entity_id,
            "attribute_id": self.attribute_id,
            "dimension_type": self.dimension_type.value,
            "time_granularity": self.time_granularity.value if self.time_granularity else None,
            "geo_type": self.geo_type,
            "allowed_values": self.allowed_values,
            "label": self.label,
            "format": self.format,
            "semantic_model_id": self.semantic_model_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Dimension":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class HierarchyLevel:
    """A level in a drill-down hierarchy."""

    level_id: str
    name: str
    dimension_id: str  # Links to a dimension
    order: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "level_id": self.level_id,
            "name": self.name,
            "dimension_id": self.dimension_id,
            "order": self.order,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HierarchyLevel":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class Hierarchy:
    """
    A drill-down hierarchy for dimensions.

    Hierarchies define logical groupings of dimensions that support
    drill-down/roll-up analysis (e.g., Year > Quarter > Month > Day).
    """

    hierarchy_id: str
    name: str
    description: Optional[str] = None

    # Levels (ordered from top to bottom)
    levels: List[HierarchyLevel] = field(default_factory=list)

    # Type
    hierarchy_type: HierarchyType = HierarchyType.CUSTOM

    # Parent model
    semantic_model_id: Optional[str] = None

    def __post_init__(self):
        """Convert string enums to enum types."""
        if isinstance(self.hierarchy_type, str):
            self.hierarchy_type = HierarchyType(self.hierarchy_type)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "hierarchy_id": self.hierarchy_id,
            "name": self.name,
            "description": self.description,
            "levels": [l.to_dict() for l in self.levels],
            "hierarchy_type": self.hierarchy_type.value,
            "semantic_model_id": self.semantic_model_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Hierarchy":
        """Create from dictionary."""
        data = data.copy()
        if "levels" in data and data["levels"]:
            data["levels"] = [HierarchyLevel.from_dict(l) for l in data["levels"]]
        return cls(**data)


def generate_id(prefix: str) -> str:
    """Generate a unique ID with prefix."""
    import uuid
    short_uuid = hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()[:12]
    return f"{prefix}_{short_uuid}"
