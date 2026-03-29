"""
Metrics Layer Data Models for MDDE Semantic Layer.

Provides business metrics with semantic context, supporting
query-time abstractions for measures and dimensions.

ADR-245: Metrics Layer
Feb 2026
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


class MetricType(Enum):
    """Types of metrics."""
    SIMPLE = "simple"  # Direct aggregation
    DERIVED = "derived"  # Calculated from other metrics
    RATIO = "ratio"  # Ratio of two metrics
    CUMULATIVE = "cumulative"  # Running total
    PERIOD_OVER_PERIOD = "period_over_period"  # Comparison
    WINDOW = "window"  # Window function based


class AggregationType(Enum):
    """Aggregation functions."""
    SUM = "SUM"
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    MEDIAN = "MEDIAN"
    PERCENTILE = "PERCENTILE"
    STDDEV = "STDDEV"
    VARIANCE = "VARIANCE"


class TimeGrain(Enum):
    """Time granularity options."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class DimensionRole(Enum):
    """Role of a dimension in metric calculations."""
    SLICE = "slice"  # Can slice the metric
    FILTER = "filter"  # Can filter the metric
    GROUP_BY = "group_by"  # Can group by
    DRILL_DOWN = "drill_down"  # For hierarchical drill-down


class MetricStatus(Enum):
    """Metric lifecycle status."""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


@dataclass
class MetricDefinition:
    """A business metric definition."""
    metric_id: str
    metric_name: str
    display_name: str
    description: str = ""
    metric_type: MetricType = MetricType.SIMPLE
    status: MetricStatus = MetricStatus.ACTIVE

    # Source
    entity_id: str = ""  # Primary entity
    attribute_id: Optional[str] = None  # Source attribute for simple metrics

    # Calculation
    aggregation: AggregationType = AggregationType.SUM
    expression: str = ""  # SQL expression or formula
    filter_expression: Optional[str] = None  # WHERE clause

    # Time
    time_grain: Optional[TimeGrain] = None
    time_attribute_id: Optional[str] = None  # Date/time column

    # Dependencies (for derived metrics)
    depends_on_metrics: List[str] = field(default_factory=list)

    # Ownership
    business_owner: Optional[str] = None
    technical_owner: Optional[str] = None
    domain: Optional[str] = None

    # Formatting
    format_string: Optional[str] = None  # e.g., "$#,##0.00"
    unit: Optional[str] = None  # e.g., "USD", "count", "%"

    # Metadata
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)


@dataclass
class MetricDimension:
    """A dimension that can be used with a metric."""
    dimension_id: str
    metric_id: str
    attribute_id: str  # MDDE attribute
    dimension_name: str
    display_name: str = ""
    description: str = ""
    role: DimensionRole = DimensionRole.GROUP_BY

    # Hierarchy support
    parent_dimension_id: Optional[str] = None
    hierarchy_level: int = 0

    # Default behavior
    is_default: bool = False  # Include by default in queries
    is_required: bool = False  # Must be included


@dataclass
class MetricFilter:
    """A predefined filter for a metric."""
    filter_id: str
    metric_id: str
    filter_name: str
    display_name: str = ""
    description: str = ""
    filter_expression: str = ""  # SQL WHERE clause
    is_default: bool = False


@dataclass
class DerivedMetricFormula:
    """Formula for a derived metric."""
    formula_id: str
    metric_id: str
    formula_expression: str  # e.g., "total_revenue / order_count"
    component_metrics: List[str] = field(default_factory=list)


@dataclass
class MetricGoal:
    """Target/goal for a metric."""
    goal_id: str
    metric_id: str
    goal_name: str
    target_value: float
    comparison: str = "gte"  # "gte", "lte", "eq", "gt", "lt"
    time_period: Optional[str] = None  # e.g., "2026-Q1"
    dimension_filters: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricAlert:
    """Alert configuration for a metric."""
    alert_id: str
    metric_id: str
    alert_name: str
    condition: str  # e.g., "value < threshold"
    threshold: float
    severity: str = "warning"  # "info", "warning", "critical"
    notification_channels: List[str] = field(default_factory=list)


@dataclass
class MetricQuery:
    """A semantic query for metrics."""
    metric_ids: List[str]
    dimensions: List[str] = field(default_factory=list)
    filters: Dict[str, Any] = field(default_factory=dict)
    time_range: Optional[Dict[str, str]] = None  # {"start": ..., "end": ...}
    time_grain: Optional[TimeGrain] = None
    order_by: Optional[List[str]] = None
    limit: Optional[int] = None


@dataclass
class MetricQueryResult:
    """Result of executing a metric query."""
    query: MetricQuery
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int
    generated_sql: str
    execution_time_ms: float
    executed_at: datetime = field(default_factory=_utc_now)


# ==================== Enhanced Metrics (v3.23.0) ====================

class PeriodComparisonType(Enum):
    """Types of period-over-period comparisons."""
    PREVIOUS_PERIOD = "previous_period"  # Compare to previous period
    SAME_PERIOD_LAST_YEAR = "same_period_last_year"  # YoY
    SAME_PERIOD_LAST_MONTH = "same_period_last_month"  # MoM
    SAME_PERIOD_LAST_WEEK = "same_period_last_week"  # WoW
    ROLLING_AVERAGE = "rolling_average"  # Rolling window
    YTD = "ytd"  # Year to date
    MTD = "mtd"  # Month to date


class MetricValidationLevel(Enum):
    """Validation levels for metrics."""
    NONE = "none"
    BASIC = "basic"  # Non-null, non-negative
    STANDARD = "standard"  # + range checks
    STRICT = "strict"  # + anomaly detection


@dataclass
class PeriodComparison:
    """Period-over-period comparison configuration."""
    comparison_id: str
    metric_id: str
    comparison_type: PeriodComparisonType
    periods_back: int = 1  # e.g., 1 for previous, 4 for same quarter last year
    label: str = ""  # e.g., "vs Last Month"
    show_absolute: bool = True  # Show absolute difference
    show_percentage: bool = True  # Show percentage change


@dataclass
class MetricLineage:
    """Tracks metric dependencies and lineage."""
    lineage_id: str
    metric_id: str
    source_type: str  # "metric", "entity", "attribute", "external"
    source_id: str  # ID of the source
    source_name: str
    transformation: str = ""  # Description of transformation
    created_at: datetime = field(default_factory=_utc_now)


@dataclass
class MetricValidation:
    """Validation rule for a metric."""
    validation_id: str
    metric_id: str
    validation_name: str
    validation_type: str  # "range", "not_null", "positive", "anomaly"
    parameters: Dict[str, Any] = field(default_factory=dict)
    severity: str = "warning"  # "info", "warning", "error"
    is_active: bool = True


@dataclass
class MetricValidationResult:
    """Result of metric validation."""
    metric_id: str
    validation_id: str
    passed: bool
    message: str
    actual_value: Optional[float] = None
    expected_range: Optional[Tuple[float, float]] = None
    checked_at: datetime = field(default_factory=_utc_now)


@dataclass
class SemanticModelExport:
    """Configuration for semantic model export (dbt, Cube.js, etc.)."""
    format: str  # "dbt", "cube", "looker", "tableau"
    model_name: str
    metrics: List[str] = field(default_factory=list)
    include_dimensions: bool = True
    include_relationships: bool = True
    include_time_spines: bool = True


@dataclass
class MetricCalculationContext:
    """Context for metric calculation."""
    as_of_date: Optional[datetime] = None
    time_zone: str = "UTC"
    currency: str = "USD"
    fiscal_year_start_month: int = 1
    week_start_day: int = 1  # 1=Monday, 7=Sunday


@dataclass
class MetricAnnotation:
    """Annotation/note on a metric value."""
    annotation_id: str
    metric_id: str
    annotation_text: str
    annotation_type: str = "note"  # "note", "warning", "explanation"
    dimension_values: Dict[str, Any] = field(default_factory=dict)
    time_period: Optional[str] = None
    created_by: Optional[str] = None
    created_at: datetime = field(default_factory=_utc_now)


@dataclass
class MetricCatalogEntry:
    """Entry in the metric catalog for discovery."""
    metric_id: str
    metric_name: str
    display_name: str
    description: str
    domain: str
    category: str
    tags: List[str]
    business_owner: str
    status: MetricStatus
    popularity_score: float = 0.0  # Based on usage
    quality_score: float = 0.0  # Based on validation results
    last_refreshed: Optional[datetime] = None
