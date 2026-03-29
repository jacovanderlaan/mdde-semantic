"""
MDDE Semantic Metrics Layer.

Provides business metrics with semantic context for query-time abstractions.
Includes period-over-period, validation, lineage, and dbt integration.

ADR-245: Metrics Layer
Feb 2026
"""

from .models import (
    # Core enums
    MetricType,
    AggregationType,
    TimeGrain,
    DimensionRole,
    MetricStatus,
    # Core data classes
    MetricDefinition,
    MetricDimension,
    MetricFilter,
    DerivedMetricFormula,
    MetricGoal,
    MetricAlert,
    MetricQuery,
    MetricQueryResult,
    # Enhanced metrics (v3.23.0)
    PeriodComparisonType,
    PeriodComparison,
    MetricLineage,
    MetricValidation,
    MetricValidationResult,
    MetricValidationLevel,
    SemanticModelExport,
    MetricCalculationContext,
    MetricAnnotation,
    MetricCatalogEntry,
)

from .manager import MetricsManager

__all__ = [
    # Core Enums
    "MetricType",
    "AggregationType",
    "TimeGrain",
    "DimensionRole",
    "MetricStatus",
    # Core Data Classes
    "MetricDefinition",
    "MetricDimension",
    "MetricFilter",
    "DerivedMetricFormula",
    "MetricGoal",
    "MetricAlert",
    "MetricQuery",
    "MetricQueryResult",
    # Enhanced Metrics (v3.23.0)
    "PeriodComparisonType",
    "PeriodComparison",
    "MetricLineage",
    "MetricValidation",
    "MetricValidationResult",
    "MetricValidationLevel",
    "SemanticModelExport",
    "MetricCalculationContext",
    "MetricAnnotation",
    "MetricCatalogEntry",
    # Manager
    "MetricsManager",
]
