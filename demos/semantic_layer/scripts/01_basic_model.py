#!/usr/bin/env python
"""
Demo 01: Basic Semantic Model

This script demonstrates creating a basic semantic model with
metrics, dimensions, and hierarchies.
"""

import sys
from pathlib import Path

# Add MDDE to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from mdde.semantic import (
    SemanticModel,
    SemanticMetric,
    SemanticDimension,
    Hierarchy,
    HierarchyLevel,
    SemanticMetricType,
    SemanticAggregationType,
    DimensionType,
    HierarchyType,
    SemanticTimeGrain,
)


def create_sales_model() -> SemanticModel:
    """Create a sample sales analytics semantic model."""

    # Create the model
    model = SemanticModel(
        semantic_model_id="sales_analytics_v1",
        name="Sales Analytics",
        description="Core sales metrics and dimensions for e-commerce analytics",
    )

    # ==================== METRICS ====================

    # Simple metric: Total Revenue
    model.add_metric(SemanticMetric(
        metric_id="metric_total_revenue",
        name="Total Revenue",
        description="Sum of all order amounts including tax and shipping",
        metric_type=SemanticMetricType.SIMPLE,
        expression="SUM(order_amount)",
        entity_id="ent_orders",
        attribute_id="order_amount",
        aggregation=SemanticAggregationType.SUM,
        unit="$",
        format="#,##0.00",
        owner="finance_team",
        certified=True,
        tags=["revenue", "core-kpi", "audited"],
        time_grains=[
            SemanticTimeGrain.DAY,
            SemanticTimeGrain.WEEK,
            SemanticTimeGrain.MONTH,
            SemanticTimeGrain.QUARTER,
            SemanticTimeGrain.YEAR,
        ],
    ))

    # Simple metric: Order Count
    model.add_metric(SemanticMetric(
        metric_id="metric_order_count",
        name="Order Count",
        description="Total number of orders placed",
        metric_type=SemanticMetricType.SIMPLE,
        expression="COUNT(DISTINCT order_id)",
        entity_id="ent_orders",
        attribute_id="order_id",
        aggregation=SemanticAggregationType.COUNT_DISTINCT,
        unit="orders",
        format="#,##0",
        owner="operations_team",
        certified=True,
        tags=["orders", "core-kpi"],
    ))

    # Derived metric: Average Order Value
    model.add_metric(SemanticMetric(
        metric_id="metric_aov",
        name="Average Order Value",
        description="Average revenue per order (Total Revenue / Order Count)",
        metric_type=SemanticMetricType.DERIVED,
        expression="metric_total_revenue / metric_order_count",
        entity_id="ent_orders",
        unit="$",
        format="#,##0.00",
        owner="finance_team",
        certified=True,
        tags=["revenue", "per-order", "core-kpi"],
    ))

    # Simple metric: Customer Count
    model.add_metric(SemanticMetric(
        metric_id="metric_customer_count",
        name="Customer Count",
        description="Number of unique customers who placed orders",
        metric_type=SemanticMetricType.SIMPLE,
        expression="COUNT(DISTINCT customer_id)",
        entity_id="ent_orders",
        attribute_id="customer_id",
        aggregation=SemanticAggregationType.COUNT_DISTINCT,
        unit="customers",
        format="#,##0",
        owner="marketing_team",
        certified=True,
        tags=["customers", "core-kpi"],
    ))

    # ==================== DIMENSIONS ====================

    # Time dimension
    model.add_dimension(SemanticDimension(
        dimension_id="dim_order_date",
        name="Order Date",
        description="Date when the order was placed",
        entity_id="ent_orders",
        attribute_id="order_date",
        dimension_type=DimensionType.TIME,
        time_granularity=SemanticTimeGrain.DAY,
        label="Date",
    ))

    # Geographic dimension
    model.add_dimension(SemanticDimension(
        dimension_id="dim_region",
        name="Sales Region",
        description="Geographic sales region",
        entity_id="ent_orders",
        attribute_id="region",
        dimension_type=DimensionType.GEOGRAPHIC,
        geo_type="region",
        label="Region",
        allowed_values=["North", "South", "East", "West", "Central"],
    ))

    # Categorical dimension
    model.add_dimension(SemanticDimension(
        dimension_id="dim_product_category",
        name="Product Category",
        description="Product category classification",
        entity_id="ent_products",
        attribute_id="category",
        dimension_type=DimensionType.CATEGORICAL,
        label="Category",
    ))

    # Categorical dimension
    model.add_dimension(SemanticDimension(
        dimension_id="dim_customer_segment",
        name="Customer Segment",
        description="Customer segmentation tier",
        entity_id="ent_customers",
        attribute_id="segment",
        dimension_type=DimensionType.CATEGORICAL,
        label="Segment",
        allowed_values=["Enterprise", "SMB", "Consumer"],
    ))

    # ==================== HIERARCHIES ====================

    # Time hierarchy
    model.add_hierarchy(Hierarchy(
        hierarchy_id="hier_time",
        name="Time Hierarchy",
        description="Calendar time drill-down from year to day",
        hierarchy_type=HierarchyType.TIME,
        levels=[
            HierarchyLevel(
                level_id="lvl_year",
                name="Year",
                dimension_id="dim_order_date",
                order=0,
            ),
            HierarchyLevel(
                level_id="lvl_quarter",
                name="Quarter",
                dimension_id="dim_order_date",
                order=1,
            ),
            HierarchyLevel(
                level_id="lvl_month",
                name="Month",
                dimension_id="dim_order_date",
                order=2,
            ),
            HierarchyLevel(
                level_id="lvl_week",
                name="Week",
                dimension_id="dim_order_date",
                order=3,
            ),
            HierarchyLevel(
                level_id="lvl_day",
                name="Day",
                dimension_id="dim_order_date",
                order=4,
            ),
        ],
    ))

    return model


def demo_basic_model():
    """Demonstrate creating and exploring a semantic model."""
    print("=" * 60)
    print("Demo 01: Basic Semantic Model")
    print("=" * 60)
    print()

    # Create the model
    model = create_sales_model()

    # Display model overview
    print(f"Model: {model.name}")
    print(f"ID: {model.semantic_model_id}")
    print(f"Description: {model.description}")
    print()

    # Display metrics
    print("Metrics:")
    print("-" * 50)
    for metric in model.metrics:
        cert_badge = "[CERTIFIED]" if metric.certified else ""
        print(f"  {metric.name} {cert_badge}")
        print(f"    Type: {metric.metric_type.value}")
        print(f"    Expression: {metric.expression}")
        print(f"    Unit: {metric.unit}")
        print(f"    Owner: {metric.owner}")
        print(f"    Tags: {', '.join(metric.tags)}")
        print()

    # Display dimensions
    print("Dimensions:")
    print("-" * 50)
    for dim in model.dimensions:
        print(f"  {dim.name}")
        print(f"    Type: {dim.dimension_type.value}")
        print(f"    Source: {dim.entity_id}.{dim.attribute_id}")
        if dim.allowed_values:
            print(f"    Values: {', '.join(dim.allowed_values)}")
        print()

    # Display hierarchies
    print("Hierarchies:")
    print("-" * 50)
    for hier in model.hierarchies:
        print(f"  {hier.name} ({hier.hierarchy_type.value})")
        for level in hier.levels:
            indent = "    " + "  " * level.order
            print(f"{indent}-> {level.name}")
        print()

    # Summary
    print("Summary:")
    print("-" * 50)
    print(f"  Total metrics: {len(model.metrics)}")
    print(f"  Certified metrics: {sum(1 for m in model.metrics if m.certified)}")
    print(f"  Total dimensions: {len(model.dimensions)}")
    print(f"  Total hierarchies: {len(model.hierarchies)}")
    print()


def demo_to_dict():
    """Demonstrate serialization."""
    print("=" * 60)
    print("Model Serialization")
    print("=" * 60)
    print()

    model = create_sales_model()
    data = model.to_dict()

    print("Model can be serialized to dictionary:")
    print(f"  Keys: {list(data.keys())}")
    print(f"  Metrics count: {len(data.get('metrics', []))}")
    print(f"  Dimensions count: {len(data.get('dimensions', []))}")
    print()


def main():
    """Run all demos."""
    demo_basic_model()
    demo_to_dict()

    print("=" * 60)
    print("Demo complete!")
    print()
    print("Key takeaways:")
    print("  1. Semantic models group related metrics and dimensions")
    print("  2. Metrics define HOW to calculate business measures")
    print("  3. Dimensions define AXES for slicing and dicing")
    print("  4. Hierarchies enable drill-down analysis")
    print("  5. Governance features (certified, owner, tags) add trust")
    print("=" * 60)


if __name__ == "__main__":
    main()
