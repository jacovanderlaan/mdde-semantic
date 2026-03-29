#!/usr/bin/env python
"""
Demo 02: Metrics Gallery

This script demonstrates different types of metrics:
- Simple metrics (direct aggregations)
- Derived metrics (calculated from other metrics)
- Cumulative metrics (running totals)
- Ratio metrics (ratios between metrics)
- Conversion metrics (funnel analysis)
"""

import sys
from pathlib import Path

# Add MDDE to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from mdde.semantic import (
    SemanticModel,
    SemanticMetric,
    SemanticMetricType,
    SemanticAggregationType,
    SemanticMetricFilter,
    SemanticTimeGrain,
)


def create_metrics_gallery() -> SemanticModel:
    """Create a model with various metric types."""

    model = SemanticModel(
        semantic_model_id="metrics_gallery",
        name="Metrics Gallery",
        description="Demonstration of all metric types",
    )

    # ==================== SIMPLE METRICS ====================
    # Direct aggregations on a single column

    model.add_metric(SemanticMetric(
        metric_id="simple_sum_revenue",
        name="Total Revenue",
        description="Simple SUM aggregation of order amounts",
        metric_type=SemanticMetricType.SIMPLE,
        expression="SUM(order_amount)",
        entity_id="ent_orders",
        attribute_id="order_amount",
        aggregation=SemanticAggregationType.SUM,
        unit="$",
        format="#,##0.00",
        certified=True,
        tags=["simple", "revenue"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="simple_count_orders",
        name="Order Count",
        description="Simple COUNT of distinct orders",
        metric_type=SemanticMetricType.SIMPLE,
        expression="COUNT(DISTINCT order_id)",
        entity_id="ent_orders",
        attribute_id="order_id",
        aggregation=SemanticAggregationType.COUNT_DISTINCT,
        unit="orders",
        format="#,##0",
        certified=True,
        tags=["simple", "orders"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="simple_avg_price",
        name="Average Unit Price",
        description="Simple AVG of product prices",
        metric_type=SemanticMetricType.SIMPLE,
        expression="AVG(unit_price)",
        entity_id="ent_order_items",
        attribute_id="unit_price",
        aggregation=SemanticAggregationType.AVG,
        unit="$",
        format="#,##0.00",
        tags=["simple", "pricing"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="simple_max_order",
        name="Largest Order",
        description="Maximum order amount",
        metric_type=SemanticMetricType.SIMPLE,
        expression="MAX(order_amount)",
        entity_id="ent_orders",
        attribute_id="order_amount",
        aggregation=SemanticAggregationType.MAX,
        unit="$",
        format="#,##0.00",
        tags=["simple", "orders"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="simple_median_order",
        name="Median Order Value",
        description="Median order amount (50th percentile)",
        metric_type=SemanticMetricType.SIMPLE,
        expression="MEDIAN(order_amount)",
        entity_id="ent_orders",
        attribute_id="order_amount",
        aggregation=SemanticAggregationType.MEDIAN,
        unit="$",
        format="#,##0.00",
        tags=["simple", "orders"],
    ))

    # ==================== SIMPLE WITH FILTERS ====================
    # Aggregations with filter conditions

    model.add_metric(SemanticMetric(
        metric_id="simple_completed_revenue",
        name="Completed Order Revenue",
        description="Revenue from completed orders only",
        metric_type=SemanticMetricType.SIMPLE,
        expression="SUM(order_amount)",
        entity_id="ent_orders",
        attribute_id="order_amount",
        aggregation=SemanticAggregationType.SUM,
        filters=[
            SemanticMetricFilter(
                filter_id="filter_completed",
                expression="status = 'completed'",
                description="Only completed orders",
            ),
        ],
        unit="$",
        format="#,##0.00",
        tags=["simple", "revenue", "filtered"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="simple_refunded_revenue",
        name="Refunded Revenue",
        description="Revenue from refunded orders",
        metric_type=SemanticMetricType.SIMPLE,
        expression="SUM(order_amount)",
        entity_id="ent_orders",
        attribute_id="order_amount",
        aggregation=SemanticAggregationType.SUM,
        filters=[
            SemanticMetricFilter(
                filter_id="filter_refunded",
                expression="status = 'refunded'",
                description="Only refunded orders",
            ),
        ],
        unit="$",
        format="#,##0.00",
        tags=["simple", "revenue", "filtered"],
    ))

    # ==================== DERIVED METRICS ====================
    # Calculated from other metrics

    model.add_metric(SemanticMetric(
        metric_id="derived_aov",
        name="Average Order Value (Derived)",
        description="Total Revenue / Order Count",
        metric_type=SemanticMetricType.DERIVED,
        expression="simple_sum_revenue / simple_count_orders",
        entity_id="ent_orders",
        unit="$",
        format="#,##0.00",
        certified=True,
        tags=["derived", "revenue", "per-order"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="derived_refund_rate",
        name="Refund Rate",
        description="Percentage of revenue refunded",
        metric_type=SemanticMetricType.DERIVED,
        expression="simple_refunded_revenue / simple_sum_revenue * 100",
        entity_id="ent_orders",
        unit="%",
        format="#,##0.0%",
        tags=["derived", "refunds"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="derived_net_revenue",
        name="Net Revenue",
        description="Completed revenue minus refunded revenue",
        metric_type=SemanticMetricType.DERIVED,
        expression="simple_completed_revenue - simple_refunded_revenue",
        entity_id="ent_orders",
        unit="$",
        format="#,##0.00",
        certified=True,
        tags=["derived", "revenue", "net"],
    ))

    # ==================== CUMULATIVE METRICS ====================
    # Running totals over time

    model.add_metric(SemanticMetric(
        metric_id="cumulative_revenue_ytd",
        name="Year-to-Date Revenue",
        description="Cumulative revenue from start of year",
        metric_type=SemanticMetricType.CUMULATIVE,
        expression="SUM(order_amount) OVER (PARTITION BY YEAR(order_date) ORDER BY order_date)",
        entity_id="ent_orders",
        attribute_id="order_amount",
        unit="$",
        format="#,##0.00",
        time_grains=[SemanticTimeGrain.DAY, SemanticTimeGrain.WEEK, SemanticTimeGrain.MONTH],
        tags=["cumulative", "revenue", "ytd"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="cumulative_orders_mtd",
        name="Month-to-Date Orders",
        description="Cumulative orders from start of month",
        metric_type=SemanticMetricType.CUMULATIVE,
        expression="COUNT(DISTINCT order_id) OVER (PARTITION BY YEAR(order_date), MONTH(order_date) ORDER BY order_date)",
        entity_id="ent_orders",
        attribute_id="order_id",
        unit="orders",
        format="#,##0",
        time_grains=[SemanticTimeGrain.DAY, SemanticTimeGrain.WEEK],
        tags=["cumulative", "orders", "mtd"],
    ))

    # ==================== RATIO METRICS ====================
    # Ratios between metrics

    model.add_metric(SemanticMetric(
        metric_id="ratio_revenue_per_customer",
        name="Revenue per Customer",
        description="Average revenue generated per unique customer",
        metric_type=SemanticMetricType.RATIO,
        expression="simple_sum_revenue / COUNT(DISTINCT customer_id)",
        entity_id="ent_orders",
        unit="$",
        format="#,##0.00",
        tags=["ratio", "revenue", "customers"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="ratio_orders_per_customer",
        name="Orders per Customer",
        description="Average number of orders per customer",
        metric_type=SemanticMetricType.RATIO,
        expression="simple_count_orders / COUNT(DISTINCT customer_id)",
        entity_id="ent_orders",
        unit="orders",
        format="#,##0.0",
        tags=["ratio", "orders", "customers"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="ratio_items_per_order",
        name="Items per Order",
        description="Average number of line items per order",
        metric_type=SemanticMetricType.RATIO,
        expression="COUNT(order_item_id) / COUNT(DISTINCT order_id)",
        entity_id="ent_order_items",
        unit="items",
        format="#,##0.0",
        tags=["ratio", "items", "orders"],
    ))

    # ==================== CONVERSION METRICS ====================
    # Funnel conversion rates

    model.add_metric(SemanticMetric(
        metric_id="conversion_visit_to_cart",
        name="Visit-to-Cart Rate",
        description="Percentage of visits that add items to cart",
        metric_type=SemanticMetricType.CONVERSION,
        expression="COUNT(DISTINCT CASE WHEN added_to_cart THEN session_id END) / COUNT(DISTINCT session_id) * 100",
        entity_id="ent_sessions",
        unit="%",
        format="#,##0.0%",
        tags=["conversion", "funnel", "cart"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="conversion_cart_to_purchase",
        name="Cart-to-Purchase Rate",
        description="Percentage of carts that convert to purchase",
        metric_type=SemanticMetricType.CONVERSION,
        expression="COUNT(DISTINCT CASE WHEN purchased THEN session_id END) / COUNT(DISTINCT CASE WHEN added_to_cart THEN session_id END) * 100",
        entity_id="ent_sessions",
        unit="%",
        format="#,##0.0%",
        tags=["conversion", "funnel", "purchase"],
    ))

    model.add_metric(SemanticMetric(
        metric_id="conversion_overall",
        name="Overall Conversion Rate",
        description="Percentage of visits that result in purchase",
        metric_type=SemanticMetricType.CONVERSION,
        expression="COUNT(DISTINCT CASE WHEN purchased THEN session_id END) / COUNT(DISTINCT session_id) * 100",
        entity_id="ent_sessions",
        unit="%",
        format="#,##0.0%",
        certified=True,
        tags=["conversion", "funnel", "overall", "core-kpi"],
    ))

    return model


def demo_metrics_gallery():
    """Display the metrics gallery."""
    print("=" * 70)
    print("Demo 02: Metrics Gallery")
    print("=" * 70)
    print()

    model = create_metrics_gallery()

    # Group metrics by type
    by_type = {}
    for metric in model.metrics:
        type_name = metric.metric_type.value
        if type_name not in by_type:
            by_type[type_name] = []
        by_type[type_name].append(metric)

    # Display each type
    for metric_type, metrics in by_type.items():
        print(f"\n{'=' * 60}")
        print(f"{metric_type.upper()} METRICS ({len(metrics)})")
        print("=" * 60)

        for metric in metrics:
            cert = "[CERTIFIED]" if metric.certified else ""
            print(f"\n  {metric.name} {cert}")
            print(f"  {'-' * len(metric.name)}")
            print(f"  Description: {metric.description}")
            print(f"  Expression:  {metric.expression}")
            if metric.aggregation:
                print(f"  Aggregation: {metric.aggregation.value}")
            print(f"  Unit:        {metric.unit}")
            if metric.filters:
                print(f"  Filters:     {len(metric.filters)}")
                for f in metric.filters:
                    print(f"               - {f.expression}")
            print(f"  Tags:        {', '.join(metric.tags)}")

    # Summary by type
    print("\n" + "=" * 60)
    print("Summary by Metric Type")
    print("=" * 60)
    print()
    print(f"{'Type':<20} {'Count':>8} {'Certified':>10}")
    print("-" * 40)
    for metric_type, metrics in by_type.items():
        certified = sum(1 for m in metrics if m.certified)
        print(f"{metric_type:<20} {len(metrics):>8} {certified:>10}")
    print("-" * 40)
    total = len(model.metrics)
    total_cert = sum(1 for m in model.metrics if m.certified)
    print(f"{'TOTAL':<20} {total:>8} {total_cert:>10}")
    print()


def main():
    """Run the demo."""
    demo_metrics_gallery()

    print("=" * 70)
    print("Demo complete!")
    print()
    print("Metric Types Summary:")
    print("  SIMPLE:      Direct aggregation on a column (SUM, COUNT, AVG)")
    print("  DERIVED:     Calculated from other metrics (revenue / orders)")
    print("  CUMULATIVE:  Running totals over time (YTD, MTD)")
    print("  RATIO:       Ratio between two measures (revenue per customer)")
    print("  CONVERSION:  Funnel conversion rates (visit-to-purchase)")
    print("=" * 70)


if __name__ == "__main__":
    main()
