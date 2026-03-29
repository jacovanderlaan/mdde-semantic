# MDDE Aboutness Layer
# ADR-247: Aboutness Layer - Semantic Intent and Purpose
# Feb 2026

"""
Aboutness layer for explicit semantic intent and purpose.

"Aboutness" is the semantic link between raw data (Data Plane) and
conceptual understanding (Knowledge Plane). It captures not just
what data IS, but what it MEANS and WHY it exists.

Example:
    from mdde.semantic.aboutness import (
        AboutnessManager,
        EntityAboutness,
        AttributeAboutness,
        AboutnessDimension,
        SemanticRole,
    )

    manager = AboutnessManager(conn)

    # Set entity aboutness
    manager.set_entity_aboutness(EntityAboutness(
        entity_id="customer_order",
        purpose="Tracks customer purchase transactions for revenue analysis",
        real_world_object="PurchaseTransaction",
        aboutness_dimension=AboutnessDimension.CLASSIFIER,
        business_use_cases=["revenue_reporting", "customer_segmentation"],
    ))

    # Set attribute aboutness
    manager.set_attribute_aboutness(AttributeAboutness(
        entity_id="customer_order",
        attribute_id="order_total",
        intent="Captures monetary value of the complete order",
        aboutness_dimension=AboutnessDimension.MEASURE,
        semantic_role=SemanticRole.AGGREGATABLE,
        measures_what="TransactionValue",
        canonical_name="transaction_amount",
    ))

    # Infer aboutness from patterns
    inferred = manager.infer_aboutness("customer_order")

    # Validate semantic consistency
    issues = manager.validate_aboutness("sales_analytics")
"""

from .models import (
    AboutnessDimension,
    SemanticRole,
    DependencyType,
    EntityAboutness,
    AttributeAboutness,
    SemanticDependency,
    AboutnessValidation,
)
from .manager import AboutnessManager
from .inference import AboutnessInferrer
from .validator import AboutnessValidator

__all__ = [
    # Enums
    "AboutnessDimension",
    "SemanticRole",
    "DependencyType",
    # Models
    "EntityAboutness",
    "AttributeAboutness",
    "SemanticDependency",
    "AboutnessValidation",
    # Manager
    "AboutnessManager",
    # Inference
    "AboutnessInferrer",
    # Validation
    "AboutnessValidator",
]
