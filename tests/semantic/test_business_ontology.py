"""
Tests for Business Ontology Layer (ADR-359).

Tests context-aware metric interpretation, causal relationships,
and ontology questionnaire functionality.
"""

import pytest
import tempfile
from pathlib import Path

import duckdb


class TestCausalRelationship:
    """Tests for CausalRelationship model."""

    def test_create_relationship(self):
        """Test creating a causal relationship."""
        from mdde.semantic.ontology.business_ontology import (
            CausalRelationship,
            CausalType,
            CausalDirection,
        )

        rel = CausalRelationship(
            relationship_id="rel_1",
            source_concept_id="engagement",
            target_concept_id="health",
            relationship_type=CausalType.INDICATES,
            direction=CausalDirection.POSITIVE,
            context="enterprise_model",
            strength=0.9,
        )

        assert rel.relationship_type == CausalType.INDICATES
        assert rel.direction == CausalDirection.POSITIVE
        assert rel.strength == 0.9

    def test_relationship_to_dict(self):
        """Test serialization."""
        from mdde.semantic.ontology.business_ontology import (
            CausalRelationship,
            CausalType,
            CausalDirection,
        )

        rel = CausalRelationship(
            relationship_id="rel_1",
            source_concept_id="a",
            target_concept_id="b",
            relationship_type=CausalType.CAUSES,
            direction=CausalDirection.NEGATIVE,
        )

        d = rel.to_dict()
        assert d["relationship_type"] == "causes"
        assert d["direction"] == "negative"

    def test_relationship_from_dict(self):
        """Test deserialization."""
        from mdde.semantic.ontology.business_ontology import (
            CausalRelationship,
            CausalType,
            CausalDirection,
        )

        data = {
            "source_concept_id": "x",
            "target_concept_id": "y",
            "relationship_type": "predicts",
            "direction": "conditional",
        }

        rel = CausalRelationship.from_dict(data)
        assert rel.relationship_type == CausalType.PREDICTS
        assert rel.direction == CausalDirection.CONDITIONAL


class TestBusinessConcept:
    """Tests for BusinessConcept model."""

    def test_create_concept(self):
        """Test creating a business concept."""
        from mdde.semantic.ontology.business_ontology import BusinessConcept

        concept = BusinessConcept(
            concept_id="customer",
            name="Customer",
            definition="A person who purchases products",
            business_model="b2c_retail",
            domain="sales",
            success_indicators=["repeat_purchase", "high_ltv"],
            failure_indicators=["churn", "returns"],
        )

        assert concept.name == "Customer"
        assert len(concept.success_indicators) == 2
        assert "churn" in concept.failure_indicators

    def test_concept_to_dict(self):
        """Test serialization."""
        from mdde.semantic.ontology.business_ontology import BusinessConcept

        concept = BusinessConcept(
            concept_id="cust",
            name="Customer",
            definition="Primary buyer",
        )

        d = concept.to_dict()
        assert d["name"] == "Customer"
        assert d["definition"] == "Primary buyer"


class TestMetricInterpretation:
    """Tests for MetricInterpretation model."""

    def test_create_interpretation(self):
        """Test creating a metric interpretation."""
        from mdde.semantic.ontology.business_ontology import (
            MetricInterpretation,
            Threshold,
            Sentiment,
        )

        interp = MetricInterpretation(
            interpretation_id="mi_1",
            metric_id="support_tickets",
            context="enterprise_white_glove",
            increase_means="Engaged customers using premium support (good)",
            decrease_means="May indicate disengagement (warning)",
            thresholds=[
                Threshold("critical", -30, Sentiment.WARNING, "Investigate"),
            ],
            recommended_actions={
                "decrease_True": "Proactive outreach",
            },
        )

        assert interp.context == "enterprise_white_glove"
        assert len(interp.thresholds) == 1
        assert "decrease_True" in interp.recommended_actions

    def test_interpretation_to_dict(self):
        """Test serialization."""
        from mdde.semantic.ontology.business_ontology import (
            MetricInterpretation,
            Threshold,
            Sentiment,
        )

        interp = MetricInterpretation(
            interpretation_id="mi_1",
            metric_id="nps",
            context="saas",
            increase_means="improving",
            decrease_means="declining",
            thresholds=[
                Threshold("good", 50, Sentiment.POSITIVE, "Great!"),
            ],
        )

        d = interp.to_dict()
        assert d["metric_id"] == "nps"
        assert len(d["thresholds"]) == 1


class TestBusinessOntology:
    """Tests for BusinessOntology model."""

    def test_create_ontology(self):
        """Test creating a business ontology."""
        from mdde.semantic.ontology.business_ontology import BusinessOntology

        ontology = BusinessOntology(
            ontology_id="bo_1",
            name="Enterprise CRM Ontology",
            business_model="b2b_enterprise",
            description="Ontology for enterprise CRM business",
        )

        assert ontology.name == "Enterprise CRM Ontology"
        assert len(ontology.concepts) == 0

    def test_ontology_to_yaml(self):
        """Test YAML serialization."""
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntology,
            BusinessConcept,
        )

        ontology = BusinessOntology(
            ontology_id="bo_1",
            name="Test Ontology",
        )
        ontology.concepts.append(BusinessConcept(
            concept_id="cust",
            name="Customer",
            definition="A buyer",
        ))

        yaml_str = ontology.to_yaml()
        assert "Test Ontology" in yaml_str
        assert "Customer" in yaml_str

    def test_ontology_from_yaml(self):
        """Test YAML deserialization."""
        from mdde.semantic.ontology.business_ontology import BusinessOntology

        yaml_content = """
ontology_id: bo_test
name: Test Ontology
version: "1.0"
business_model: saas
concepts:
  - concept_id: customer
    name: Customer
    definition: A user
relationships: []
interpretations: []
"""
        ontology = BusinessOntology.from_yaml(yaml_content)
        assert ontology.name == "Test Ontology"
        assert ontology.business_model == "saas"
        assert len(ontology.concepts) == 1

    def test_get_causal_chain(self):
        """Test causal chain traversal."""
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntology,
            CausalRelationship,
            CausalType,
            CausalDirection,
        )

        ontology = BusinessOntology(
            ontology_id="bo_1",
            name="Test",
        )

        # Create chain: A -> B -> C
        ontology.relationships.append(CausalRelationship(
            relationship_id="r1",
            source_concept_id="A",
            target_concept_id="B",
            relationship_type=CausalType.CAUSES,
            direction=CausalDirection.POSITIVE,
        ))
        ontology.relationships.append(CausalRelationship(
            relationship_id="r2",
            source_concept_id="B",
            target_concept_id="C",
            relationship_type=CausalType.CAUSES,
            direction=CausalDirection.POSITIVE,
        ))

        chains = ontology.get_causal_chain("A")
        assert len(chains) >= 2  # A->B and A->B->C


class TestBusinessOntologyManager:
    """Tests for BusinessOntologyManager."""

    @pytest.fixture
    def conn(self):
        """Create test database."""
        return duckdb.connect(":memory:")

    def test_create_manager(self, conn):
        """Test creating manager."""
        from mdde.semantic.ontology.business_ontology import BusinessOntologyManager

        manager = BusinessOntologyManager(conn)
        assert manager is not None

    def test_create_ontology(self, conn):
        """Test creating an ontology."""
        from mdde.semantic.ontology.business_ontology import BusinessOntologyManager

        manager = BusinessOntologyManager(conn)
        ontology = manager.create_ontology(
            name="Enterprise CRM",
            business_model="b2b_enterprise",
            description="CRM ontology",
        )

        assert ontology.name == "Enterprise CRM"
        assert ontology.ontology_id.startswith("bo_")

    def test_add_concept(self, conn):
        """Test adding a concept."""
        from mdde.semantic.ontology.business_ontology import BusinessOntologyManager

        manager = BusinessOntologyManager(conn)
        ontology = manager.create_ontology("Test", "test")

        concept = manager.add_concept(
            ontology_id=ontology.ontology_id,
            name="Customer",
            definition="Primary buyer",
            success_indicators=["repeat_purchase"],
        )

        assert concept.name == "Customer"

        # Verify it's in the ontology
        retrieved = manager.get_ontology(ontology.ontology_id)
        assert len(retrieved.concepts) == 1

    def test_add_causal_relationship(self, conn):
        """Test adding a causal relationship."""
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntologyManager,
            CausalType,
            CausalDirection,
        )

        manager = BusinessOntologyManager(conn)
        ontology = manager.create_ontology("Test", "test")

        manager.add_concept(ontology.ontology_id, "engagement", "User engagement")
        manager.add_concept(ontology.ontology_id, "health", "Customer health")

        rel = manager.add_causal_relationship(
            ontology_id=ontology.ontology_id,
            source_concept_id="engagement",
            target_concept_id="health",
            relationship_type=CausalType.INDICATES,
            direction=CausalDirection.POSITIVE,
        )

        assert rel.relationship_type == CausalType.INDICATES

    def test_add_metric_interpretation(self, conn):
        """Test adding a metric interpretation."""
        from mdde.semantic.ontology.business_ontology import BusinessOntologyManager

        manager = BusinessOntologyManager(conn)
        ontology = manager.create_ontology("Test", "b2b_enterprise")

        interp = manager.add_metric_interpretation(
            ontology_id=ontology.ontology_id,
            metric_id="support_tickets",
            context="enterprise_white_glove",
            increase_means="Good engagement",
            decrease_means="Warning: disengagement",
        )

        assert interp.metric_id == "support_tickets"
        assert interp.context == "enterprise_white_glove"

    def test_interpret_metric_change_with_context(self, conn):
        """Test interpreting metric change with business context."""
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntologyManager,
            Sentiment,
        )

        manager = BusinessOntologyManager(conn)
        ontology = manager.create_ontology("Test", "b2b_enterprise")

        # Add interpretation for white-glove model
        manager.add_metric_interpretation(
            ontology_id=ontology.ontology_id,
            metric_id="support_tickets",
            context="enterprise_white_glove",
            increase_means="Engaged customers using premium support",
            decrease_means="WARNING: Customer disengagement detected",
            recommended_actions={
                "decrease_True": "Proactive outreach to check satisfaction",
                "default": "Monitor",
            },
        )

        # Test: 45% decrease in tickets
        result = manager.interpret_metric_change(
            metric_id="support_tickets",
            change_percent=-45,
            context="enterprise_white_glove",
            ontology_id=ontology.ontology_id,
        )

        assert result.sentiment == Sentiment.WARNING
        assert "disengagement" in result.explanation.lower()
        assert "outreach" in result.recommended_action.lower()

    def test_interpret_same_metric_different_context(self, conn):
        """Test that same metric means different things in different contexts."""
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntologyManager,
            Sentiment,
        )

        manager = BusinessOntologyManager(conn)
        ontology = manager.create_ontology("Multi-Context", "mixed")

        # White-glove: low tickets = bad
        manager.add_metric_interpretation(
            ontology_id=ontology.ontology_id,
            metric_id="support_tickets",
            context="white_glove",
            increase_means="Good customer engagement",
            decrease_means="WARNING: Customer disengagement",
        )

        # Self-service: low tickets = good
        manager.add_metric_interpretation(
            ontology_id=ontology.ontology_id,
            metric_id="support_tickets",
            context="self_service",
            increase_means="WARNING: Product issues",
            decrease_means="Great! Product working well",
        )

        # Same metric drop, different interpretations
        white_glove_result = manager.interpret_metric_change(
            metric_id="support_tickets",
            change_percent=-45,
            context="white_glove",
            ontology_id=ontology.ontology_id,
        )

        self_service_result = manager.interpret_metric_change(
            metric_id="support_tickets",
            change_percent=-45,
            context="self_service",
            ontology_id=ontology.ontology_id,
        )

        # White-glove: bad
        assert white_glove_result.sentiment == Sentiment.WARNING

        # Self-service: good
        assert self_service_result.sentiment == Sentiment.POSITIVE

    def test_interpret_no_context(self, conn):
        """Test interpretation when no context matches."""
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntologyManager,
            Sentiment,
        )

        manager = BusinessOntologyManager(conn)

        result = manager.interpret_metric_change(
            metric_id="unknown_metric",
            change_percent=-10,
            context="unknown_context",
        )

        assert result.sentiment == Sentiment.NEUTRAL
        assert result.confidence == 0.0

    def test_get_causal_explanation(self, conn):
        """Test getting causal explanation between concepts."""
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntologyManager,
            CausalType,
            CausalDirection,
        )

        manager = BusinessOntologyManager(conn)
        ontology = manager.create_ontology("Test", "test")

        manager.add_concept(ontology.ontology_id, "engagement", "Engagement")
        manager.add_concept(ontology.ontology_id, "health", "Health")
        manager.add_concept(ontology.ontology_id, "revenue", "Revenue")

        # Engagement -> Health -> Revenue
        manager.add_causal_relationship(
            ontology.ontology_id, "engagement", "health",
            CausalType.INDICATES, CausalDirection.POSITIVE,
        )
        manager.add_causal_relationship(
            ontology.ontology_id, "health", "revenue",
            CausalType.CAUSES, CausalDirection.POSITIVE,
        )

        explanations = manager.get_causal_explanation(
            source_concept_id="engagement",
            target_concept_id="revenue",
            ontology_id=ontology.ontology_id,
        )

        assert len(explanations) >= 1

    def test_load_ontology_from_yaml(self, conn):
        """Test loading ontology from YAML string."""
        from mdde.semantic.ontology.business_ontology import BusinessOntologyManager

        yaml_content = """
ontology_id: bo_loaded
name: Loaded Ontology
business_model: saas
concepts:
  - concept_id: customer
    name: Customer
    definition: A subscriber
interpretations:
  - interpretation_id: mi_1
    metric_id: mrr
    context: saas
    increase_means: Revenue growing
    decrease_means: Revenue declining
relationships: []
"""

        manager = BusinessOntologyManager(conn)
        ontology = manager.load_ontology_from_yaml(yaml_content)

        assert ontology.name == "Loaded Ontology"
        assert len(ontology.concepts) == 1
        assert len(ontology.interpretations) == 1


class TestOntologyQuestionnaire:
    """Tests for OntologyQuestionnaire."""

    def test_get_standard_questions(self):
        """Test getting standard questions."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire

        q = OntologyQuestionnaire()
        questions = q.get_questions()

        assert len(questions) >= 20
        assert questions[0].question_id == "Q01"

    def test_set_answer(self):
        """Test setting answers."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire

        q = OntologyQuestionnaire()
        q.set_answer("Q01", "b2b_enterprise")
        q.set_answer("Q02", "Fortune 500 companies")

        assert q._answers["Q01"] == "b2b_enterprise"
        assert q._answers["Q02"] == "Fortune 500 companies"

    def test_generate_ontology_minimal(self):
        """Test generating ontology with minimal answers."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire

        q = OntologyQuestionnaire()
        q.set_answers({
            "Q01": "b2b_enterprise",
            "Q02": "Enterprise customers",
            "Q03": "Reduced costs",
            "Q04": ["ARR", "NPS"],
            "Q05": "high_engagement_good",
        })

        ontology = q.generate_ontology("Test Enterprise Ontology")

        assert ontology.name == "Test Enterprise Ontology"
        assert ontology.business_model == "b2b_enterprise"
        assert len(ontology.concepts) >= 3  # customer, engagement, health

    def test_generate_ontology_with_interpretations(self):
        """Test that generated ontology includes interpretations."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire

        q = OntologyQuestionnaire()
        q.set_answers({
            "Q01": "b2b_enterprise",
            "Q02": "Enterprise buyers",
            "Q05": "high_engagement_good",
            "Q13": "white_glove",
        })

        ontology = q.generate_ontology()

        # Should have support ticket interpretation
        assert len(ontology.interpretations) >= 1

        # Check interpretation matches business model
        support_interp = None
        for interp in ontology.interpretations:
            if interp.metric_id == "support_tickets":
                support_interp = interp
                break

        assert support_interp is not None
        assert "disengagement" in support_interp.decrease_means.lower()

    def test_generate_ontology_self_service_model(self):
        """Test ontology generation for self-service model."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire

        q = OntologyQuestionnaire()
        q.set_answers({
            "Q01": "b2c_transactional",
            "Q02": "Individual consumers",
            "Q05": "low_contact_good",  # Self-service model
            "Q13": "self_service",
        })

        ontology = q.generate_ontology()

        # Check that interpretation is inverted
        support_interp = None
        for interp in ontology.interpretations:
            if interp.metric_id == "support_tickets":
                support_interp = interp
                break

        assert support_interp is not None
        # In self-service, decrease is GOOD
        assert "working well" in support_interp.decrease_means.lower()
        # Increase is WARNING
        assert "warning" in support_interp.increase_means.lower()

    def test_generate_causal_relationships(self):
        """Test that questionnaire generates causal relationships."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire
        from mdde.semantic.ontology.business_ontology import CausalDirection

        q = OntologyQuestionnaire()
        q.set_answers({
            "Q01": "saas",
            "Q05": "high_engagement_good",
            "Q10": "direct",  # Satisfaction -> Revenue
            "Q18": ["Feature adoption"],  # Leading indicators
            "Q04": ["ARR"],  # Success metrics
        })

        ontology = q.generate_ontology()

        # Should have engagement -> health relationship
        engagement_health = None
        for rel in ontology.relationships:
            if rel.source_concept_id == "engagement":
                engagement_health = rel
                break

        assert engagement_health is not None
        assert engagement_health.direction == CausalDirection.POSITIVE

    def test_industry_questions(self):
        """Test industry-specific questions."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire

        q = OntologyQuestionnaire()

        retail_questions = q.get_questions(industry="retail")
        assert any("order value" in q.question.lower() for q in retail_questions)

        healthcare_questions = q.get_questions(industry="healthcare")
        assert any("hipaa" in q.question.lower() for q in healthcare_questions)

        finance_questions = q.get_questions(industry="finance")
        assert any("regulatory" in q.question.lower() for q in finance_questions)

    def test_completion_percentage(self):
        """Test completion tracking."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire

        q = OntologyQuestionnaire()

        assert q.get_completion_percentage() == 0

        q.set_answer("Q01", "saas")
        q.set_answer("Q02", "Developers")

        percentage = q.get_completion_percentage()
        assert percentage > 0
        assert percentage < 100

    def test_unanswered_required(self):
        """Test getting unanswered required questions."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire

        q = OntologyQuestionnaire()

        unanswered = q.get_unanswered_required()
        assert len(unanswered) > 0

        # Answer all required
        for question in q.get_questions():
            if question.required:
                q.set_answer(question.question_id, "test")

        unanswered = q.get_unanswered_required()
        assert len(unanswered) == 0

    def test_questionnaire_to_yaml(self):
        """Test YAML export."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire

        q = OntologyQuestionnaire()
        q.set_answer("Q01", "saas")

        yaml_str = q.to_yaml()
        assert "questions:" in yaml_str
        assert "answers:" in yaml_str
        assert "saas" in yaml_str


class TestIntegration:
    """Integration tests for business ontology workflow."""

    @pytest.fixture
    def conn(self):
        """Create test database."""
        return duckdb.connect(":memory:")

    def test_questionnaire_to_manager_workflow(self, conn):
        """Test full workflow from questionnaire to manager."""
        from mdde.semantic.ontology.questionnaire import OntologyQuestionnaire
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntologyManager,
            Sentiment,
        )

        # Step 1: Fill questionnaire
        q = OntologyQuestionnaire()
        q.set_answers({
            "Q01": "b2b_enterprise",
            "Q02": "Enterprise companies",
            "Q03": "Reduced operational costs",
            "Q04": ["ARR", "NPS", "Customer Retention"],
            "Q05": "high_engagement_good",
            "Q07": ["Poor onboarding", "Lack of adoption"],
            "Q10": "direct",
            "Q13": "white_glove",
        })

        # Step 2: Generate ontology
        ontology = q.generate_ontology("Enterprise CRM Ontology")

        # Step 3: Load into manager
        manager = BusinessOntologyManager(conn)
        manager._ontologies[ontology.ontology_id] = ontology

        # Step 4: Use for interpretation
        result = manager.interpret_metric_change(
            metric_id="support_tickets",
            change_percent=-45,
            context=ontology.business_model,
            ontology_id=ontology.ontology_id,
        )

        # Verify correct interpretation for enterprise model
        assert result.sentiment == Sentiment.WARNING
        assert result.confidence > 0

    def test_yaml_roundtrip(self, conn):
        """Test YAML save and load roundtrip."""
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntology,
            BusinessConcept,
            CausalRelationship,
            MetricInterpretation,
            CausalType,
            CausalDirection,
        )

        # Create ontology
        original = BusinessOntology(
            ontology_id="bo_test",
            name="Test Ontology",
            business_model="test",
        )
        original.concepts.append(BusinessConcept(
            concept_id="c1",
            name="Concept 1",
            definition="First concept",
        ))
        original.relationships.append(CausalRelationship(
            relationship_id="r1",
            source_concept_id="c1",
            target_concept_id="c2",
            relationship_type=CausalType.CAUSES,
            direction=CausalDirection.POSITIVE,
        ))
        original.interpretations.append(MetricInterpretation(
            interpretation_id="mi1",
            metric_id="metric1",
            context="test",
            increase_means="Good",
            decrease_means="Bad",
        ))

        # Save to YAML
        yaml_str = original.to_yaml()

        # Load from YAML
        loaded = BusinessOntology.from_yaml(yaml_str)

        # Verify
        assert loaded.name == original.name
        assert len(loaded.concepts) == 1
        assert len(loaded.relationships) == 1
        assert len(loaded.interpretations) == 1

    def test_mundane_misdirection_scenario(self, conn):
        """
        Test the 'Mundane Misdirection' scenario from dltHub article.

        Support tickets drop 45%, response times halve, CSAT improves.
        - Without ontology: Celebrates success
        - With ontology: Detects customer disengagement
        """
        from mdde.semantic.ontology.business_ontology import (
            BusinessOntologyManager,
            Sentiment,
        )

        manager = BusinessOntologyManager(conn)

        # Create enterprise white-glove ontology
        ontology = manager.create_ontology(
            name="Enterprise White-Glove",
            business_model="enterprise_white_glove",
            description="High-touch enterprise model where engagement = health",
        )

        # Add the critical interpretation
        manager.add_metric_interpretation(
            ontology_id=ontology.ontology_id,
            metric_id="support_tickets",
            context="enterprise_white_glove",
            increase_means="Customers actively using premium support services",
            decrease_means="WARNING: Customer disengagement - premium support not being utilized",
            recommended_actions={
                "decrease_True": "Proactive outreach to check customer satisfaction",
                "default": "Monitor",
            },
            confidence=0.9,
        )

        # The scenario: 45% drop in tickets
        result = manager.interpret_metric_change(
            metric_id="support_tickets",
            change_percent=-45,
            context="enterprise_white_glove",
            ontology_id=ontology.ontology_id,
        )

        # AI WITH ontology should flag this as a warning
        assert result.sentiment == Sentiment.WARNING
        assert "disengagement" in result.explanation.lower()
        assert "outreach" in result.recommended_action.lower()
        assert result.confidence >= 0.9


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
