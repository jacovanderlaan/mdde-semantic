"""
Ontology Questionnaire Generator (ADR-362).

Generate business ontology through structured questionnaire.
Inspired by dltHub's "20-Question Ontology Prompt" concept.

Feb 2026
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import yaml

from .business_ontology import (
    BusinessOntology,
    BusinessConcept,
    CausalRelationship,
    MetricInterpretation,
    CausalType,
    CausalDirection,
    Threshold,
    Sentiment,
    _generate_id,
)

logger = logging.getLogger(__name__)


class QuestionCategory(Enum):
    """Categories of ontology questions."""

    BUSINESS_MODEL = "business_model"
    CUSTOMER = "customer"
    SUCCESS_METRICS = "success_metrics"
    PROCESSES = "processes"
    HEALTH_INDICATORS = "health_indicators"
    RELATIONSHIPS = "relationships"
    RISKS = "risks"
    CONTEXT = "context"


class AnswerType(Enum):
    """Types of answers expected."""

    TEXT = "text"  # Free text
    SELECT = "select"  # Single selection
    MULTI_SELECT = "multi_select"  # Multiple selections
    BOOLEAN = "boolean"  # Yes/No
    NUMERIC = "numeric"  # Number
    LIST = "list"  # List of items


@dataclass
class QuestionOption:
    """An option for select-type questions."""

    value: str
    label: str
    description: str = ""


@dataclass
class OntologyQuestion:
    """A question for ontology generation."""

    question_id: str
    question: str
    category: QuestionCategory
    captures: str  # What this question captures
    answer_type: AnswerType
    required: bool = True
    options: List[QuestionOption] = field(default_factory=list)
    examples: List[str] = field(default_factory=list)
    follow_up_on: Optional[str] = None  # Question ID this follows up
    help_text: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "question_id": self.question_id,
            "question": self.question,
            "category": self.category.value,
            "captures": self.captures,
            "answer_type": self.answer_type.value,
            "required": self.required,
            "options": [
                {"value": o.value, "label": o.label, "description": o.description}
                for o in self.options
            ],
            "examples": self.examples,
            "help_text": self.help_text,
        }


@dataclass
class QuestionAnswer:
    """An answer to a question."""

    question_id: str
    answer: Any  # Type depends on question


# Standard questions for business ontology generation
STANDARD_QUESTIONS = [
    OntologyQuestion(
        question_id="Q01",
        question="What business are you in?",
        category=QuestionCategory.BUSINESS_MODEL,
        captures="business_model",
        answer_type=AnswerType.SELECT,
        options=[
            QuestionOption("b2b_enterprise", "B2B Enterprise", "High-touch, relationship-based sales"),
            QuestionOption("b2b_smb", "B2B SMB", "Self-serve or low-touch B2B"),
            QuestionOption("b2c_subscription", "B2C Subscription", "Consumer subscription model"),
            QuestionOption("b2c_transactional", "B2C Transactional", "E-commerce, one-time purchases"),
            QuestionOption("marketplace", "Marketplace", "Two-sided platform"),
            QuestionOption("saas", "SaaS", "Software-as-a-Service"),
            QuestionOption("other", "Other", "Custom business model"),
        ],
        examples=["Enterprise SaaS", "E-commerce marketplace"],
    ),
    OntologyQuestion(
        question_id="Q02",
        question="Who are your primary customers?",
        category=QuestionCategory.CUSTOMER,
        captures="customer_definition",
        answer_type=AnswerType.TEXT,
        examples=["Fortune 500 enterprises", "Small business owners", "Individual consumers"],
        help_text="Describe your ideal customer profile",
    ),
    OntologyQuestion(
        question_id="Q03",
        question="What does success look like for your customers?",
        category=QuestionCategory.SUCCESS_METRICS,
        captures="customer_success_definition",
        answer_type=AnswerType.TEXT,
        examples=["Reduced operational costs", "Increased revenue", "Better customer satisfaction"],
    ),
    OntologyQuestion(
        question_id="Q04",
        question="What are your key success metrics?",
        category=QuestionCategory.SUCCESS_METRICS,
        captures="success_metrics",
        answer_type=AnswerType.LIST,
        examples=["ARR", "NPS", "Customer Retention", "Daily Active Users"],
        help_text="List 3-5 key metrics that define business success",
    ),
    OntologyQuestion(
        question_id="Q05",
        question="How do you measure customer health?",
        category=QuestionCategory.HEALTH_INDICATORS,
        captures="health_indicators",
        answer_type=AnswerType.SELECT,
        options=[
            QuestionOption(
                "high_engagement_good",
                "High engagement = healthy",
                "B2B enterprise, high-touch models where engagement signals satisfaction"
            ),
            QuestionOption(
                "low_contact_good",
                "Low contact = healthy",
                "Self-service models where 'just works' is ideal"
            ),
            QuestionOption(
                "usage_based",
                "Usage volume = health",
                "Consumption-based models"
            ),
            QuestionOption(
                "outcome_based",
                "Outcomes achieved = health",
                "ROI or goal-based models"
            ),
        ],
        help_text="This determines how support ticket volume is interpreted",
    ),
    OntologyQuestion(
        question_id="Q06",
        question="What are your key business processes?",
        category=QuestionCategory.PROCESSES,
        captures="process_ontology",
        answer_type=AnswerType.LIST,
        examples=["Lead → Opportunity → Customer", "Order → Fulfillment → Delivery"],
        help_text="Describe 2-3 core business processes",
    ),
    OntologyQuestion(
        question_id="Q07",
        question="What causes customers to leave (churn)?",
        category=QuestionCategory.RISKS,
        captures="churn_causes",
        answer_type=AnswerType.LIST,
        examples=["Poor onboarding", "Lack of feature adoption", "Budget constraints"],
        help_text="List the main reasons customers churn",
    ),
    OntologyQuestion(
        question_id="Q08",
        question="What indicates a customer is at risk?",
        category=QuestionCategory.HEALTH_INDICATORS,
        captures="risk_indicators",
        answer_type=AnswerType.LIST,
        examples=["Declining usage", "Support escalations", "Missed renewals"],
        follow_up_on="Q07",
    ),
    OntologyQuestion(
        question_id="Q09",
        question="What actions lead to customer success?",
        category=QuestionCategory.RELATIONSHIPS,
        captures="success_drivers",
        answer_type=AnswerType.LIST,
        examples=["Quick time-to-value", "Regular check-ins", "Feature adoption"],
    ),
    OntologyQuestion(
        question_id="Q10",
        question="How does customer satisfaction relate to revenue?",
        category=QuestionCategory.RELATIONSHIPS,
        captures="satisfaction_revenue_relationship",
        answer_type=AnswerType.SELECT,
        options=[
            QuestionOption("direct", "Direct relationship", "Higher satisfaction directly drives revenue"),
            QuestionOption("lagging", "Lagging indicator", "Satisfaction affects renewal/expansion"),
            QuestionOption("indirect", "Indirect", "Satisfaction affects referrals"),
            QuestionOption("complex", "Complex", "Multiple pathways"),
        ],
    ),
    OntologyQuestion(
        question_id="Q11",
        question="What is the typical customer journey length?",
        category=QuestionCategory.CONTEXT,
        captures="customer_journey_length",
        answer_type=AnswerType.SELECT,
        options=[
            QuestionOption("short", "Short (< 1 month)", "Quick sales cycle"),
            QuestionOption("medium", "Medium (1-6 months)", "Moderate sales cycle"),
            QuestionOption("long", "Long (6+ months)", "Enterprise sales cycle"),
        ],
    ),
    OntologyQuestion(
        question_id="Q12",
        question="Is your product mission-critical for customers?",
        category=QuestionCategory.CONTEXT,
        captures="product_criticality",
        answer_type=AnswerType.BOOLEAN,
        help_text="Does customer business depend on your product?",
    ),
    OntologyQuestion(
        question_id="Q13",
        question="What is your support model?",
        category=QuestionCategory.BUSINESS_MODEL,
        captures="support_model",
        answer_type=AnswerType.SELECT,
        options=[
            QuestionOption("white_glove", "White-glove", "Dedicated support, high-touch"),
            QuestionOption("tiered", "Tiered support", "Different levels based on plan"),
            QuestionOption("self_service", "Self-service", "Documentation, community, automation"),
            QuestionOption("hybrid", "Hybrid", "Mix of self-service and human support"),
        ],
    ),
    OntologyQuestion(
        question_id="Q14",
        question="What are your primary data sources?",
        category=QuestionCategory.CONTEXT,
        captures="data_sources",
        answer_type=AnswerType.LIST,
        examples=["CRM (Salesforce)", "Product analytics", "Support tickets"],
    ),
    OntologyQuestion(
        question_id="Q15",
        question="How frequently do business conditions change?",
        category=QuestionCategory.CONTEXT,
        captures="change_frequency",
        answer_type=AnswerType.SELECT,
        options=[
            QuestionOption("stable", "Stable", "Business rules rarely change"),
            QuestionOption("seasonal", "Seasonal", "Regular seasonal patterns"),
            QuestionOption("dynamic", "Dynamic", "Frequent changes and pivots"),
        ],
    ),
    OntologyQuestion(
        question_id="Q16",
        question="What external factors affect your metrics?",
        category=QuestionCategory.CONTEXT,
        captures="external_factors",
        answer_type=AnswerType.LIST,
        examples=["Seasonality", "Economic conditions", "Competitor actions"],
    ),
    OntologyQuestion(
        question_id="Q17",
        question="What does 'engagement' mean in your context?",
        category=QuestionCategory.CONTEXT,
        captures="engagement_definition",
        answer_type=AnswerType.TEXT,
        help_text="Define what customer engagement looks like",
    ),
    OntologyQuestion(
        question_id="Q18",
        question="What leading indicators predict success?",
        category=QuestionCategory.RELATIONSHIPS,
        captures="leading_indicators",
        answer_type=AnswerType.LIST,
        examples=["Feature adoption rate", "Time-to-value", "Onboarding completion"],
    ),
    OntologyQuestion(
        question_id="Q19",
        question="What are your key business domains?",
        category=QuestionCategory.CONTEXT,
        captures="business_domains",
        answer_type=AnswerType.LIST,
        examples=["Sales", "Marketing", "Customer Success", "Product", "Finance"],
    ),
    OntologyQuestion(
        question_id="Q20",
        question="Who are the key stakeholders for data decisions?",
        category=QuestionCategory.CONTEXT,
        captures="stakeholders",
        answer_type=AnswerType.LIST,
        examples=["VP Sales", "Head of Customer Success", "CFO"],
    ),
]


class OntologyQuestionnaire:
    """
    Generate business ontology through questionnaire.

    Guides users through structured questions to capture
    business context that AI needs for correct interpretation.
    """

    def __init__(self):
        """Initialize questionnaire."""
        self._questions = STANDARD_QUESTIONS
        self._custom_questions: List[OntologyQuestion] = []
        self._answers: Dict[str, Any] = {}

    def get_questions(
        self,
        industry: Optional[str] = None,
    ) -> List[OntologyQuestion]:
        """
        Get all questions for ontology generation.

        Args:
            industry: Optional industry for customized questions

        Returns:
            List of questions
        """
        questions = list(self._questions)

        # Add industry-specific questions
        if industry:
            industry_questions = self._get_industry_questions(industry)
            questions.extend(industry_questions)

        # Add custom questions
        questions.extend(self._custom_questions)

        return questions

    def _get_industry_questions(self, industry: str) -> List[OntologyQuestion]:
        """Get industry-specific questions."""
        industry_lower = industry.lower()

        if "retail" in industry_lower or "ecommerce" in industry_lower:
            return [
                OntologyQuestion(
                    question_id="IND_RETAIL_01",
                    question="What is your average order value?",
                    category=QuestionCategory.CONTEXT,
                    captures="average_order_value",
                    answer_type=AnswerType.NUMERIC,
                ),
                OntologyQuestion(
                    question_id="IND_RETAIL_02",
                    question="What is your return rate?",
                    category=QuestionCategory.CONTEXT,
                    captures="return_rate",
                    answer_type=AnswerType.NUMERIC,
                ),
            ]
        elif "healthcare" in industry_lower:
            return [
                OntologyQuestion(
                    question_id="IND_HEALTH_01",
                    question="Is your data subject to HIPAA?",
                    category=QuestionCategory.CONTEXT,
                    captures="hipaa_compliance",
                    answer_type=AnswerType.BOOLEAN,
                ),
            ]
        elif "finance" in industry_lower or "fintech" in industry_lower:
            return [
                OntologyQuestion(
                    question_id="IND_FIN_01",
                    question="What regulatory frameworks apply?",
                    category=QuestionCategory.CONTEXT,
                    captures="regulatory_frameworks",
                    answer_type=AnswerType.MULTI_SELECT,
                    options=[
                        QuestionOption("sox", "SOX", "Sarbanes-Oxley"),
                        QuestionOption("pci", "PCI-DSS", "Payment Card Industry"),
                        QuestionOption("gdpr", "GDPR", "General Data Protection"),
                        QuestionOption("basel", "Basel III", "Banking regulations"),
                    ],
                ),
            ]

        return []

    def add_custom_question(self, question: OntologyQuestion) -> None:
        """Add a custom question."""
        self._custom_questions.append(question)

    def set_answer(self, question_id: str, answer: Any) -> None:
        """
        Set an answer to a question.

        Args:
            question_id: Question ID
            answer: Answer value
        """
        self._answers[question_id] = answer

    def set_answers(self, answers: Dict[str, Any]) -> None:
        """
        Set multiple answers.

        Args:
            answers: Dict of question_id -> answer
        """
        self._answers.update(answers)

    def generate_ontology(self, name: str = "Generated Ontology") -> BusinessOntology:
        """
        Generate business ontology from collected answers.

        Args:
            name: Name for the generated ontology

        Returns:
            Generated BusinessOntology
        """
        # Create base ontology
        ontology = BusinessOntology(
            ontology_id=_generate_id("bo_"),
            name=name,
            description="Generated from questionnaire",
            business_model=self._answers.get("Q01", ""),
        )

        # Generate concepts from answers
        self._generate_concepts(ontology)

        # Generate relationships from answers
        self._generate_relationships(ontology)

        # Generate interpretations from answers
        self._generate_interpretations(ontology)

        return ontology

    def _generate_concepts(self, ontology: BusinessOntology) -> None:
        """Generate business concepts from answers."""
        # Customer concept
        customer_def = self._answers.get("Q02", "Customer")
        customer_success = self._answers.get("Q03", "")

        ontology.concepts.append(BusinessConcept(
            concept_id="customer",
            name="Customer",
            definition=f"Primary customer: {customer_def}",
            business_model=ontology.business_model,
            domain="customer_success",
            success_indicators=[customer_success] if customer_success else [],
            failure_indicators=self._answers.get("Q07", []),
        ))

        # Engagement concept
        engagement_def = self._answers.get("Q17", "Customer interaction with product")
        ontology.concepts.append(BusinessConcept(
            concept_id="engagement",
            name="Customer Engagement",
            definition=engagement_def,
            domain="customer_success",
        ))

        # Health concept
        health_model = self._answers.get("Q05", "high_engagement_good")
        health_def = self._get_health_definition(health_model)
        risk_indicators = self._answers.get("Q08", [])

        ontology.concepts.append(BusinessConcept(
            concept_id="customer_health",
            name="Customer Health",
            definition=health_def,
            domain="customer_success",
            failure_indicators=risk_indicators,
        ))

        # Success metrics concepts
        metrics = self._answers.get("Q04", [])
        for metric in metrics:
            ontology.concepts.append(BusinessConcept(
                concept_id=f"metric_{metric.lower().replace(' ', '_')}",
                name=metric,
                definition=f"Key success metric: {metric}",
                domain="metrics",
                key_metrics=[metric],
            ))

        # Business domains
        domains = self._answers.get("Q19", [])
        for domain in domains:
            ontology.concepts.append(BusinessConcept(
                concept_id=f"domain_{domain.lower().replace(' ', '_')}",
                name=domain,
                definition=f"Business domain: {domain}",
                domain="organization",
            ))

    def _get_health_definition(self, health_model: str) -> str:
        """Get health definition based on model."""
        definitions = {
            "high_engagement_good": (
                "High customer engagement indicates healthy relationship. "
                "Low engagement signals disengagement risk."
            ),
            "low_contact_good": (
                "Low support contact indicates product working well. "
                "High contact may indicate issues."
            ),
            "usage_based": (
                "Health determined by product usage volume. "
                "Declining usage indicates risk."
            ),
            "outcome_based": (
                "Health determined by customer achieving their goals. "
                "Track outcome metrics."
            ),
        }
        return definitions.get(health_model, "Customer health status")

    def _generate_relationships(self, ontology: BusinessOntology) -> None:
        """Generate causal relationships from answers."""
        health_model = self._answers.get("Q05", "high_engagement_good")
        sat_revenue = self._answers.get("Q10", "direct")

        # Engagement -> Health relationship
        if health_model == "high_engagement_good":
            ontology.relationships.append(CausalRelationship(
                relationship_id=_generate_id("rel_"),
                source_concept_id="engagement",
                target_concept_id="customer_health",
                relationship_type=CausalType.INDICATES,
                direction=CausalDirection.POSITIVE,
                context=ontology.business_model,
                evidence="High engagement correlates with healthy customers in this model",
            ))
        elif health_model == "low_contact_good":
            ontology.relationships.append(CausalRelationship(
                relationship_id=_generate_id("rel_"),
                source_concept_id="engagement",
                target_concept_id="customer_health",
                relationship_type=CausalType.INDICATES,
                direction=CausalDirection.NEGATIVE,
                context=ontology.business_model,
                evidence="Low contact indicates product working well in self-service model",
            ))

        # Health -> Revenue relationship
        if sat_revenue in ("direct", "lagging"):
            ontology.relationships.append(CausalRelationship(
                relationship_id=_generate_id("rel_"),
                source_concept_id="customer_health",
                target_concept_id="metric_arr" if "ARR" in self._answers.get("Q04", []) else "revenue",
                relationship_type=CausalType.CAUSES,
                direction=CausalDirection.POSITIVE,
                context="Customer health drives revenue through retention and expansion",
            ))

        # Leading indicators -> Success
        leading = self._answers.get("Q18", [])
        success_metrics = self._answers.get("Q04", [])

        for indicator in leading:
            for metric in success_metrics:
                ontology.relationships.append(CausalRelationship(
                    relationship_id=_generate_id("rel_"),
                    source_concept_id=f"metric_{indicator.lower().replace(' ', '_')}",
                    target_concept_id=f"metric_{metric.lower().replace(' ', '_')}",
                    relationship_type=CausalType.PREDICTS,
                    direction=CausalDirection.POSITIVE,
                ))

    def _generate_interpretations(self, ontology: BusinessOntology) -> None:
        """Generate metric interpretations from answers."""
        health_model = self._answers.get("Q05", "high_engagement_good")
        support_model = self._answers.get("Q13", "hybrid")

        # Support ticket interpretation based on business model
        if health_model == "high_engagement_good":
            ontology.interpretations.append(MetricInterpretation(
                interpretation_id=_generate_id("mi_"),
                metric_id="support_tickets",
                context=ontology.business_model,
                increase_means="Engaged customers using premium support services (positive in white-glove model)",
                decrease_means="WARNING: May indicate customer disengagement or declining relationship health",
                thresholds=[
                    Threshold("critical_drop", -30, Sentiment.WARNING, "Significant drop - investigate immediately"),
                    Threshold("normal_range", -10, Sentiment.NEUTRAL, "Normal fluctuation"),
                ],
                recommended_actions={
                    "decrease_True": "Proactive outreach to check customer satisfaction",
                    "increase_True": "Ensure support team is adequately staffed",
                    "default": "Monitor trend",
                },
                confidence=0.85,
            ))
        elif health_model == "low_contact_good":
            ontology.interpretations.append(MetricInterpretation(
                interpretation_id=_generate_id("mi_"),
                metric_id="support_tickets",
                context=ontology.business_model,
                increase_means="WARNING: Potential product issues or user experience problems",
                decrease_means="Product working well, users self-sufficient (positive)",
                thresholds=[
                    Threshold("critical_rise", 30, Sentiment.WARNING, "Significant rise - investigate product issues"),
                    Threshold("normal_range", 10, Sentiment.NEUTRAL, "Normal fluctuation"),
                ],
                recommended_actions={
                    "increase_True": "Investigate product issues and improve documentation",
                    "decrease_True": "Celebrate product quality improvements",
                    "default": "Monitor trend",
                },
                confidence=0.85,
            ))

        # NPS interpretation (if tracked)
        if "NPS" in self._answers.get("Q04", []):
            ontology.interpretations.append(MetricInterpretation(
                interpretation_id=_generate_id("mi_"),
                metric_id="nps_score",
                context=ontology.business_model,
                increase_means="Improving customer satisfaction and likelihood to recommend",
                decrease_means="Declining satisfaction - identify detractors and root causes",
                thresholds=[
                    Threshold("promoter", 50, Sentiment.POSITIVE, "Strong promoter territory"),
                    Threshold("passive", 0, Sentiment.NEUTRAL, "Passive range"),
                    Threshold("detractor", -50, Sentiment.NEGATIVE, "Detractor territory"),
                ],
                recommended_actions={
                    "decrease_True": "Contact detractors, run root cause analysis",
                    "increase_True": "Leverage promoters for referrals and case studies",
                    "default": "Continue monitoring",
                },
                confidence=0.9,
            ))

    def to_yaml(self) -> str:
        """Export questionnaire to YAML."""
        data = {
            "questions": [q.to_dict() for q in self.get_questions()],
            "answers": self._answers,
        }
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

    def from_yaml(self, yaml_content: str) -> None:
        """
        Load answers from YAML.

        Args:
            yaml_content: YAML content
        """
        data = yaml.safe_load(yaml_content)
        self._answers = data.get("answers", {})

    def get_unanswered_required(self) -> List[OntologyQuestion]:
        """Get list of required questions without answers."""
        unanswered = []
        for question in self.get_questions():
            if question.required and question.question_id not in self._answers:
                unanswered.append(question)
        return unanswered

    def get_completion_percentage(self) -> float:
        """Get percentage of questions answered."""
        total = len(self.get_questions())
        answered = len(self._answers)
        return (answered / total * 100) if total > 0 else 0


__all__ = [
    "QuestionCategory",
    "AnswerType",
    "QuestionOption",
    "OntologyQuestion",
    "QuestionAnswer",
    "OntologyQuestionnaire",
    "STANDARD_QUESTIONS",
]
