"""
Verified Query Retriever.

Retrieves relevant verified queries for a given question using
similarity-based matching (ADR-375).
"""

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Callable, Dict, List, Optional, Set

from .repository import VerifiedQueryRepository
from .types import QuestionIntent, RetrievalResult, VerifiedQuery


@dataclass
class RetrieverConfig:
    """Configuration for query retrieval."""

    # Weights for scoring
    text_similarity_weight: float = 0.4
    intent_match_weight: float = 0.2
    entity_overlap_weight: float = 0.2
    keyword_match_weight: float = 0.2

    # Thresholds
    min_similarity_score: float = 0.3
    max_results: int = 5

    # Use embeddings if available
    use_embeddings: bool = True


class VerifiedQueryRetriever:
    """
    Retrieve relevant verified queries for text-to-SQL generation.

    When a new question comes in, retrieves the most relevant verified
    examples to inject as context for the LLM.

    Usage:
        retriever = VerifiedQueryRetriever(repository)

        # Retrieve similar queries
        results = retriever.retrieve(
            question="Top 10 customers by revenue this year",
            semantic_model_id="sales_model",
            top_k=3
        )

        # Use in prompt
        for r in results:
            print(f"Example (score={r.similarity_score:.2f}):")
            print(f"  Q: {r.query.question}")
            print(f"  SQL: {r.query.sql[:100]}...")
    """

    # Intent keywords for classification
    INTENT_KEYWORDS: Dict[QuestionIntent, Set[str]] = {
        QuestionIntent.RANKING: {
            "top", "bottom", "best", "worst", "highest", "lowest",
            "most", "least", "rank", "leading", "largest", "smallest"
        },
        QuestionIntent.TREND: {
            "trend", "over time", "monthly", "weekly", "daily", "yearly",
            "growth", "decline", "change", "progression", "history"
        },
        QuestionIntent.COMPARISON: {
            "compare", "vs", "versus", "difference", "between",
            "against", "relative", "compared to"
        },
        QuestionIntent.AGGREGATION: {
            "total", "sum", "count", "average", "mean", "aggregate",
            "how many", "how much"
        },
        QuestionIntent.FILTERING: {
            "where", "filter", "only", "specific", "particular",
            "segment", "subset"
        },
        QuestionIntent.LOOKUP: {
            "show me", "find", "get", "what is", "details", "info"
        },
        QuestionIntent.DISTRIBUTION: {
            "breakdown", "distribution", "by category", "split",
            "grouped by", "per"
        },
        QuestionIntent.CORRELATION: {
            "correlation", "relationship", "related", "impact",
            "affect", "influence"
        },
    }

    def __init__(
        self,
        repository: VerifiedQueryRepository,
        config: Optional[RetrieverConfig] = None,
        embedding_fn: Optional[Callable[[str], List[float]]] = None,
    ):
        """
        Initialize retriever.

        Args:
            repository: Verified query repository
            config: Retrieval configuration
            embedding_fn: Optional function to compute embeddings
        """
        self.repository = repository
        self.config = config or RetrieverConfig()
        self.embedding_fn = embedding_fn

    def retrieve(
        self,
        question: str,
        semantic_model_id: str,
        top_k: Optional[int] = None,
        intent_hint: Optional[QuestionIntent] = None,
        entity_hints: Optional[List[str]] = None,
    ) -> List[RetrievalResult]:
        """
        Retrieve most relevant verified queries for a question.

        Args:
            question: Natural language question
            semantic_model_id: Semantic model to search
            top_k: Number of results (default from config)
            intent_hint: Optional intent classification hint
            entity_hints: Optional entity name hints

        Returns:
            List of RetrievalResult sorted by relevance
        """
        top_k = top_k or self.config.max_results

        # Get all verified queries for this model
        candidates = self.repository.list_queries(
            semantic_model_id=semantic_model_id,
            limit=500,  # Get more candidates for scoring
        )

        if not candidates:
            return []

        # Classify question intent
        detected_intent = intent_hint or self._classify_intent(question)

        # Extract keywords
        question_keywords = self._extract_keywords(question)

        # Score each candidate
        scored: List[RetrievalResult] = []

        for candidate in candidates:
            score, reasons = self._score_candidate(
                question=question,
                question_keywords=question_keywords,
                detected_intent=detected_intent,
                entity_hints=entity_hints or [],
                candidate=candidate,
            )

            if score >= self.config.min_similarity_score:
                scored.append(RetrievalResult(
                    query=candidate,
                    similarity_score=score,
                    match_reasons=reasons,
                ))

        # Sort by score and return top_k
        scored.sort(key=lambda r: r.similarity_score, reverse=True)
        return scored[:top_k]

    def retrieve_for_entity(
        self,
        entity_name: str,
        semantic_model_id: str,
        top_k: int = 5,
    ) -> List[RetrievalResult]:
        """Retrieve queries that demonstrate usage of a specific entity."""
        candidates = self.repository.list_by_entity(
            semantic_model_id=semantic_model_id,
            entity_name=entity_name,
            limit=top_k * 2,
        )

        results = []
        for candidate in candidates[:top_k]:
            results.append(RetrievalResult(
                query=candidate,
                similarity_score=1.0,
                match_reasons=[f"References entity: {entity_name}"],
            ))

        return results

    def retrieve_by_intent(
        self,
        intent: QuestionIntent,
        semantic_model_id: str,
        top_k: int = 5,
    ) -> List[RetrievalResult]:
        """Retrieve queries by question intent."""
        candidates = self.repository.list_queries(
            semantic_model_id=semantic_model_id,
            intent=intent,
            limit=top_k,
        )

        results = []
        for candidate in candidates:
            results.append(RetrievalResult(
                query=candidate,
                similarity_score=1.0,
                match_reasons=[f"Intent: {intent.value}"],
            ))

        return results

    def _score_candidate(
        self,
        question: str,
        question_keywords: Set[str],
        detected_intent: Optional[QuestionIntent],
        entity_hints: List[str],
        candidate: VerifiedQuery,
    ) -> tuple[float, List[str]]:
        """Score a candidate query for relevance."""
        config = self.config
        score = 0.0
        reasons: List[str] = []

        # 1. Text similarity
        text_sim = self._text_similarity(question, candidate.question)
        score += text_sim * config.text_similarity_weight
        if text_sim > 0.5:
            reasons.append(f"Text similarity: {text_sim:.0%}")

        # Also check question variations
        for variation in candidate.question_variations:
            var_sim = self._text_similarity(question, variation)
            if var_sim > text_sim:
                text_sim = var_sim
                score = text_sim * config.text_similarity_weight
                if text_sim > 0.5:
                    reasons.append(f"Matches variation: {text_sim:.0%}")
                break

        # 2. Intent match
        if detected_intent and candidate.intent == detected_intent:
            score += config.intent_match_weight
            reasons.append(f"Intent match: {detected_intent.value}")
        elif detected_intent:
            # Partial credit for related intents
            if self._intents_related(detected_intent, candidate.intent):
                score += config.intent_match_weight * 0.5
                reasons.append(f"Related intent: {candidate.intent.value}")

        # 3. Entity overlap
        if entity_hints:
            overlap = len(set(entity_hints) & set(candidate.entities))
            if overlap > 0:
                entity_score = overlap / max(len(entity_hints), len(candidate.entities))
                score += entity_score * config.entity_overlap_weight
                reasons.append(f"Entity overlap: {overlap} entities")

        # 4. Keyword match
        candidate_keywords = self._extract_keywords(candidate.question)
        keyword_overlap = len(question_keywords & candidate_keywords)
        if keyword_overlap > 0:
            keyword_score = keyword_overlap / max(
                len(question_keywords), len(candidate_keywords), 1
            )
            score += keyword_score * config.keyword_match_weight
            if keyword_score > 0.3:
                reasons.append(f"Keyword match: {keyword_overlap} keywords")

        # Normalize score to 0-1
        score = min(score, 1.0)

        return score, reasons

    def _classify_intent(self, question: str) -> Optional[QuestionIntent]:
        """Classify question intent from keywords."""
        question_lower = question.lower()

        best_intent = None
        best_count = 0

        for intent, keywords in self.INTENT_KEYWORDS.items():
            count = sum(1 for kw in keywords if kw in question_lower)
            if count > best_count:
                best_count = count
                best_intent = intent

        return best_intent

    def _extract_keywords(self, text: str) -> Set[str]:
        """Extract significant keywords from text."""
        # Normalize
        text = text.lower()

        # Remove common stop words
        stop_words = {
            "the", "a", "an", "is", "are", "was", "were", "be", "been",
            "being", "have", "has", "had", "do", "does", "did", "will",
            "would", "could", "should", "may", "might", "must", "shall",
            "can", "need", "to", "of", "in", "for", "on", "with", "at",
            "by", "from", "as", "into", "through", "during", "before",
            "after", "above", "below", "between", "under", "again",
            "further", "then", "once", "here", "there", "when", "where",
            "why", "how", "all", "each", "few", "more", "most", "other",
            "some", "such", "no", "nor", "not", "only", "own", "same",
            "so", "than", "too", "very", "just", "and", "but", "if",
            "or", "because", "until", "while", "what", "which", "who",
            "this", "that", "these", "those", "me", "my", "show", "give",
        }

        # Extract words
        words = re.findall(r'\b[a-z]+\b', text)
        keywords = {w for w in words if w not in stop_words and len(w) > 2}

        return keywords

    def _text_similarity(self, text1: str, text2: str) -> float:
        """Calculate text similarity using SequenceMatcher."""
        return SequenceMatcher(
            None,
            text1.lower(),
            text2.lower()
        ).ratio()

    def _intents_related(
        self,
        intent1: QuestionIntent,
        intent2: QuestionIntent,
    ) -> bool:
        """Check if two intents are related."""
        related_pairs = {
            (QuestionIntent.RANKING, QuestionIntent.AGGREGATION),
            (QuestionIntent.TREND, QuestionIntent.COMPARISON),
            (QuestionIntent.DISTRIBUTION, QuestionIntent.AGGREGATION),
            (QuestionIntent.FILTERING, QuestionIntent.LOOKUP),
        }

        pair = tuple(sorted([intent1, intent2], key=lambda x: x.value))
        return pair in related_pairs

    def format_for_prompt(
        self,
        results: List[RetrievalResult],
        include_sql: bool = True,
        max_sql_length: int = 500,
    ) -> str:
        """
        Format retrieval results for LLM prompt injection.

        Args:
            results: Retrieved queries
            include_sql: Include SQL in output
            max_sql_length: Truncate SQL to this length

        Returns:
            Formatted string for prompt
        """
        if not results:
            return ""

        lines = ["## Verified Examples", ""]

        for i, result in enumerate(results, 1):
            q = result.query
            lines.append(f"### Example {i}")
            lines.append(f"**Question:** {q.question}")

            if include_sql:
                sql = q.sql
                if len(sql) > max_sql_length:
                    sql = sql[:max_sql_length] + "..."
                lines.append(f"**SQL:**\n```sql\n{sql}\n```")

            lines.append(f"**Intent:** {q.intent.value}")
            if q.entities:
                lines.append(f"**Entities:** {', '.join(q.entities)}")
            lines.append("")

        return "\n".join(lines)
