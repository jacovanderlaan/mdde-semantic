"""
Verified Query Repository Module (ADR-375).

Verified queries are question-SQL pairs attached to semantic models
that improve text-to-SQL accuracy through example-based learning.

Key Concepts:
- VerifiedQuery: A question with its correct SQL answer
- Repository: Storage and retrieval of verified queries
- Retriever: Find similar queries for prompt injection
- Promoter: Auto-promote popular queries to verified status

Usage:
    from mdde.semantic.verified import (
        VerifiedQuery,
        VerifiedQueryRepository,
        VerifiedQueryRetriever,
        VerifiedQueryPromoter,
        QuestionIntent,
    )

    # Store verified queries
    repo = VerifiedQueryRepository(conn)
    repo.add_query(VerifiedQuery(
        query_id="vq_001",
        question="Top customers by revenue",
        sql="SELECT ...",
        intent=QuestionIntent.RANKING,
        semantic_model_id="sales_model",
        entities=["dim_customer", "fact_orders"],
    ))

    # Retrieve for text-to-SQL
    retriever = VerifiedQueryRetriever(repo)
    examples = retriever.retrieve(
        question="Best products last month",
        semantic_model_id="sales_model",
        top_k=3
    )

    # Inject into prompt
    prompt_context = retriever.format_for_prompt(examples)

    # Auto-promote from usage
    promoter = VerifiedQueryPromoter(repo, conn)
    candidates = promoter.find_candidates("sales_model")
    promoter.promote_candidates(candidates, "sales_model", approved_by="admin")
"""

from .promoter import (
    PromoterConfig,
    VerifiedQueryPromoter,
)
from .repository import VerifiedQueryRepository
from .retriever import (
    RetrieverConfig,
    VerifiedQueryRetriever,
)
from .types import (
    PromotionCandidate,
    QueryComplexity,
    QuerySource,
    QuestionIntent,
    RepositoryStats,
    RetrievalResult,
    VerificationStatus,
    VerifiedQuery,
)

__all__ = [
    # Types
    "VerifiedQuery",
    "QuestionIntent",
    "QueryComplexity",
    "QuerySource",
    "VerificationStatus",
    "RetrievalResult",
    "PromotionCandidate",
    "RepositoryStats",
    # Repository
    "VerifiedQueryRepository",
    # Retriever
    "VerifiedQueryRetriever",
    "RetrieverConfig",
    # Promoter
    "VerifiedQueryPromoter",
    "PromoterConfig",
]
