"""
Knowledge Graph Export Module (ADR-200).

Exports MDDE metadata as semantic web formats for integration with
knowledge graphs, ontologies, and AI systems.

Supported Formats:
- RDF/Turtle (.ttl)
- OWL Ontology (.owl)
- JSON-LD (.jsonld)
- N-Triples (.nt)

Usage:
    from mdde.semantic.export import (
        RDFExporter,
        OWLExporter,
        JSONLDExporter,
        export_knowledge_graph,
    )

    # Export to RDF/Turtle
    exporter = RDFExporter(conn)
    turtle = exporter.export_model("MDL_sales")

    # Export with ontology
    owl_exporter = OWLExporter(conn)
    owl_exporter.export_to_file("sales_ontology.owl")

    # Quick export
    export_knowledge_graph(conn, "MDL_sales", format="turtle", output="sales.ttl")
"""

from .rdf_exporter import (
    RDFExporter,
    RDFFormat,
    export_to_rdf,
)
from .owl_exporter import (
    OWLExporter,
    export_ontology,
)
from .jsonld_exporter import (
    JSONLDExporter,
    JSONLDContext,
    export_to_jsonld,
)
from .knowledge_graph import (
    KnowledgeGraphExporter,
    export_knowledge_graph,
    ExportConfig,
    ExportFormat,
    ExportResult,
)

__all__ = [
    # RDF
    "RDFExporter",
    "RDFFormat",
    "export_to_rdf",
    # OWL
    "OWLExporter",
    "export_ontology",
    # JSON-LD
    "JSONLDExporter",
    "JSONLDContext",
    "export_to_jsonld",
    # Knowledge Graph
    "KnowledgeGraphExporter",
    "export_knowledge_graph",
    "ExportConfig",
    "ExportFormat",
    "ExportResult",
]
