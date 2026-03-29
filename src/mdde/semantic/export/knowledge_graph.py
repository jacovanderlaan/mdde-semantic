"""
Unified Knowledge Graph Exporter.

High-level interface for exporting MDDE metadata to various
knowledge graph formats.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

from .rdf_exporter import RDFExporter, RDFFormat
from .owl_exporter import OWLExporter
from .jsonld_exporter import JSONLDExporter


class ExportFormat(Enum):
    """Supported export formats."""
    TURTLE = "turtle"
    NTRIPLES = "ntriples"
    OWL = "owl"
    JSONLD = "jsonld"
    ALL = "all"


@dataclass
class ExportConfig:
    """Configuration for knowledge graph export."""
    format: ExportFormat = ExportFormat.TURTLE
    base_uri: str = "https://example.org/mdde/"
    include_ontology: bool = True
    include_instances: bool = True
    include_relationships: bool = True
    include_domains: bool = True
    include_lineage: bool = False
    output_dir: Optional[str] = None
    model_namespace: Optional[str] = None

    def get_file_extension(self) -> str:
        """Get appropriate file extension for format."""
        extensions = {
            ExportFormat.TURTLE: ".ttl",
            ExportFormat.NTRIPLES: ".nt",
            ExportFormat.OWL: ".owl",
            ExportFormat.JSONLD: ".jsonld",
        }
        return extensions.get(self.format, ".txt")


@dataclass
class ExportResult:
    """Result of knowledge graph export."""
    format: ExportFormat
    content: str
    file_path: Optional[str] = None
    triple_count: int = 0
    entity_count: int = 0
    relationship_count: int = 0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "format": self.format.value,
            "file_path": self.file_path,
            "triple_count": self.triple_count,
            "entity_count": self.entity_count,
            "relationship_count": self.relationship_count,
            "errors": self.errors,
        }


class KnowledgeGraphExporter:
    """
    Unified exporter for knowledge graph formats.

    Provides a single interface for exporting MDDE metadata
    to RDF, OWL, and JSON-LD formats.

    Example:
        exporter = KnowledgeGraphExporter(conn)

        # Export to single format
        result = exporter.export("MDL_sales", format=ExportFormat.TURTLE)

        # Export to all formats
        results = exporter.export_all("MDL_sales", output_dir="./exports")

        # Export with custom config
        config = ExportConfig(
            format=ExportFormat.JSONLD,
            base_uri="https://mycompany.com/data/",
            include_lineage=True,
        )
        result = exporter.export("MDL_sales", config=config)
    """

    def __init__(
        self,
        conn: Any = None,
        base_uri: str = "https://example.org/mdde/",
    ):
        """
        Initialize exporter.

        Args:
            conn: DuckDB connection
            base_uri: Base URI for resources
        """
        self.conn = conn
        self.base_uri = base_uri

    def export(
        self,
        model_id: str,
        format: ExportFormat = ExportFormat.TURTLE,
        config: Optional[ExportConfig] = None,
        output_path: Optional[str] = None,
    ) -> ExportResult:
        """
        Export model to specified format.

        Args:
            model_id: Model ID to export
            format: Export format
            config: Optional export configuration
            output_path: Optional output file path

        Returns:
            ExportResult with content and metadata
        """
        config = config or ExportConfig(format=format, base_uri=self.base_uri)

        if format == ExportFormat.TURTLE or format == ExportFormat.NTRIPLES:
            return self._export_rdf(model_id, config, output_path)
        elif format == ExportFormat.OWL:
            return self._export_owl(model_id, config, output_path)
        elif format == ExportFormat.JSONLD:
            return self._export_jsonld(model_id, config, output_path)
        elif format == ExportFormat.ALL:
            # For ALL, return first format; use export_all for multiple
            return self._export_rdf(model_id, config, output_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def export_all(
        self,
        model_id: str,
        output_dir: str,
        config: Optional[ExportConfig] = None,
    ) -> List[ExportResult]:
        """
        Export model to all supported formats.

        Args:
            model_id: Model ID to export
            output_dir: Output directory for files
            config: Optional base configuration

        Returns:
            List of ExportResults, one per format
        """
        config = config or ExportConfig(base_uri=self.base_uri)
        results = []

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        formats = [
            (ExportFormat.TURTLE, f"{model_id}.ttl"),
            (ExportFormat.NTRIPLES, f"{model_id}.nt"),
            (ExportFormat.OWL, f"{model_id}.owl"),
            (ExportFormat.JSONLD, f"{model_id}.jsonld"),
        ]

        for fmt, filename in formats:
            file_path = str(output_path / filename)
            result = self.export(model_id, format=fmt, config=config, output_path=file_path)
            results.append(result)

        return results

    def _export_rdf(
        self,
        model_id: str,
        config: ExportConfig,
        output_path: Optional[str],
    ) -> ExportResult:
        """Export as RDF/Turtle or N-Triples."""
        exporter = RDFExporter(
            conn=self.conn,
            base_uri=config.base_uri,
            model_namespace=config.model_namespace,
        )

        rdf_format = (
            RDFFormat.NTRIPLES if config.format == ExportFormat.NTRIPLES
            else RDFFormat.TURTLE
        )

        content = exporter.export_model(model_id)

        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                if rdf_format == RDFFormat.NTRIPLES:
                    f.write(exporter.graph.to_ntriples())
                else:
                    f.write(content)

        return ExportResult(
            format=config.format,
            content=content,
            file_path=output_path,
            triple_count=len(exporter.graph.triples),
            entity_count=self._count_entities(model_id),
            relationship_count=self._count_relationships(model_id),
        )

    def _export_owl(
        self,
        model_id: str,
        config: ExportConfig,
        output_path: Optional[str],
    ) -> ExportResult:
        """Export as OWL ontology."""
        exporter = OWLExporter(
            conn=self.conn,
            base_uri=config.base_uri,
        )

        content = exporter.export_model(model_id)

        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(content)

        return ExportResult(
            format=ExportFormat.OWL,
            content=content,
            file_path=output_path,
            entity_count=self._count_entities(model_id),
            relationship_count=self._count_relationships(model_id),
        )

    def _export_jsonld(
        self,
        model_id: str,
        config: ExportConfig,
        output_path: Optional[str],
    ) -> ExportResult:
        """Export as JSON-LD."""
        exporter = JSONLDExporter(
            conn=self.conn,
            base_uri=config.base_uri,
        )

        content = exporter.export_model(model_id)

        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(content)

        return ExportResult(
            format=ExportFormat.JSONLD,
            content=content,
            file_path=output_path,
            entity_count=self._count_entities(model_id),
            relationship_count=self._count_relationships(model_id),
        )

    def _count_entities(self, model_id: str) -> int:
        """Count entities in model."""
        if not self.conn:
            return 0
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM metadata.entity WHERE model_id = ?
            """, [model_id]).fetchone()
            return result[0] if result else 0
        except Exception:
            return 0

    def _count_relationships(self, model_id: str) -> int:
        """Count relationships in model."""
        if not self.conn:
            return 0
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM metadata.relationship WHERE model_id = ?
            """, [model_id]).fetchone()
            return result[0] if result else 0
        except Exception:
            return 0


def export_knowledge_graph(
    conn: Any,
    model_id: str,
    format: Union[str, ExportFormat] = "turtle",
    output: Optional[str] = None,
    base_uri: str = "https://example.org/mdde/",
) -> Union[str, ExportResult]:
    """
    Quick export of model to knowledge graph format.

    Args:
        conn: DuckDB connection
        model_id: Model ID to export
        format: Export format (turtle, ntriples, owl, jsonld, all)
        output: Optional output file or directory
        base_uri: Base URI for resources

    Returns:
        Content string (if no output) or ExportResult (if output specified)

    Example:
        # Get Turtle content
        turtle = export_knowledge_graph(conn, "MDL_sales")

        # Export to file
        export_knowledge_graph(conn, "MDL_sales", format="owl", output="sales.owl")

        # Export all formats
        export_knowledge_graph(conn, "MDL_sales", format="all", output="./exports/")
    """
    if isinstance(format, str):
        format_map = {
            "turtle": ExportFormat.TURTLE,
            "ttl": ExportFormat.TURTLE,
            "ntriples": ExportFormat.NTRIPLES,
            "nt": ExportFormat.NTRIPLES,
            "owl": ExportFormat.OWL,
            "jsonld": ExportFormat.JSONLD,
            "json-ld": ExportFormat.JSONLD,
            "all": ExportFormat.ALL,
        }
        export_format = format_map.get(format.lower(), ExportFormat.TURTLE)
    else:
        export_format = format

    exporter = KnowledgeGraphExporter(conn, base_uri=base_uri)

    if export_format == ExportFormat.ALL:
        if not output:
            output = "."
        results = exporter.export_all(model_id, output)
        return results[0]  # Return first result

    result = exporter.export(model_id, format=export_format, output_path=output)

    if output:
        return result
    else:
        return result.content


__all__ = [
    "KnowledgeGraphExporter",
    "ExportFormat",
    "ExportConfig",
    "ExportResult",
    "export_knowledge_graph",
]
