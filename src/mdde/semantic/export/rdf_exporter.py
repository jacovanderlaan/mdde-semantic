"""
RDF/Turtle Exporter for Knowledge Graph.

Exports MDDE metadata as RDF triples in Turtle format.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone


class RDFFormat(Enum):
    """RDF serialization formats."""
    TURTLE = "turtle"
    NTRIPLES = "ntriples"
    XML = "xml"


@dataclass
class Namespace:
    """RDF namespace definition."""
    prefix: str
    uri: str

    def term(self, local: str) -> str:
        """Create prefixed term."""
        return f"{self.prefix}:{local}"

    def full_uri(self, local: str) -> str:
        """Create full URI."""
        return f"<{self.uri}{local}>"


# Standard namespaces
RDF = Namespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFS = Namespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
OWL = Namespace("owl", "http://www.w3.org/2002/07/owl#")
XSD = Namespace("xsd", "http://www.w3.org/2001/XMLSchema#")
DCTERMS = Namespace("dcterms", "http://purl.org/dc/terms/")
SKOS = Namespace("skos", "http://www.w3.org/2004/02/skos/core#")


@dataclass
class Triple:
    """RDF triple."""
    subject: str
    predicate: str
    object: str
    is_literal: bool = False
    datatype: Optional[str] = None
    language: Optional[str] = None

    def to_turtle(self) -> str:
        """Serialize to Turtle format."""
        obj = self.object
        if self.is_literal:
            # Escape quotes in literals
            escaped = obj.replace("\\", "\\\\").replace('"', '\\"')
            if self.datatype:
                obj = f'"{escaped}"^^{self.datatype}'
            elif self.language:
                obj = f'"{escaped}"@{self.language}'
            else:
                obj = f'"{escaped}"'
        return f"{self.subject} {self.predicate} {obj} ."

    def to_ntriples(self) -> str:
        """Serialize to N-Triples format."""
        # N-Triples requires full URIs
        subj = self.subject if self.subject.startswith("<") else f"<{self.subject}>"
        pred = self.predicate if self.predicate.startswith("<") else f"<{self.predicate}>"

        if self.is_literal:
            escaped = self.object.replace("\\", "\\\\").replace('"', '\\"')
            if self.datatype:
                dt = self.datatype if self.datatype.startswith("<") else f"<{self.datatype}>"
                obj = f'"{escaped}"^^{dt}'
            else:
                obj = f'"{escaped}"'
        else:
            obj = self.object if self.object.startswith("<") else f"<{self.object}>"

        return f"{subj} {pred} {obj} ."


@dataclass
class RDFGraph:
    """Collection of RDF triples with namespace management."""
    namespaces: Dict[str, str] = field(default_factory=dict)
    triples: List[Triple] = field(default_factory=list)
    base_uri: str = "https://example.org/mdde/"

    def __post_init__(self):
        # Add standard namespaces
        self.namespaces.setdefault("rdf", RDF.uri)
        self.namespaces.setdefault("rdfs", RDFS.uri)
        self.namespaces.setdefault("owl", OWL.uri)
        self.namespaces.setdefault("xsd", XSD.uri)
        self.namespaces.setdefault("dcterms", DCTERMS.uri)
        self.namespaces.setdefault("skos", SKOS.uri)
        self.namespaces.setdefault("mdde", self.base_uri)

    def add_namespace(self, prefix: str, uri: str) -> None:
        """Add namespace prefix."""
        self.namespaces[prefix] = uri

    def add_triple(
        self,
        subject: str,
        predicate: str,
        obj: str,
        is_literal: bool = False,
        datatype: Optional[str] = None,
        language: Optional[str] = None,
    ) -> None:
        """Add a triple to the graph."""
        self.triples.append(Triple(
            subject=subject,
            predicate=predicate,
            object=obj,
            is_literal=is_literal,
            datatype=datatype,
            language=language,
        ))

    def add_type(self, subject: str, rdf_type: str) -> None:
        """Add rdf:type triple."""
        self.add_triple(subject, "rdf:type", rdf_type)

    def add_label(self, subject: str, label: str, language: str = "en") -> None:
        """Add rdfs:label."""
        self.add_triple(subject, "rdfs:label", label, is_literal=True, language=language)

    def add_comment(self, subject: str, comment: str, language: str = "en") -> None:
        """Add rdfs:comment."""
        self.add_triple(subject, "rdfs:comment", comment, is_literal=True, language=language)

    def to_turtle(self) -> str:
        """Serialize graph to Turtle format."""
        lines = []

        # Prefixes
        for prefix, uri in sorted(self.namespaces.items()):
            lines.append(f"@prefix {prefix}: <{uri}> .")
        lines.append("")

        # Base URI
        lines.append(f"@base <{self.base_uri}> .")
        lines.append("")

        # Group triples by subject
        subjects: Dict[str, List[Triple]] = {}
        for triple in self.triples:
            subjects.setdefault(triple.subject, []).append(triple)

        # Serialize
        for subject, triples in subjects.items():
            if len(triples) == 1:
                lines.append(triples[0].to_turtle())
            else:
                # Multi-predicate format
                lines.append(f"{subject}")
                for i, triple in enumerate(triples):
                    obj = triple.object
                    if triple.is_literal:
                        escaped = obj.replace("\\", "\\\\").replace('"', '\\"')
                        if triple.datatype:
                            obj = f'"{escaped}"^^{triple.datatype}'
                        elif triple.language:
                            obj = f'"{escaped}"@{triple.language}'
                        else:
                            obj = f'"{escaped}"'
                    sep = ";" if i < len(triples) - 1 else "."
                    lines.append(f"    {triple.predicate} {obj} {sep}")
            lines.append("")

        return "\n".join(lines)

    def to_ntriples(self) -> str:
        """Serialize graph to N-Triples format."""
        lines = [triple.to_ntriples() for triple in self.triples]
        return "\n".join(lines)


class RDFExporter:
    """
    Exports MDDE metadata as RDF.

    Converts entities, attributes, and relationships to RDF triples
    using a standard vocabulary mapping.
    """

    def __init__(
        self,
        conn: Any = None,
        base_uri: str = "https://example.org/mdde/",
        model_namespace: Optional[str] = None,
    ):
        """
        Initialize exporter.

        Args:
            conn: DuckDB connection
            base_uri: Base URI for generated resources
            model_namespace: Optional model-specific namespace
        """
        self.conn = conn
        self.base_uri = base_uri
        self.model_namespace = model_namespace or base_uri
        self.graph = RDFGraph(base_uri=base_uri)

    def export_model(self, model_id: str) -> str:
        """
        Export a model to RDF/Turtle.

        Args:
            model_id: Model ID to export

        Returns:
            Turtle-formatted RDF string
        """
        self.graph = RDFGraph(base_uri=self.base_uri)
        self.graph.add_namespace("model", f"{self.model_namespace}{model_id}/")

        # Export model metadata
        self._export_model_metadata(model_id)

        # Export entities
        self._export_entities(model_id)

        # Export relationships
        self._export_relationships(model_id)

        # Export domains
        self._export_domains(model_id)

        return self.graph.to_turtle()

    def _export_model_metadata(self, model_id: str) -> None:
        """Export model-level metadata."""
        model_uri = f"model:{model_id}"

        self.graph.add_type(model_uri, "mdde:DataModel")
        self.graph.add_label(model_uri, model_id)

        if self.conn:
            result = self.conn.execute("""
                SELECT model_name, description, created_at
                FROM metadata.model
                WHERE model_id = ?
            """, [model_id]).fetchone()

            if result:
                model_name, description, created_at = result
                if model_name:
                    self.graph.add_triple(
                        model_uri, "dcterms:title", model_name,
                        is_literal=True
                    )
                if description:
                    self.graph.add_comment(model_uri, description)
                if created_at:
                    self.graph.add_triple(
                        model_uri, "dcterms:created", str(created_at),
                        is_literal=True, datatype="xsd:dateTime"
                    )

    def _export_entities(self, model_id: str) -> None:
        """Export entities as RDF classes."""
        if not self.conn:
            return

        entities = self.conn.execute("""
            SELECT entity_id, entity_name, stereotype, classification,
                   layer, description
            FROM metadata.entity
            WHERE model_id = ?
        """, [model_id]).fetchall()

        for row in entities:
            entity_id, entity_name, stereotype, classification, layer, description = row
            entity_uri = f"model:{entity_id}"

            # Entity is a class
            self.graph.add_type(entity_uri, "owl:Class")
            self.graph.add_type(entity_uri, "mdde:Entity")
            self.graph.add_label(entity_uri, entity_name)

            if description:
                self.graph.add_comment(entity_uri, description)

            if stereotype:
                self.graph.add_triple(
                    entity_uri, "mdde:stereotype", stereotype,
                    is_literal=True
                )

            if classification:
                self.graph.add_triple(
                    entity_uri, "mdde:classification", classification,
                    is_literal=True
                )

            if layer:
                self.graph.add_triple(
                    entity_uri, "mdde:layer", layer,
                    is_literal=True
                )

            # Export attributes
            self._export_attributes(model_id, entity_id)

    def _export_attributes(self, model_id: str, entity_id: str) -> None:
        """Export attributes as RDF properties."""
        if not self.conn:
            return

        attributes = self.conn.execute("""
            SELECT attribute_id, attribute_name, data_type,
                   is_mandatory, is_primary_key, is_business_key,
                   description
            FROM metadata.attribute
            WHERE model_id = ? AND entity_id = ?
        """, [model_id, entity_id]).fetchall()

        for row in attributes:
            (attr_id, attr_name, data_type, is_mandatory,
             is_pk, is_bk, description) = row

            attr_uri = f"model:{attr_id}"
            entity_uri = f"model:{entity_id}"

            # Attribute is a property
            self.graph.add_type(attr_uri, "owl:DatatypeProperty")
            self.graph.add_type(attr_uri, "mdde:Attribute")
            self.graph.add_label(attr_uri, attr_name)

            # Domain is the entity
            self.graph.add_triple(attr_uri, "rdfs:domain", entity_uri)

            if description:
                self.graph.add_comment(attr_uri, description)

            if data_type:
                xsd_type = self._map_datatype(data_type)
                self.graph.add_triple(attr_uri, "rdfs:range", xsd_type)

            if is_pk:
                self.graph.add_triple(
                    attr_uri, "mdde:isPrimaryKey", "true",
                    is_literal=True, datatype="xsd:boolean"
                )

            if is_bk:
                self.graph.add_triple(
                    attr_uri, "mdde:isBusinessKey", "true",
                    is_literal=True, datatype="xsd:boolean"
                )

            if is_mandatory:
                self.graph.add_triple(
                    attr_uri, "mdde:isMandatory", "true",
                    is_literal=True, datatype="xsd:boolean"
                )

    def _export_relationships(self, model_id: str) -> None:
        """Export relationships as RDF object properties."""
        if not self.conn:
            return

        relationships = self.conn.execute("""
            SELECT relationship_id, relationship_name,
                   parent_entity_id, child_entity_id,
                   cardinality, relationship_type
            FROM metadata.relationship
            WHERE model_id = ?
        """, [model_id]).fetchall()

        for row in relationships:
            (rel_id, rel_name, parent_id, child_id,
             cardinality, rel_type) = row

            rel_uri = f"model:{rel_id}"
            parent_uri = f"model:{parent_id}"
            child_uri = f"model:{child_id}"

            # Relationship is an object property
            self.graph.add_type(rel_uri, "owl:ObjectProperty")
            self.graph.add_type(rel_uri, "mdde:Relationship")

            if rel_name:
                self.graph.add_label(rel_uri, rel_name)

            self.graph.add_triple(rel_uri, "rdfs:domain", parent_uri)
            self.graph.add_triple(rel_uri, "rdfs:range", child_uri)

            if cardinality:
                self.graph.add_triple(
                    rel_uri, "mdde:cardinality", cardinality,
                    is_literal=True
                )

            if rel_type:
                self.graph.add_triple(
                    rel_uri, "mdde:relationshipType", rel_type,
                    is_literal=True
                )

    def _export_domains(self, model_id: str) -> None:
        """Export domains as SKOS concept schemes."""
        if not self.conn:
            return

        try:
            domains = self.conn.execute("""
                SELECT domain_id, domain_name, description
                FROM metadata.domain
                WHERE model_id = ?
            """, [model_id]).fetchall()

            for row in domains:
                domain_id, domain_name, description = row
                domain_uri = f"model:{domain_id}"

                self.graph.add_type(domain_uri, "skos:ConceptScheme")
                self.graph.add_type(domain_uri, "mdde:Domain")
                self.graph.add_label(domain_uri, domain_name)

                if description:
                    self.graph.add_comment(domain_uri, description)

        except Exception:
            pass  # Domain table may not exist

    def _map_datatype(self, sql_type: str) -> str:
        """Map SQL data type to XSD type."""
        sql_type_upper = sql_type.upper()

        # Order matters: check longer/more specific types first to avoid
        # substring matches (e.g., BIGINT must be checked before INT)
        type_map = [
            ("VARCHAR", "xsd:string"),
            ("STRING", "xsd:string"),
            ("TEXT", "xsd:string"),
            ("CHAR", "xsd:string"),
            ("BIGINT", "xsd:long"),
            ("SMALLINT", "xsd:short"),
            ("TINYINT", "xsd:byte"),
            ("INTEGER", "xsd:integer"),
            ("INT", "xsd:integer"),
            ("DECIMAL", "xsd:decimal"),
            ("NUMERIC", "xsd:decimal"),
            ("DOUBLE", "xsd:double"),
            ("FLOAT", "xsd:float"),
            ("REAL", "xsd:float"),
            ("BOOLEAN", "xsd:boolean"),
            ("BOOL", "xsd:boolean"),
            ("DATETIME", "xsd:dateTime"),
            ("TIMESTAMP", "xsd:dateTime"),
            ("DATE", "xsd:date"),
            ("TIME", "xsd:time"),
            ("BINARY", "xsd:hexBinary"),
            ("BLOB", "xsd:hexBinary"),
        ]

        for key, xsd in type_map:
            if key in sql_type_upper:
                return xsd

        return "xsd:string"

    def export_to_file(
        self,
        model_id: str,
        output_path: str,
        format: RDFFormat = RDFFormat.TURTLE,
    ) -> str:
        """
        Export model to file.

        Args:
            model_id: Model to export
            output_path: Output file path
            format: RDF format

        Returns:
            Path to written file
        """
        if format == RDFFormat.TURTLE:
            content = self.export_model(model_id)
        elif format == RDFFormat.NTRIPLES:
            self.export_model(model_id)  # Populate graph
            content = self.graph.to_ntriples()
        else:
            content = self.export_model(model_id)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

        return output_path


def export_to_rdf(
    conn: Any,
    model_id: str,
    output_path: Optional[str] = None,
    format: RDFFormat = RDFFormat.TURTLE,
    base_uri: str = "https://example.org/mdde/",
) -> str:
    """
    Quick export model to RDF.

    Args:
        conn: DuckDB connection
        model_id: Model ID
        output_path: Optional output file
        format: RDF format
        base_uri: Base URI

    Returns:
        RDF string or file path
    """
    exporter = RDFExporter(conn, base_uri=base_uri)

    if output_path:
        return exporter.export_to_file(model_id, output_path, format)
    else:
        return exporter.export_model(model_id)


__all__ = [
    "RDFExporter",
    "RDFFormat",
    "RDFGraph",
    "Triple",
    "Namespace",
    "export_to_rdf",
]
