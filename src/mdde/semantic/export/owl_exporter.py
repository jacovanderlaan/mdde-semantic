"""
OWL Ontology Exporter.

Exports MDDE metadata as OWL ontology for semantic web integration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone


@dataclass
class OWLClass:
    """OWL class definition."""
    uri: str
    label: str
    comment: Optional[str] = None
    superclass: Optional[str] = None
    equivalent_class: Optional[str] = None
    disjoint_with: List[str] = field(default_factory=list)

    def to_owl_xml(self, indent: int = 2) -> str:
        """Serialize to OWL/XML."""
        ind = " " * indent
        lines = [f'{ind}<owl:Class rdf:about="{self.uri}">']

        if self.label:
            lines.append(f'{ind}  <rdfs:label>{_escape_xml(self.label)}</rdfs:label>')

        if self.comment:
            lines.append(f'{ind}  <rdfs:comment>{_escape_xml(self.comment)}</rdfs:comment>')

        if self.superclass:
            lines.append(f'{ind}  <rdfs:subClassOf rdf:resource="{self.superclass}"/>')

        if self.equivalent_class:
            lines.append(f'{ind}  <owl:equivalentClass rdf:resource="{self.equivalent_class}"/>')

        for dw in self.disjoint_with:
            lines.append(f'{ind}  <owl:disjointWith rdf:resource="{dw}"/>')

        lines.append(f'{ind}</owl:Class>')
        return "\n".join(lines)


@dataclass
class OWLProperty:
    """OWL property definition."""
    uri: str
    label: str
    property_type: str = "DatatypeProperty"  # or ObjectProperty
    domain: Optional[str] = None
    range: Optional[str] = None
    comment: Optional[str] = None
    is_functional: bool = False
    inverse_of: Optional[str] = None

    def to_owl_xml(self, indent: int = 2) -> str:
        """Serialize to OWL/XML."""
        ind = " " * indent
        tag = f"owl:{self.property_type}"
        lines = [f'{ind}<{tag} rdf:about="{self.uri}">']

        if self.label:
            lines.append(f'{ind}  <rdfs:label>{_escape_xml(self.label)}</rdfs:label>')

        if self.comment:
            lines.append(f'{ind}  <rdfs:comment>{_escape_xml(self.comment)}</rdfs:comment>')

        if self.domain:
            lines.append(f'{ind}  <rdfs:domain rdf:resource="{self.domain}"/>')

        if self.range:
            lines.append(f'{ind}  <rdfs:range rdf:resource="{self.range}"/>')

        if self.is_functional:
            lines.append(f'{ind}  <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#FunctionalProperty"/>')

        if self.inverse_of:
            lines.append(f'{ind}  <owl:inverseOf rdf:resource="{self.inverse_of}"/>')

        lines.append(f'{ind}</{tag}>')
        return "\n".join(lines)


@dataclass
class OWLOntology:
    """Complete OWL ontology."""
    uri: str
    label: str
    version: Optional[str] = None
    comment: Optional[str] = None
    imports: List[str] = field(default_factory=list)
    classes: List[OWLClass] = field(default_factory=list)
    properties: List[OWLProperty] = field(default_factory=list)
    annotations: Dict[str, str] = field(default_factory=dict)

    def to_owl_xml(self) -> str:
        """Serialize to OWL/XML format."""
        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<rdf:RDF xmlns="' + self.uri + '#"',
            '     xml:base="' + self.uri + '"',
            '     xmlns:owl="http://www.w3.org/2002/07/owl#"',
            '     xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"',
            '     xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"',
            '     xmlns:xsd="http://www.w3.org/2001/XMLSchema#"',
            '     xmlns:dcterms="http://purl.org/dc/terms/"',
            '     xmlns:skos="http://www.w3.org/2004/02/skos/core#">',
            '',
            '  <!-- Ontology Declaration -->',
            f'  <owl:Ontology rdf:about="{self.uri}">',
            f'    <rdfs:label>{_escape_xml(self.label)}</rdfs:label>',
        ]

        if self.version:
            lines.append(f'    <owl:versionInfo>{_escape_xml(self.version)}</owl:versionInfo>')

        if self.comment:
            lines.append(f'    <rdfs:comment>{_escape_xml(self.comment)}</rdfs:comment>')

        for imp in self.imports:
            lines.append(f'    <owl:imports rdf:resource="{imp}"/>')

        for key, value in self.annotations.items():
            lines.append(f'    <{key}>{_escape_xml(value)}</{key}>')

        lines.append('  </owl:Ontology>')
        lines.append('')

        # Classes
        if self.classes:
            lines.append('  <!-- Classes -->')
            for cls in self.classes:
                lines.append(cls.to_owl_xml(indent=2))
                lines.append('')

        # Properties
        if self.properties:
            lines.append('  <!-- Properties -->')
            for prop in self.properties:
                lines.append(prop.to_owl_xml(indent=2))
                lines.append('')

        lines.append('</rdf:RDF>')
        return "\n".join(lines)


def _escape_xml(text: str) -> str:
    """Escape XML special characters."""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;"))


class OWLExporter:
    """
    Exports MDDE metadata as OWL ontology.

    Creates a formal ontology from entity definitions,
    suitable for semantic web tools and reasoners.
    """

    def __init__(
        self,
        conn: Any = None,
        base_uri: str = "https://example.org/mdde/ontology",
    ):
        """
        Initialize exporter.

        Args:
            conn: DuckDB connection
            base_uri: Base URI for ontology
        """
        self.conn = conn
        self.base_uri = base_uri

    def export_model(self, model_id: str, model_name: Optional[str] = None) -> str:
        """
        Export model as OWL ontology.

        Args:
            model_id: Model ID to export
            model_name: Optional display name

        Returns:
            OWL/XML string
        """
        ontology = OWLOntology(
            uri=f"{self.base_uri}/{model_id}",
            label=model_name or model_id,
            version="1.0",
            comment=f"MDDE-generated ontology for {model_id}",
        )

        # Add MDDE base classes
        ontology.classes.extend(self._create_base_classes())

        # Export entities as classes
        if self.conn:
            self._export_entities(ontology, model_id)
            self._export_relationships(ontology, model_id)

        return ontology.to_owl_xml()

    def _create_base_classes(self) -> List[OWLClass]:
        """Create MDDE base vocabulary classes."""
        return [
            OWLClass(
                uri=f"{self.base_uri}#Entity",
                label="Entity",
                comment="Base class for all MDDE entities",
            ),
            OWLClass(
                uri=f"{self.base_uri}#SourceEntity",
                label="Source Entity",
                comment="Entity from source systems",
                superclass=f"{self.base_uri}#Entity",
            ),
            OWLClass(
                uri=f"{self.base_uri}#IntegrationEntity",
                label="Integration Entity",
                comment="Entity in the integration layer",
                superclass=f"{self.base_uri}#Entity",
            ),
            OWLClass(
                uri=f"{self.base_uri}#BusinessEntity",
                label="Business Entity",
                comment="Entity in the business layer",
                superclass=f"{self.base_uri}#Entity",
            ),
        ]

    def _export_entities(self, ontology: OWLOntology, model_id: str) -> None:
        """Export entities as OWL classes."""
        entities = self.conn.execute("""
            SELECT entity_id, entity_name, stereotype, layer, description
            FROM metadata.entity
            WHERE model_id = ?
        """, [model_id]).fetchall()

        layer_map = {
            "source": f"{self.base_uri}#SourceEntity",
            "staging": f"{self.base_uri}#SourceEntity",
            "integration": f"{self.base_uri}#IntegrationEntity",
            "business": f"{self.base_uri}#BusinessEntity",
            "delivery": f"{self.base_uri}#BusinessEntity",
        }

        for row in entities:
            entity_id, entity_name, stereotype, layer, description = row

            superclass = layer_map.get(
                (layer or "").lower(),
                f"{self.base_uri}#Entity"
            )

            ontology.classes.append(OWLClass(
                uri=f"{self.base_uri}/{model_id}#{entity_id}",
                label=entity_name,
                comment=description,
                superclass=superclass,
            ))

            # Export attributes as properties
            self._export_attributes(ontology, model_id, entity_id)

    def _export_attributes(
        self,
        ontology: OWLOntology,
        model_id: str,
        entity_id: str,
    ) -> None:
        """Export attributes as OWL datatype properties."""
        attributes = self.conn.execute("""
            SELECT attribute_id, attribute_name, data_type, description
            FROM metadata.attribute
            WHERE model_id = ? AND entity_id = ?
        """, [model_id, entity_id]).fetchall()

        for row in attributes:
            attr_id, attr_name, data_type, description = row

            xsd_range = self._map_to_xsd(data_type or "VARCHAR")

            ontology.properties.append(OWLProperty(
                uri=f"{self.base_uri}/{model_id}#{attr_id}",
                label=attr_name,
                property_type="DatatypeProperty",
                domain=f"{self.base_uri}/{model_id}#{entity_id}",
                range=xsd_range,
                comment=description,
            ))

    def _export_relationships(self, ontology: OWLOntology, model_id: str) -> None:
        """Export relationships as OWL object properties."""
        relationships = self.conn.execute("""
            SELECT relationship_id, relationship_name,
                   parent_entity_id, child_entity_id,
                   cardinality
            FROM metadata.relationship
            WHERE model_id = ?
        """, [model_id]).fetchall()

        for row in relationships:
            rel_id, rel_name, parent_id, child_id, cardinality = row

            is_functional = cardinality in ("1:1", "N:1")

            ontology.properties.append(OWLProperty(
                uri=f"{self.base_uri}/{model_id}#{rel_id}",
                label=rel_name or f"{parent_id}_to_{child_id}",
                property_type="ObjectProperty",
                domain=f"{self.base_uri}/{model_id}#{parent_id}",
                range=f"{self.base_uri}/{model_id}#{child_id}",
                is_functional=is_functional,
            ))

    def _map_to_xsd(self, sql_type: str) -> str:
        """Map SQL type to XSD type URI."""
        sql_upper = sql_type.upper()
        xsd_base = "http://www.w3.org/2001/XMLSchema#"

        if any(t in sql_upper for t in ["INT", "INTEGER", "BIGINT"]):
            return f"{xsd_base}integer"
        elif any(t in sql_upper for t in ["DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"]):
            return f"{xsd_base}decimal"
        elif "BOOL" in sql_upper:
            return f"{xsd_base}boolean"
        elif any(t in sql_upper for t in ["DATE", "TIMESTAMP", "DATETIME"]):
            return f"{xsd_base}dateTime"
        elif "TIME" in sql_upper:
            return f"{xsd_base}time"
        else:
            return f"{xsd_base}string"

    def export_to_file(self, model_id: str, output_path: str) -> str:
        """
        Export ontology to file.

        Args:
            model_id: Model to export
            output_path: Output file path

        Returns:
            Path to written file
        """
        owl_xml = self.export_model(model_id)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(owl_xml)

        return output_path


def export_ontology(
    conn: Any,
    model_id: str,
    output_path: Optional[str] = None,
    base_uri: str = "https://example.org/mdde/ontology",
) -> str:
    """
    Quick export model as OWL ontology.

    Args:
        conn: DuckDB connection
        model_id: Model ID
        output_path: Optional output file
        base_uri: Base URI

    Returns:
        OWL/XML string or file path
    """
    exporter = OWLExporter(conn, base_uri=base_uri)

    if output_path:
        return exporter.export_to_file(model_id, output_path)
    else:
        return exporter.export_model(model_id)


__all__ = [
    "OWLExporter",
    "OWLOntology",
    "OWLClass",
    "OWLProperty",
    "export_ontology",
]
