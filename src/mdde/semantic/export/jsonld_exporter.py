"""
JSON-LD Exporter for Knowledge Graph.

Exports MDDE metadata as JSON-LD for web APIs and linked data applications.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone
import json


@dataclass
class JSONLDContext:
    """
    JSON-LD context definition.

    Defines the mapping between JSON keys and RDF predicates.
    """
    base_uri: str = "https://example.org/mdde/"
    vocab: str = "https://example.org/mdde/vocab#"
    prefixes: Dict[str, str] = field(default_factory=dict)
    terms: Dict[str, Union[str, Dict[str, str]]] = field(default_factory=dict)

    def __post_init__(self):
        # Default prefixes
        self.prefixes.setdefault("schema", "https://schema.org/")
        self.prefixes.setdefault("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        self.prefixes.setdefault("xsd", "http://www.w3.org/2001/XMLSchema#")
        self.prefixes.setdefault("dcterms", "http://purl.org/dc/terms/")
        self.prefixes.setdefault("mdde", self.vocab)

        # Default term mappings
        self.terms.setdefault("name", "rdfs:label")
        self.terms.setdefault("description", "rdfs:comment")
        self.terms.setdefault("created", {"@id": "dcterms:created", "@type": "xsd:dateTime"})
        self.terms.setdefault("modified", {"@id": "dcterms:modified", "@type": "xsd:dateTime"})

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-LD @context object."""
        context: Dict[str, Any] = {}

        # Base and vocab
        if self.base_uri:
            context["@base"] = self.base_uri
        if self.vocab:
            context["@vocab"] = self.vocab

        # Prefixes
        context.update(self.prefixes)

        # Terms
        context.update(self.terms)

        return context


@dataclass
class JSONLDNode:
    """
    JSON-LD node (resource).
    """
    id: str
    type: Union[str, List[str]]
    properties: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-LD object."""
        result: Dict[str, Any] = {
            "@id": self.id,
            "@type": self.type if isinstance(self.type, list) else [self.type],
        }
        result.update(self.properties)
        return result


@dataclass
class JSONLDDocument:
    """
    Complete JSON-LD document.
    """
    context: JSONLDContext
    graph: List[JSONLDNode] = field(default_factory=list)

    def add_node(self, node: JSONLDNode) -> None:
        """Add a node to the graph."""
        self.graph.append(node)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-LD document."""
        return {
            "@context": self.context.to_dict(),
            "@graph": [node.to_dict() for node in self.graph],
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


class JSONLDExporter:
    """
    Exports MDDE metadata as JSON-LD.

    Creates linked data documents suitable for web APIs,
    search engines, and knowledge graph integration.
    """

    def __init__(
        self,
        conn: Any = None,
        base_uri: str = "https://example.org/mdde/",
        context: Optional[JSONLDContext] = None,
    ):
        """
        Initialize exporter.

        Args:
            conn: DuckDB connection
            base_uri: Base URI for resources
            context: Optional custom context
        """
        self.conn = conn
        self.base_uri = base_uri
        self.context = context or JSONLDContext(base_uri=base_uri)

    def export_model(self, model_id: str) -> str:
        """
        Export model as JSON-LD.

        Args:
            model_id: Model ID to export

        Returns:
            JSON-LD string
        """
        doc = JSONLDDocument(context=self.context)

        # Model node
        model_node = self._create_model_node(model_id)
        if model_node:
            doc.add_node(model_node)

        # Entity nodes
        if self.conn:
            entities = self._export_entities(model_id)
            for entity_node in entities:
                doc.add_node(entity_node)

            # Relationship nodes
            relationships = self._export_relationships(model_id)
            for rel_node in relationships:
                doc.add_node(rel_node)

        return doc.to_json()

    def _create_model_node(self, model_id: str) -> Optional[JSONLDNode]:
        """Create JSON-LD node for model."""
        node = JSONLDNode(
            id=f"{self.base_uri}model/{model_id}",
            type="mdde:DataModel",
            properties={
                "identifier": model_id,
            },
        )

        if self.conn:
            try:
                result = self.conn.execute("""
                    SELECT model_name, description, created_at
                    FROM metadata.model
                    WHERE model_id = ?
                """, [model_id]).fetchone()

                if result:
                    model_name, description, created_at = result
                    if model_name:
                        node.properties["name"] = model_name
                    if description:
                        node.properties["description"] = description
                    if created_at:
                        node.properties["created"] = str(created_at)
            except Exception:
                pass

        return node

    def _export_entities(self, model_id: str) -> List[JSONLDNode]:
        """Export entities as JSON-LD nodes."""
        nodes = []

        entities = self.conn.execute("""
            SELECT entity_id, entity_name, stereotype, classification,
                   layer, description
            FROM metadata.entity
            WHERE model_id = ?
        """, [model_id]).fetchall()

        for row in entities:
            entity_id, entity_name, stereotype, classification, layer, description = row

            types = ["mdde:Entity"]
            if stereotype:
                types.append(f"mdde:{stereotype}")

            node = JSONLDNode(
                id=f"{self.base_uri}entity/{entity_id}",
                type=types,
                properties={
                    "identifier": entity_id,
                    "name": entity_name,
                    "mdde:belongsToModel": {"@id": f"{self.base_uri}model/{model_id}"},
                },
            )

            if description:
                node.properties["description"] = description
            if stereotype:
                node.properties["mdde:stereotype"] = stereotype
            if classification:
                node.properties["mdde:classification"] = classification
            if layer:
                node.properties["mdde:layer"] = layer

            # Add attributes
            attributes = self._export_attributes(model_id, entity_id)
            if attributes:
                node.properties["mdde:hasAttribute"] = attributes

            nodes.append(node)

        return nodes

    def _export_attributes(
        self,
        model_id: str,
        entity_id: str,
    ) -> List[Dict[str, Any]]:
        """Export attributes as nested objects."""
        attributes = []

        rows = self.conn.execute("""
            SELECT attribute_id, attribute_name, data_type,
                   is_mandatory, is_primary_key, is_business_key,
                   description
            FROM metadata.attribute
            WHERE model_id = ? AND entity_id = ?
            ORDER BY ordinal_position
        """, [model_id, entity_id]).fetchall()

        for row in rows:
            (attr_id, attr_name, data_type, is_mandatory,
             is_pk, is_bk, description) = row

            attr = {
                "@id": f"{self.base_uri}attribute/{attr_id}",
                "@type": "mdde:Attribute",
                "identifier": attr_id,
                "name": attr_name,
            }

            if data_type:
                attr["mdde:dataType"] = data_type
            if description:
                attr["description"] = description
            if is_pk:
                attr["mdde:isPrimaryKey"] = True
            if is_bk:
                attr["mdde:isBusinessKey"] = True
            if is_mandatory:
                attr["mdde:isMandatory"] = True

            attributes.append(attr)

        return attributes

    def _export_relationships(self, model_id: str) -> List[JSONLDNode]:
        """Export relationships as JSON-LD nodes."""
        nodes = []

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

            node = JSONLDNode(
                id=f"{self.base_uri}relationship/{rel_id}",
                type="mdde:Relationship",
                properties={
                    "identifier": rel_id,
                    "mdde:sourceEntity": {"@id": f"{self.base_uri}entity/{parent_id}"},
                    "mdde:targetEntity": {"@id": f"{self.base_uri}entity/{child_id}"},
                },
            )

            if rel_name:
                node.properties["name"] = rel_name
            if cardinality:
                node.properties["mdde:cardinality"] = cardinality
            if rel_type:
                node.properties["mdde:relationshipType"] = rel_type

            nodes.append(node)

        return nodes

    def export_to_file(self, model_id: str, output_path: str) -> str:
        """
        Export to file.

        Args:
            model_id: Model to export
            output_path: Output file path

        Returns:
            Path to written file
        """
        jsonld = self.export_model(model_id)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(jsonld)

        return output_path

    def export_entity(self, model_id: str, entity_id: str) -> str:
        """
        Export single entity as JSON-LD.

        Args:
            model_id: Model ID
            entity_id: Entity ID

        Returns:
            JSON-LD string
        """
        doc = JSONLDDocument(context=self.context)

        if self.conn:
            entities = self._export_entities(model_id)
            for entity in entities:
                if entity.id.endswith(f"/{entity_id}"):
                    doc.add_node(entity)
                    break

        return doc.to_json()

    def export_schema_org(self, model_id: str) -> str:
        """
        Export as Schema.org-compatible JSON-LD.

        Useful for search engine structured data.

        Args:
            model_id: Model ID

        Returns:
            Schema.org JSON-LD string
        """
        # Create Schema.org context
        schema_context = JSONLDContext(
            base_uri=self.base_uri,
            vocab="https://schema.org/",
        )
        schema_context.prefixes["schema"] = "https://schema.org/"
        schema_context.terms["name"] = "schema:name"
        schema_context.terms["description"] = "schema:description"

        doc = JSONLDDocument(context=schema_context)

        # Model as Dataset
        if self.conn:
            result = self.conn.execute("""
                SELECT model_name, description
                FROM metadata.model
                WHERE model_id = ?
            """, [model_id]).fetchone()

            if result:
                model_name, description = result
                doc.add_node(JSONLDNode(
                    id=f"{self.base_uri}model/{model_id}",
                    type="schema:Dataset",
                    properties={
                        "name": model_name or model_id,
                        "description": description or f"Data model {model_id}",
                        "schema:url": f"{self.base_uri}model/{model_id}",
                    },
                ))

        return doc.to_json()


def export_to_jsonld(
    conn: Any,
    model_id: str,
    output_path: Optional[str] = None,
    base_uri: str = "https://example.org/mdde/",
) -> str:
    """
    Quick export model to JSON-LD.

    Args:
        conn: DuckDB connection
        model_id: Model ID
        output_path: Optional output file
        base_uri: Base URI

    Returns:
        JSON-LD string or file path
    """
    exporter = JSONLDExporter(conn, base_uri=base_uri)

    if output_path:
        return exporter.export_to_file(model_id, output_path)
    else:
        return exporter.export_model(model_id)


__all__ = [
    "JSONLDExporter",
    "JSONLDDocument",
    "JSONLDNode",
    "JSONLDContext",
    "export_to_jsonld",
]
