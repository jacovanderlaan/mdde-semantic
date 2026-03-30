"""
Unit tests for Knowledge Graph Export (ADR-200).

Tests the RDF, OWL, and JSON-LD export functionality.
"""

import pytest
from mdde.semantic.export import (
    RDFExporter,
    RDFFormat,
    OWLExporter,
    JSONLDExporter,
    JSONLDContext,
    KnowledgeGraphExporter,
    ExportFormat,
    ExportConfig,
    export_knowledge_graph,
    export_to_rdf,
    export_ontology,
    export_to_jsonld,
)
from mdde.semantic.export.rdf_exporter import (
    RDFGraph,
    Triple,
    Namespace,
)
from mdde.semantic.export.owl_exporter import (
    OWLOntology,
    OWLClass,
    OWLProperty,
)
from mdde.semantic.export.jsonld_exporter import (
    JSONLDDocument,
    JSONLDNode,
)


# =============================================================================
# Test RDF Components
# =============================================================================


class TestNamespace:
    """Tests for Namespace class."""

    def test_create_namespace(self):
        """Test namespace creation."""
        ns = Namespace("ex", "http://example.org/")
        assert ns.prefix == "ex"
        assert ns.uri == "http://example.org/"

    def test_term(self):
        """Test prefixed term generation."""
        ns = Namespace("ex", "http://example.org/")
        assert ns.term("Person") == "ex:Person"

    def test_full_uri(self):
        """Test full URI generation."""
        ns = Namespace("ex", "http://example.org/")
        assert ns.full_uri("Person") == "<http://example.org/Person>"


class TestTriple:
    """Tests for Triple class."""

    def test_basic_triple(self):
        """Test basic triple creation."""
        triple = Triple("ex:subject", "ex:predicate", "ex:object")
        assert triple.subject == "ex:subject"
        assert triple.predicate == "ex:predicate"
        assert triple.object == "ex:object"

    def test_literal_triple(self):
        """Test literal object triple."""
        triple = Triple(
            "ex:subject",
            "rdfs:label",
            "Hello World",
            is_literal=True,
        )
        assert triple.is_literal is True

    def test_typed_literal(self):
        """Test typed literal."""
        triple = Triple(
            "ex:subject",
            "ex:count",
            "42",
            is_literal=True,
            datatype="xsd:integer",
        )
        turtle = triple.to_turtle()
        assert '"42"^^xsd:integer' in turtle

    def test_language_literal(self):
        """Test language-tagged literal."""
        triple = Triple(
            "ex:subject",
            "rdfs:label",
            "Bonjour",
            is_literal=True,
            language="fr",
        )
        turtle = triple.to_turtle()
        assert '"Bonjour"@fr' in turtle

    def test_to_turtle(self):
        """Test Turtle serialization."""
        triple = Triple("ex:s", "ex:p", "ex:o")
        turtle = triple.to_turtle()
        assert turtle == "ex:s ex:p ex:o ."


class TestRDFGraph:
    """Tests for RDFGraph class."""

    def test_create_graph(self):
        """Test graph creation."""
        graph = RDFGraph()
        assert "rdf" in graph.namespaces
        assert "rdfs" in graph.namespaces

    def test_add_namespace(self):
        """Test adding namespace."""
        graph = RDFGraph()
        graph.add_namespace("foaf", "http://xmlns.com/foaf/0.1/")
        assert graph.namespaces["foaf"] == "http://xmlns.com/foaf/0.1/"

    def test_add_triple(self):
        """Test adding triple."""
        graph = RDFGraph()
        graph.add_triple("ex:person", "rdf:type", "foaf:Person")
        assert len(graph.triples) == 1

    def test_add_type(self):
        """Test adding type triple."""
        graph = RDFGraph()
        graph.add_type("ex:person", "foaf:Person")
        assert len(graph.triples) == 1
        assert graph.triples[0].predicate == "rdf:type"

    def test_add_label(self):
        """Test adding label."""
        graph = RDFGraph()
        graph.add_label("ex:person", "John Doe")
        triple = graph.triples[0]
        assert triple.predicate == "rdfs:label"
        assert triple.is_literal is True

    def test_to_turtle(self):
        """Test Turtle output."""
        graph = RDFGraph(base_uri="http://example.org/")
        graph.add_type("ex:person", "foaf:Person")
        graph.add_label("ex:person", "John")

        turtle = graph.to_turtle()
        assert "@prefix" in turtle
        assert "@base" in turtle

    def test_to_ntriples(self):
        """Test N-Triples output."""
        graph = RDFGraph()
        graph.add_triple("<http://ex.org/s>", "<http://ex.org/p>", "<http://ex.org/o>")
        ntriples = graph.to_ntriples()
        assert "<http://ex.org/s>" in ntriples


# =============================================================================
# Test RDFExporter
# =============================================================================


class TestRDFExporter:
    """Tests for RDFExporter class."""

    def test_init(self):
        """Test exporter initialization."""
        exporter = RDFExporter(base_uri="http://test.org/")
        assert exporter.base_uri == "http://test.org/"

    def test_export_model_no_conn(self):
        """Test export without connection."""
        exporter = RDFExporter()
        turtle = exporter.export_model("test_model")

        assert "@prefix" in turtle
        assert "mdde:" in turtle

    def test_datatype_mapping(self):
        """Test SQL to XSD type mapping."""
        exporter = RDFExporter()

        assert exporter._map_datatype("VARCHAR(100)") == "xsd:string"
        assert exporter._map_datatype("INTEGER") == "xsd:integer"
        assert exporter._map_datatype("BIGINT") == "xsd:long"
        assert exporter._map_datatype("DECIMAL(10,2)") == "xsd:decimal"
        assert exporter._map_datatype("BOOLEAN") == "xsd:boolean"
        assert exporter._map_datatype("DATE") == "xsd:date"
        assert exporter._map_datatype("TIMESTAMP") == "xsd:dateTime"


# =============================================================================
# Test OWL Components
# =============================================================================


class TestOWLClass:
    """Tests for OWLClass class."""

    def test_basic_class(self):
        """Test basic class creation."""
        cls = OWLClass(
            uri="http://ex.org/Person",
            label="Person",
        )
        assert cls.uri == "http://ex.org/Person"
        assert cls.label == "Person"

    def test_class_with_superclass(self):
        """Test class with inheritance."""
        cls = OWLClass(
            uri="http://ex.org/Customer",
            label="Customer",
            superclass="http://ex.org/Person",
        )
        xml = cls.to_owl_xml()
        assert "subClassOf" in xml

    def test_to_owl_xml(self):
        """Test OWL/XML serialization."""
        cls = OWLClass(
            uri="http://ex.org/Person",
            label="Person",
            comment="A human being",
        )
        xml = cls.to_owl_xml()

        assert "owl:Class" in xml
        assert "rdfs:label" in xml
        assert "rdfs:comment" in xml


class TestOWLProperty:
    """Tests for OWLProperty class."""

    def test_datatype_property(self):
        """Test datatype property."""
        prop = OWLProperty(
            uri="http://ex.org/name",
            label="name",
            property_type="DatatypeProperty",
            range="http://www.w3.org/2001/XMLSchema#string",
        )
        xml = prop.to_owl_xml()

        assert "DatatypeProperty" in xml
        assert "rdfs:range" in xml

    def test_object_property(self):
        """Test object property."""
        prop = OWLProperty(
            uri="http://ex.org/knows",
            label="knows",
            property_type="ObjectProperty",
            domain="http://ex.org/Person",
            range="http://ex.org/Person",
        )
        xml = prop.to_owl_xml()

        assert "ObjectProperty" in xml
        assert "rdfs:domain" in xml
        assert "rdfs:range" in xml

    def test_functional_property(self):
        """Test functional property."""
        prop = OWLProperty(
            uri="http://ex.org/id",
            label="identifier",
            is_functional=True,
        )
        xml = prop.to_owl_xml()

        assert "FunctionalProperty" in xml


class TestOWLOntology:
    """Tests for OWLOntology class."""

    def test_basic_ontology(self):
        """Test basic ontology creation."""
        onto = OWLOntology(
            uri="http://example.org/ontology",
            label="Test Ontology",
        )
        assert onto.uri == "http://example.org/ontology"

    def test_ontology_with_classes(self):
        """Test ontology with classes."""
        onto = OWLOntology(
            uri="http://ex.org/onto",
            label="Test",
            classes=[
                OWLClass("http://ex.org/Person", "Person"),
                OWLClass("http://ex.org/Organization", "Organization"),
            ],
        )
        xml = onto.to_owl_xml()

        assert "Person" in xml
        assert "Organization" in xml

    def test_to_owl_xml(self):
        """Test complete OWL/XML output."""
        onto = OWLOntology(
            uri="http://ex.org/onto",
            label="Test Ontology",
            version="1.0",
            classes=[OWLClass("http://ex.org/Person", "Person")],
            properties=[OWLProperty("http://ex.org/name", "name")],
        )
        xml = onto.to_owl_xml()

        assert '<?xml version="1.0"' in xml
        assert "owl:Ontology" in xml
        assert "owl:Class" in xml
        assert "owl:DatatypeProperty" in xml


class TestOWLExporter:
    """Tests for OWLExporter class."""

    def test_init(self):
        """Test exporter initialization."""
        exporter = OWLExporter(base_uri="http://test.org/")
        assert exporter.base_uri == "http://test.org/"

    def test_export_model_no_conn(self):
        """Test export without connection."""
        exporter = OWLExporter()
        owl = exporter.export_model("test_model")

        assert "owl:Ontology" in owl
        assert "test_model" in owl


# =============================================================================
# Test JSON-LD Components
# =============================================================================


class TestJSONLDContext:
    """Tests for JSONLDContext class."""

    def test_basic_context(self):
        """Test basic context creation."""
        ctx = JSONLDContext()
        assert ctx.base_uri is not None

    def test_context_with_prefixes(self):
        """Test context with custom prefixes."""
        ctx = JSONLDContext()
        ctx.prefixes["ex"] = "http://example.org/"

        d = ctx.to_dict()
        assert "ex" in d

    def test_to_dict(self):
        """Test context serialization."""
        ctx = JSONLDContext(
            base_uri="http://test.org/",
            vocab="http://test.org/vocab#",
        )
        d = ctx.to_dict()

        assert "@base" in d
        assert "@vocab" in d


class TestJSONLDNode:
    """Tests for JSONLDNode class."""

    def test_basic_node(self):
        """Test basic node creation."""
        node = JSONLDNode(
            id="http://ex.org/person/1",
            type="Person",
        )
        assert node.id == "http://ex.org/person/1"

    def test_node_with_properties(self):
        """Test node with properties."""
        node = JSONLDNode(
            id="http://ex.org/person/1",
            type="Person",
            properties={"name": "John", "age": 30},
        )
        d = node.to_dict()

        assert d["name"] == "John"
        assert d["age"] == 30

    def test_to_dict(self):
        """Test node serialization."""
        node = JSONLDNode(
            id="http://ex.org/1",
            type=["Person", "Agent"],
        )
        d = node.to_dict()

        assert "@id" in d
        assert "@type" in d
        assert len(d["@type"]) == 2


class TestJSONLDDocument:
    """Tests for JSONLDDocument class."""

    def test_basic_document(self):
        """Test basic document creation."""
        doc = JSONLDDocument(context=JSONLDContext())
        assert doc.context is not None
        assert len(doc.graph) == 0

    def test_add_node(self):
        """Test adding nodes."""
        doc = JSONLDDocument(context=JSONLDContext())
        doc.add_node(JSONLDNode("http://ex.org/1", "Person"))

        assert len(doc.graph) == 1

    def test_to_json(self):
        """Test JSON serialization."""
        doc = JSONLDDocument(context=JSONLDContext())
        doc.add_node(JSONLDNode("http://ex.org/1", "Person", {"name": "John"}))

        json_str = doc.to_json()
        assert "@context" in json_str
        assert "@graph" in json_str
        assert "John" in json_str


class TestJSONLDExporter:
    """Tests for JSONLDExporter class."""

    def test_init(self):
        """Test exporter initialization."""
        exporter = JSONLDExporter(base_uri="http://test.org/")
        assert exporter.base_uri == "http://test.org/"

    def test_export_model_no_conn(self):
        """Test export without connection."""
        exporter = JSONLDExporter()
        jsonld = exporter.export_model("test_model")

        assert "@context" in jsonld
        assert "@graph" in jsonld


# =============================================================================
# Test KnowledgeGraphExporter
# =============================================================================


class TestExportConfig:
    """Tests for ExportConfig class."""

    def test_default_config(self):
        """Test default configuration."""
        config = ExportConfig()
        assert config.format == ExportFormat.TURTLE
        assert config.include_ontology is True

    def test_custom_config(self):
        """Test custom configuration."""
        config = ExportConfig(
            format=ExportFormat.JSONLD,
            base_uri="http://mycompany.com/",
            include_lineage=True,
        )
        assert config.format == ExportFormat.JSONLD
        assert config.include_lineage is True

    def test_file_extension(self):
        """Test file extension mapping."""
        assert ExportConfig(format=ExportFormat.TURTLE).get_file_extension() == ".ttl"
        assert ExportConfig(format=ExportFormat.NTRIPLES).get_file_extension() == ".nt"
        assert ExportConfig(format=ExportFormat.OWL).get_file_extension() == ".owl"
        assert ExportConfig(format=ExportFormat.JSONLD).get_file_extension() == ".jsonld"


class TestKnowledgeGraphExporter:
    """Tests for KnowledgeGraphExporter class."""

    def test_init(self):
        """Test exporter initialization."""
        exporter = KnowledgeGraphExporter(base_uri="http://test.org/")
        assert exporter.base_uri == "http://test.org/"

    def test_export_turtle(self):
        """Test Turtle export."""
        exporter = KnowledgeGraphExporter()
        result = exporter.export("test_model", format=ExportFormat.TURTLE)

        assert result.format == ExportFormat.TURTLE
        assert "@prefix" in result.content

    def test_export_owl(self):
        """Test OWL export."""
        exporter = KnowledgeGraphExporter()
        result = exporter.export("test_model", format=ExportFormat.OWL)

        assert result.format == ExportFormat.OWL
        assert "owl:Ontology" in result.content

    def test_export_jsonld(self):
        """Test JSON-LD export."""
        exporter = KnowledgeGraphExporter()
        result = exporter.export("test_model", format=ExportFormat.JSONLD)

        assert result.format == ExportFormat.JSONLD
        assert "@context" in result.content


# =============================================================================
# Test Convenience Functions
# =============================================================================


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_export_to_rdf(self):
        """Test export_to_rdf function."""
        turtle = export_to_rdf(None, "test_model")
        assert "@prefix" in turtle

    def test_export_ontology(self):
        """Test export_ontology function."""
        owl = export_ontology(None, "test_model")
        assert "owl:Ontology" in owl

    def test_export_to_jsonld(self):
        """Test export_to_jsonld function."""
        jsonld = export_to_jsonld(None, "test_model")
        assert "@context" in jsonld

    def test_export_knowledge_graph_turtle(self):
        """Test unified export to Turtle."""
        content = export_knowledge_graph(None, "test", format="turtle")
        assert "@prefix" in content

    def test_export_knowledge_graph_owl(self):
        """Test unified export to OWL."""
        content = export_knowledge_graph(None, "test", format="owl")
        assert "owl:Ontology" in content

    def test_export_knowledge_graph_jsonld(self):
        """Test unified export to JSON-LD."""
        content = export_knowledge_graph(None, "test", format="jsonld")
        assert "@context" in content


# =============================================================================
# Integration Tests
# =============================================================================


class TestKnowledgeGraphIntegration:
    """Integration tests for complete export workflow."""

    def test_full_rdf_export(self):
        """Test complete RDF export workflow."""
        exporter = RDFExporter(base_uri="http://company.com/data/")

        # Export model
        turtle = exporter.export_model("sales_model")

        # Verify output structure
        assert "@prefix rdf:" in turtle
        assert "@prefix rdfs:" in turtle
        assert "@prefix mdde:" in turtle
        assert "@base <http://company.com/data/>" in turtle

    def test_full_owl_export(self):
        """Test complete OWL export workflow."""
        exporter = OWLExporter(base_uri="http://company.com/ontology")

        owl_xml = exporter.export_model("sales_model", "Sales Data Model")

        # Verify OWL structure
        assert '<?xml version="1.0"' in owl_xml
        assert '<owl:Ontology' in owl_xml
        assert 'Sales Data Model' in owl_xml

    def test_full_jsonld_export(self):
        """Test complete JSON-LD export workflow."""
        exporter = JSONLDExporter(base_uri="http://company.com/data/")

        jsonld = exporter.export_model("sales_model")

        # Verify JSON-LD structure
        import json
        doc = json.loads(jsonld)

        assert "@context" in doc
        assert "@graph" in doc

    def test_all_formats_consistent(self):
        """Test that all formats export successfully."""
        model_id = "test_model"

        turtle = export_to_rdf(None, model_id)
        owl = export_ontology(None, model_id)
        jsonld = export_to_jsonld(None, model_id)

        # All should be non-empty and valid
        assert len(turtle) > 100
        assert len(owl) > 100
        assert len(jsonld) > 100

        # Each should mention the model
        assert model_id in turtle
        assert model_id in owl
        assert model_id in jsonld
