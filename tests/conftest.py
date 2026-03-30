import pytest
import duckdb

@pytest.fixture(scope="module")
def conn():
    conn = duckdb.connect(":memory:")

    # --------------------------------------------
    # Create metadata schema
    # --------------------------------------------
    conn.execute("CREATE SCHEMA metadata;")

    # --------------------------------------------
    # Create metadata schema
    # --------------------------------------------
    conn.execute("""
        CREATE TABLE metadata.model (
            model_id VARCHAR,
            model_name VARCHAR,
            status VARCHAR
        );
    """)

    # --------------------------------------------
    # Create metadata tables
    # --------------------------------------------
    conn.execute("""
        CREATE TABLE metadata.entity (
            entity_id VARCHAR,
            entity_name VARCHAR,
            model_id VARCHAR,
            entity_type VARCHAR,
            query_type VARCHAR,
            root_table_name VARCHAR,
            entity_hierarchy VARCHAR,
            qualify_clause VARCHAR,
            status VARCHAR,
            parent_query_id VARCHAR,
            canonical_path VARCHAR,
            inheritance_kind VARCHAR,
            origin_id VARCHAR,
            origin_type VARCHAR,
            raw_sql VARCHAR,
            combine_type VARCHAR,
            modifier VARCHAR,
            ordinal INTEGER,
            alias VARCHAR,
            source_origin_query VARCHAR,
            is_query_root BOOLEAN,
            comment VARCHAR
        );
    """)

    conn.execute("""
    CREATE TABLE metadata.attribute (
        entity_id VARCHAR,
        attribute_id VARCHAR,
        attribute_name VARCHAR,
        expression_sql VARCHAR,
        ordinal INTEGER,
        deprecated BOOLEAN,
        function_id VARCHAR,
        comment VARCHAR,
        aggregate_function VARCHAR
    );
    """)

    conn.execute("""
    CREATE TABLE metadata.entity_mapping (
        entity_id VARCHAR,
        mapping_id VARCHAR,
        source_type VARCHAR,
        source_schema VARCHAR,
        source_table VARCHAR,
        source_entity_id VARCHAR,
        source_alias VARCHAR,
        join_type VARCHAR,
        join_condition_sql VARCHAR,
        ordinal INTEGER,
        comment VARCHAR
    );
    """)

    conn.execute("""
    CREATE TABLE metadata.entity_mapping_key_mapping (
        left_entity_mapping_id VARCHAR,
        left_attribute_id VARCHAR,
        right_entity_mapping_id VARCHAR,
        right_attribute_id VARCHAR,
        operator VARCHAR,
        entity_mapping_id VARCHAR,
        predicate_sql VARCHAR,
        ordinal INTEGER
    );
    """)

    conn.execute("""
    CREATE TABLE metadata.entity_filter (
        entity_id VARCHAR,
        filter_id VARCHAR,
        clause_type VARCHAR,
        expression_sql VARCHAR,
        filter_def_id VARCHAR,
        logic VARCHAR,
        ordinal INTEGER
    );
    """)

    conn.execute("""
    CREATE TABLE metadata.filter_def (
        filter_def_id VARCHAR,
        sql_template VARCHAR
    );
    """)

    conn.execute("""
    CREATE TABLE metadata.filter_param_mapping (
        source_mapping_id VARCHAR,
        literal_type VARCHAR,
        filter_id VARCHAR,
        param_name VARCHAR,
        literal_value VARCHAR,
        source_attribute_id VARCHAR,
        ordinal INTEGER
    );
    """)

    conn.execute("""
    CREATE TABLE metadata.entity_window (
        entity_id VARCHAR,
        window_id VARCHAR,
        target_attribute_id VARCHAR,
        function_name VARCHAR,
        function_args VARCHAR,
        partition_by VARCHAR,
        order_by VARCHAR,
        frame_clause VARCHAR,
        expression_sql VARCHAR,
        ordinal INTEGER
    );
    """)

    conn.execute("""
    CREATE TABLE metadata.entity_order (
        entity_id VARCHAR,
        attribute_id VARCHAR,
        expression_sql VARCHAR,
        direction VARCHAR,
        ordinal INTEGER
    );
    """)

    conn.execute("""
    CREATE TABLE metadata.entity_time_selection (
        entity_id VARCHAR,
        time_profile_id VARCHAR,
        selection_type VARCHAR,
        time_attribute VARCHAR,
        time_to_attribute VARCHAR,
        point_in_time VARCHAR,
        range_start VARCHAR,
        range_end VARCHAR,
        calendar_table VARCHAR,
        calendar_granularity VARCHAR,
        join_condition VARCHAR,
        align_to_calendar BOOLEAN,
        stitch_gaps BOOLEAN
    );
    """)

    conn.execute("""
    CREATE TABLE metadata.time_profile_def (
        time_profile_id VARCHAR,
        selection_type VARCHAR,
        time_attribute VARCHAR,
        time_to_attribute VARCHAR,
        point_in_time VARCHAR,
        range_start VARCHAR,
        range_end VARCHAR,
        calendar_table VARCHAR,
        calendar_granularity VARCHAR,
        join_condition VARCHAR,
        regulatory_rule VARCHAR,
        regulatory_offset VARCHAR,
        align_to_calendar BOOLEAN,
        stitch_gaps BOOLEAN
    );
    """)

    _add_missing_tables(conn)
    return conn

def pytest_addoption(parser):
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Update SQL golden snapshots"
    )

import pytest

@pytest.fixture
def UPDATE_GOLDEN(request):
    return request.config.getoption("--update-golden")

# Additional tables needed by generator
def _add_missing_tables(conn):
    """Add missing tables to test database."""
    
    conn.execute("""
    CREATE TABLE IF NOT EXISTS metadata.attribute_mapping (
        target_entity_id VARCHAR,
        target_attribute_id VARCHAR,
        source_mapping_id VARCHAR,
        source_attribute_id VARCHAR,
        param_name VARCHAR,
        literal_value VARCHAR,
        ordinal INTEGER
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS metadata.entity_limit (
        entity_id VARCHAR,
        limit_value INTEGER,
        offset_value INTEGER
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS metadata.entity_group_by (
        entity_id VARCHAR,
        attribute_id VARCHAR,
        ordinal INTEGER,
        origin_id VARCHAR
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS metadata.entity_pivot_config (
        entity_id VARCHAR,
        key_attributes VARCHAR,
        pivot_columns VARCHAR,
        value_columns VARCHAR
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS metadata.seed_value (
        seed_id VARCHAR,
        literal_value VARCHAR
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS metadata.seed_usage (
        seed_id VARCHAR,
        object_id VARCHAR
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS metadata.function_def (
        function_id VARCHAR,
        sql_template VARCHAR,
        is_deterministic BOOLEAN
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS metadata.function_param_mapping (
        attribute_id VARCHAR,
        function_id VARCHAR,
        param_name VARCHAR,
        source_mapping_id VARCHAR,
        source_attribute_id VARCHAR,
        literal_value VARCHAR,
        literal_type VARCHAR,
        ordinal INTEGER
    );
    """)
