# Hybrid Metadata Strategy - MDDE Inspiration

Based on community feedback on SQL-only modeling article (Feb 2026).

## The Feedback

> "You would have a lot of duplication in your annotations if you have to document all the pipelines like this, especially on the attributes."
>
> "Have you considered moving these to DBML and have them on the data structures (source and target) so that the pipelines can leverage the rich metadata instead of duplicating and potentially contradicting?"
>
> "In the age of AI, I can't help but think that DBML is a nice approach to implement and document the semantic layer which can become a knowledge graph for business context!"

## The Problem: Annotation Duplication

Current approach in SQL-only modeling:

```sql
-- Pipeline 1: customers_to_silver.sql
-- @source: bronze.raw_customers
-- @target: silver.clean_customers
-- @attribute: customer_id | PK | Customer unique identifier
-- @attribute: email | PII | Customer email address
-- @attribute: segment | ENUM(enterprise,smb,consumer) | Business segment
SELECT ...

-- Pipeline 2: customers_to_gold.sql
-- @source: silver.clean_customers
-- @target: gold.dim_customer
-- @attribute: customer_id | PK | Customer unique identifier  -- DUPLICATED!
-- @attribute: email | PII | Customer email address           -- DUPLICATED!
-- @attribute: segment | ENUM | Business segment              -- DUPLICATED!
SELECT ...
```

**Issues:**
1. Same attributes documented multiple times
2. Definitions can diverge/contradict
3. Maintenance burden grows with pipeline count
4. No single source of truth

---

## The Solution: Hybrid Metadata Strategy

Instead of choosing ONE place for metadata, use a **layered approach** where each layer handles what it's best at:

### Layer 1: Structure Definitions (DDL/DBML)

Define column-level metadata in CREATE statements or DBML:

```sql
-- structures/silver_customers.sql
CREATE TABLE silver.clean_customers (
    customer_id VARCHAR(50) NOT NULL
        COMMENT 'Customer unique identifier'
        CONSTRAINT pk_customer PRIMARY KEY,

    email VARCHAR(255)
        COMMENT 'Customer email address'
        -- @tag: pii, gdpr
        ,

    segment VARCHAR(20)
        COMMENT 'Business segment classification'
        -- @enum: enterprise, smb, consumer
        CONSTRAINT chk_segment CHECK (segment IN ('enterprise', 'smb', 'consumer')),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        COMMENT 'Record creation timestamp'
)
COMMENT = 'Cleaned and validated customer data from CRM';
```

Or in DBML:

```dbml
Table silver.clean_customers {
  customer_id varchar(50) [pk, not null, note: 'Customer unique identifier']
  email varchar(255) [note: 'Customer email address', tags: ['pii', 'gdpr']]
  segment varchar(20) [note: 'Business segment', enum: ['enterprise', 'smb', 'consumer']]
  created_at timestamp [default: `CURRENT_TIMESTAMP`, note: 'Record creation timestamp']

  Note: 'Cleaned and validated customer data from CRM'
}
```

### Layer 2: Transformation Logic (SQL with CTE Documentation)

Document complex transformations inline with CTEs:

```sql
-- pipelines/build_customer_360.sql
-- @pipeline: customer_360_daily
-- @schedule: 0 6 * * *
-- @owner: data-platform@company.com

/*
 * Customer 360 Pipeline
 *
 * Builds unified customer view from multiple sources.
 * Destination: gold.dim_customer (SCD Type 2)
 */

WITH
-- CTE 1: Deduplicate raw customers
-- @description: Remove duplicate customer records, keep most recent
-- @grain: one row per customer_id
deduplicated_customers AS (
    SELECT
        customer_id,
        email,
        segment,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC
        ) as rn
    FROM silver.clean_customers
),

-- CTE 2: Enrich with order history
-- @description: Add order metrics per customer
-- @joins: fact_orders on customer_id
-- @metrics: total_orders, total_revenue, first_order_date, last_order_date
customer_orders AS (
    SELECT
        c.customer_id,
        COUNT(o.order_id) as total_orders,
        SUM(o.order_total) as total_revenue,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date
    FROM deduplicated_customers c
    LEFT JOIN silver.fact_orders o ON c.customer_id = o.customer_id
    WHERE c.rn = 1
    GROUP BY c.customer_id
),

-- CTE 3: Calculate customer health score
-- @description: Derive health score based on engagement
-- @business_rule: health = (recency * 0.4) + (frequency * 0.3) + (monetary * 0.3)
-- @context: enterprise_white_glove (high engagement = healthy)
customer_health AS (
    SELECT
        customer_id,
        total_orders,
        total_revenue,
        CASE
            WHEN last_order_date > CURRENT_DATE - INTERVAL '30 days' THEN 100
            WHEN last_order_date > CURRENT_DATE - INTERVAL '90 days' THEN 70
            WHEN last_order_date > CURRENT_DATE - INTERVAL '180 days' THEN 40
            ELSE 10
        END as recency_score,
        -- ... more scoring logic
    FROM customer_orders
)

-- Final output: gold.dim_customer
-- @target: gold.dim_customer
-- @stereotype: dim_scd2
-- @grain: one row per customer per version
-- @pk: customer_key (surrogate)
-- @nk: customer_id (natural key)
SELECT
    -- Surrogate key generated by target system
    customer_id,
    c.email,
    c.segment,
    h.total_orders,
    h.total_revenue,
    h.recency_score,
    -- SCD2 columns
    CURRENT_TIMESTAMP as valid_from,
    NULL as valid_to,
    TRUE as is_current
FROM deduplicated_customers c
JOIN customer_health h ON c.customer_id = h.customer_id
WHERE c.rn = 1;
```

### Layer 3: Platform Tags (Databricks, Snowflake, etc.)

Publish metadata as platform-native tags for governance:

```python
# publish_tags.py - Sync MDDE metadata to Databricks Unity Catalog

from mdde.integrations.unity_catalog import UnityTagPublisher

publisher = UnityTagPublisher(conn, databricks_client)

# Publish from MDDE metadata
publisher.publish_model_tags("customer_360", tags={
    # Column-level tags
    "columns": {
        "customer_id": ["pk", "not_null"],
        "email": ["pii", "gdpr", "mask_on_export"],
        "segment": ["dimension", "enum"],
        "total_revenue": ["metric", "currency_usd"],
    },
    # Table-level tags
    "table": ["gold", "certified", "daily_refresh"],
    # Lineage tags
    "lineage": {
        "sources": ["silver.clean_customers", "silver.fact_orders"],
        "pipeline": "customer_360_daily",
    }
})
```

Result in Databricks:

```sql
-- Tags visible in Unity Catalog
SHOW TAGS ON TABLE gold.dim_customer;
-- tag_name        | tag_value
-- gold            | true
-- certified       | true
-- daily_refresh   | true

SHOW TAGS ON COLUMN gold.dim_customer.email;
-- tag_name        | tag_value
-- pii             | true
-- gdpr            | true
-- mask_on_export  | true
```

---

## Hybrid Metadata Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         METADATA SOURCES                             │
├─────────────────┬─────────────────┬─────────────────┬───────────────┤
│   DDL/DBML      │   SQL CTEs      │   MDDE YAML     │  Platform     │
│   (Structure)   │ (Transformation)│   (Governance)  │   (Tags)      │
├─────────────────┼─────────────────┼─────────────────┼───────────────┤
│ • Column types  │ • CTE purpose   │ • Stereotypes   │ • PK/FK       │
│ • PKs/FKs       │ • Grain         │ • Quality rules │ • PII         │
│ • Comments      │ • Joins         │ • Owners        │ • Certified   │
│ • Constraints   │ • Business rules│ • Domains       │ • Lineage     │
│ • Enums         │ • Metrics       │ • SLAs          │ • Sensitivity │
└─────────────────┴─────────────────┴─────────────────┴───────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   MDDE Parser   │
                    │   (Unified)     │
                    └────────┬────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  MDDE Metadata  │
                    │   Repository    │
                    └────────┬────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
     ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
     │   AI/LLM    │  │  Governance │  │  Platform   │
     │   Context   │  │  Dashboard  │  │   Tags      │
     └─────────────┘  └─────────────┘  └─────────────┘
```

---

## Proposed Enhancement: ADR-365 Hybrid Metadata Strategy

### 1. DDL Comment Parser

Extract metadata from CREATE TABLE comments:

```python
class DDLMetadataParser:
    """Parse metadata from DDL comments."""

    def parse_create_table(self, sql: str) -> EntityMetadata:
        """
        Extract metadata from CREATE TABLE statement.

        Parses:
        - Column COMMENTs
        - @tag annotations
        - @enum values
        - Constraint names (PK, FK, CHECK)
        - Table COMMENT
        """
        pass

    def parse_inline_tags(self, comment: str) -> List[Tag]:
        """
        Parse @tag: value annotations from comments.

        Example:
            'Customer email -- @tag: pii, gdpr'
            Returns: [Tag('pii'), Tag('gdpr')]
        """
        pass
```

### 2. CTE Documentation Parser

Extract metadata from CTE comments:

```python
class CTEMetadataParser:
    """Parse metadata from SQL CTE comments."""

    def parse_pipeline(self, sql: str) -> PipelineMetadata:
        """
        Extract metadata from pipeline SQL.

        Parses:
        - Pipeline-level annotations (@pipeline, @schedule, @owner)
        - CTE-level annotations (@description, @grain, @joins)
        - Final SELECT annotations (@target, @stereotype)
        """
        pass

    def extract_cte_metadata(self, cte_sql: str) -> CTEMetadata:
        """
        Extract metadata from single CTE.

        Returns:
        - Description
        - Grain
        - Joins
        - Metrics calculated
        - Business rules applied
        """
        pass
```

### 3. Platform Tag Publisher

Publish MDDE metadata as platform tags:

```python
class PlatformTagPublisher:
    """Publish MDDE metadata to platform tags."""

    def publish_to_unity_catalog(
        self,
        model_id: str,
        tag_mapping: TagMapping,
    ) -> PublishResult:
        """
        Publish tags to Databricks Unity Catalog.

        Tag types:
        - PK/FK: Primary and foreign key markers
        - PII: Personal identifiable information
        - Classification: Data sensitivity level
        - Certified: Data quality certification
        - Lineage: Source tables and pipelines
        """
        pass

    def publish_to_snowflake(
        self,
        model_id: str,
        tag_mapping: TagMapping,
    ) -> PublishResult:
        """Publish tags to Snowflake."""
        pass

    def publish_to_purview(
        self,
        model_id: str,
        tag_mapping: TagMapping,
    ) -> PublishResult:
        """Publish tags to Microsoft Purview."""
        pass
```

### 4. Tag Mapping Configuration

```yaml
# tag_mapping.yaml
mappings:
  # Map MDDE classifications to platform tags
  unity_catalog:
    pii: "pii"
    pk: "primary_key"
    fk: "foreign_key"
    metric: "metric"
    dimension: "dimension"
    certified: "certified"

  snowflake:
    pii: "PII"
    pk: "PRIMARY_KEY"
    sensitive: "SENSITIVE"

  purview:
    pii: "Microsoft.PersonalData"
    pci: "Microsoft.Financial"

# Auto-publish rules
auto_publish:
  on_model_change: true
  platforms: [unity_catalog, purview]
  include_tags: [pk, fk, pii, certified]
```

---

## Where Each Metadata Type Lives

| Metadata Type | Primary Source | Reason |
|--------------|----------------|--------|
| **Column name/type** | DDL | Schema definition |
| **Column description** | DDL COMMENT | Close to structure |
| **PK/FK constraints** | DDL | Enforced by database |
| **PII/sensitivity** | DDL @tag or MDDE | Governance policy |
| **Enums/valid values** | DDL CHECK or @enum | Data validation |
| **CTE purpose** | SQL comment | Inline with logic |
| **Transformation grain** | SQL @grain | Pipeline-specific |
| **Business rules** | SQL @business_rule | Logic documentation |
| **Joins/dependencies** | SQL @joins or inferred | Lineage |
| **Stereotypes** | MDDE YAML | MDDE-specific patterns |
| **Quality checks** | MDDE YAML | Executable rules |
| **Owners/stewards** | MDDE YAML | Governance |
| **Platform tags** | Published from MDDE | Runtime governance |

---

## Example: Full Hybrid Flow

### Step 1: Define Structure (DDL)

```sql
-- structures/gold/dim_customer.sql
CREATE TABLE gold.dim_customer (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate key'
        CONSTRAINT pk_dim_customer PRIMARY KEY,

    customer_id VARCHAR(50) NOT NULL
        COMMENT 'Natural key from source system'
        -- @tag: nk, not_null
        ,

    email VARCHAR(255)
        COMMENT 'Customer email address'
        -- @tag: pii, gdpr, mask_on_export
        ,

    segment VARCHAR(20)
        COMMENT 'Business segment classification'
        -- @enum: enterprise, smb, consumer
        -- @tag: dimension
        ,

    total_revenue DECIMAL(15,2)
        COMMENT 'Lifetime revenue from customer'
        -- @tag: metric, currency_usd
        ,

    health_score INT
        COMMENT 'Customer health score (0-100)'
        -- @tag: metric, derived
        -- @business_context: enterprise_white_glove
        ,

    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL

) COMMENT = 'Customer dimension with SCD Type 2 history';
```

### Step 2: Document Transformation (SQL CTE)

```sql
-- pipelines/customer_360_daily.sql
-- @pipeline: customer_360_daily
-- @stereotype: dim_scd2
-- @target: gold.dim_customer

WITH
-- @cte: dedupe | Remove duplicates keeping most recent
-- @grain: one row per customer_id
deduplicated AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) as rn
    FROM silver.clean_customers
),

-- @cte: enrich | Add order metrics
-- @joins: silver.fact_orders
-- @metrics: total_revenue, order_count
enriched AS (
    SELECT
        d.customer_id,
        d.email,
        d.segment,
        SUM(o.order_total) as total_revenue,
        COUNT(o.order_id) as order_count
    FROM deduplicated d
    LEFT JOIN silver.fact_orders o ON d.customer_id = o.customer_id
    WHERE d.rn = 1
    GROUP BY d.customer_id, d.email, d.segment
),

-- @cte: score | Calculate health score
-- @business_rule: RFM scoring model
-- @context: enterprise_white_glove (high engagement = healthy)
scored AS (
    SELECT
        *,
        CASE
            WHEN order_count > 10 AND total_revenue > 10000 THEN 100
            WHEN order_count > 5 THEN 70
            ELSE 30
        END as health_score
    FROM enriched
)

SELECT * FROM scored;
```

### Step 3: Add Governance (MDDE YAML)

```yaml
# models/customer_360.yaml
model:
  id: customer_360
  name: Customer 360

entities:
  - id: dim_customer
    stereotype: dim_scd2
    owner: customer-data-team@company.com
    domain: customer_success
    certified: true
    sla:
      freshness: 24h
      quality_threshold: 99.5

quality_checks:
  - entity: dim_customer
    checks:
      - not_null: [customer_id, email]
      - unique: [customer_id, valid_from]
      - range: { health_score: [0, 100] }
```

### Step 4: Publish to Platform

```python
# Sync to Databricks Unity Catalog
from mdde.integrations.unity_catalog import UnityTagPublisher

publisher = UnityTagPublisher(conn, databricks_client)
result = publisher.publish_model("customer_360")

# Result:
# - gold.dim_customer tagged with: certified, scd2, daily_refresh
# - email column tagged with: pii, gdpr
# - customer_id tagged with: primary_key, natural_key
# - health_score tagged with: metric, derived
```

---

## Databricks Comment Header

Deploy rich metadata as a comment header in Databricks notebooks/views:

```sql
-- ============================================================================
-- TABLE: gold.dim_customer
-- ============================================================================
-- Description:    Customer dimension with SCD Type 2 history
-- Stereotype:     dim_scd2
-- Owner:          customer-data-team@company.com
-- Domain:         customer_success
-- Certified:      Yes
-- SLA Freshness:  24h
-- Last Updated:   2026-02-27T10:30:00Z
-- MDDE Version:   3.62.0
-- ============================================================================
-- LINEAGE:
--   Sources:      silver.clean_customers, silver.fact_orders
--   Pipeline:     customer_360_daily
--   Schedule:     Daily 06:00 UTC
-- ============================================================================
-- COLUMNS:
--   customer_key   | BIGINT      | PK, Surrogate key
--   customer_id    | VARCHAR(50) | NK, Natural key from source
--   email          | VARCHAR     | PII, GDPR, Customer email
--   segment        | VARCHAR(20) | Dimension, Enum(enterprise,smb,consumer)
--   total_revenue  | DECIMAL     | Metric, Currency USD
--   health_score   | INT         | Metric, Derived, Range(0-100)
--   valid_from     | TIMESTAMP   | SCD2 start
--   valid_to       | TIMESTAMP   | SCD2 end
--   is_current     | BOOLEAN     | SCD2 current flag
-- ============================================================================
-- QUALITY CHECKS:
--   - not_null: customer_id, email
--   - unique: (customer_id, valid_from)
--   - range: health_score [0, 100]
-- ============================================================================

CREATE OR REPLACE TABLE gold.dim_customer AS
...
```

### Comment Header Generator

```python
class CommentHeaderGenerator:
    """Generate rich comment headers for Databricks."""

    def generate_table_header(
        self,
        entity_id: str,
        include_lineage: bool = True,
        include_columns: bool = True,
        include_quality: bool = True,
    ) -> str:
        """
        Generate comment header for table.

        Includes:
        - Table metadata (owner, domain, SLA)
        - Lineage (sources, pipeline)
        - Column catalog with types and tags
        - Quality checks
        """
        pass

    def generate_view_header(self, view_id: str) -> str:
        """Generate header for view/CTE."""
        pass

    def inject_header(self, sql: str, header: str) -> str:
        """Inject header at top of SQL file."""
        pass
```

---

## Metadata Ownership & Versioning

When metadata lives in multiple places, we need clear ownership rules:

### Ownership Matrix

| Metadata Type | Owner | Source of Truth | Sync Direction |
|--------------|-------|-----------------|----------------|
| **Column types** | DBA/Platform | DDL | DDL → MDDE |
| **Column descriptions** | Data Engineer | DDL COMMENT | DDL → MDDE |
| **PK/FK constraints** | DBA | DDL | DDL → MDDE |
| **PII/sensitivity** | Data Governance | MDDE | MDDE → Platform |
| **Business definitions** | Data Steward | MDDE | MDDE → Platform |
| **Stereotypes** | Data Architect | MDDE | MDDE only |
| **Quality rules** | Data Engineer | MDDE | MDDE → Platform |
| **Lineage** | Auto-inferred | MDDE | MDDE → Platform |
| **Platform tags** | Published | MDDE | MDDE → Platform |

### Version Control

```yaml
# metadata_manifest.yaml
# Tracks metadata versions across sources

manifest:
  mdde_version: "3.62.0"
  generated_at: "2026-02-27T10:30:00Z"

sources:
  - type: ddl
    path: structures/gold/dim_customer.sql
    checksum: sha256:abc123...
    last_modified: "2026-02-26T15:00:00Z"

  - type: pipeline
    path: pipelines/customer_360_daily.sql
    checksum: sha256:def456...
    last_modified: "2026-02-27T09:00:00Z"

  - type: mdde_model
    path: models/customer_360.yaml
    checksum: sha256:ghi789...
    last_modified: "2026-02-27T10:00:00Z"

published:
  - platform: unity_catalog
    published_at: "2026-02-27T10:30:00Z"
    manifest_version: "1.2.3"
    tags_published: 47

conflicts:
  # Track when sources disagree
  - field: email.description
    ddl_value: "Customer email"
    mdde_value: "Primary contact email address"
    resolution: mdde_wins
    resolved_at: "2026-02-27T10:30:00Z"
```

### Conflict Resolution

```python
class MetadataConflictResolver:
    """Resolve conflicts when metadata sources disagree."""

    def __init__(self, priority_order: List[str]):
        """
        Initialize with priority order.

        Example: ["mdde", "ddl", "platform"]
        MDDE wins over DDL, DDL wins over platform.
        """
        self.priority = priority_order

    def detect_conflicts(
        self,
        model_id: str,
    ) -> List[MetadataConflict]:
        """Find all conflicts between sources."""
        pass

    def resolve(
        self,
        conflict: MetadataConflict,
        strategy: str = "priority",  # or "manual", "newest"
    ) -> Resolution:
        """Resolve a conflict."""
        pass

    def generate_conflict_report(
        self,
        model_id: str,
    ) -> ConflictReport:
        """Generate report of all conflicts and resolutions."""
        pass
```

### Release Tracking

```yaml
# releases/customer_360_v1.2.0.yaml
release:
  model: customer_360
  version: "1.2.0"
  released_at: "2026-02-27T10:30:00Z"
  released_by: data-platform@company.com

changes:
  - type: column_added
    entity: dim_customer
    column: health_score
    description: "Added customer health score metric"

  - type: tag_added
    entity: dim_customer
    column: email
    tag: gdpr
    reason: "GDPR compliance requirement"

metadata_checksums:
  ddl: sha256:abc123...
  mdde: sha256:def456...
  platform_tags: sha256:ghi789...

dependencies:
  - silver.clean_customers: ">=1.0.0"
  - silver.fact_orders: ">=2.1.0"
```

---

## Atomic Queries

Queries with embedded metadata are **atomic** - they are self-contained and can execute without any external metadata store. During deployment, metadata is **extracted** from the code and published to the environment.

### The Atomic Query Concept

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ATOMIC QUERY                                  │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  -- @pipeline: customer_360                                  │    │
│  │  -- @target: gold.dim_customer                               │    │
│  │  -- @owner: data-team@company.com                            │    │
│  │  -- @depends_on: silver.customers, silver.orders             │    │
│  │                                                               │    │
│  │  SELECT ... FROM silver.customers c                          │    │
│  │  JOIN silver.orders o ON c.id = o.customer_id                │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  Can execute standalone ✓   Metadata embedded ✓   Version control ✓ │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ DEPLOYMENT
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      ENVIRONMENT (Dev/Test/Prod)                     │
├─────────────────┬─────────────────┬─────────────────────────────────┤
│  ETL Scheduler  │  Metadata Store │  Platform Tags                  │
├─────────────────┼─────────────────┼─────────────────────────────────┤
│  Dependencies:  │  MDDE Database: │  Unity Catalog:                 │
│  silver.custs   │  - Lineage      │  - PK tags                      │
│  silver.orders  │  - Owners       │  - PII tags                     │
│  ↓              │  - Quality      │  - Certified                    │
│  gold.dim_cust  │  - Business ctx │  - Freshness                    │
└─────────────────┴─────────────────┴─────────────────────────────────┘
```

### Why Atomic?

1. **Self-contained** - Query runs without external dependencies
2. **Portable** - Move between environments without metadata loss
3. **Reviewable** - All context visible in code review
4. **Testable** - Run in isolation during development
5. **Extractable** - Metadata harvested during deployment

### Deployment Flow

```python
class AtomicQueryDeployer:
    """Deploy atomic queries and extract metadata."""

    def deploy(
        self,
        query_path: Path,
        environment: str,  # dev, test, prod
    ) -> DeploymentResult:
        """
        Deploy atomic query to environment.

        Steps:
        1. Parse query and extract metadata annotations
        2. Validate query executes successfully
        3. Register dependencies in ETL scheduler
        4. Store metadata in MDDE repository
        5. Publish tags to platform (Unity Catalog, etc.)
        6. Update lineage graph
        """
        pass

    def extract_metadata(self, sql: str) -> QueryMetadata:
        """
        Extract all metadata from atomic query.

        Returns:
        - Pipeline info (@pipeline, @owner, @schedule)
        - Dependencies (@depends_on, @sources)
        - Target info (@target, @stereotype)
        - Column metadata (from CTEs)
        - Quality checks (@check)
        """
        pass

    def register_dependencies(
        self,
        metadata: QueryMetadata,
        scheduler: ETLScheduler,
    ) -> None:
        """
        Register dependencies in ETL orchestrator.

        Creates DAG edges:
        - silver.customers → gold.dim_customer
        - silver.orders → gold.dim_customer
        """
        pass
```

### Atomic Query Example

```sql
-- ============================================================================
-- ATOMIC QUERY: customer_360_daily
-- ============================================================================
-- @pipeline: customer_360_daily
-- @version: 1.2.0
-- @owner: data-platform@company.com
-- @schedule: 0 6 * * *
-- @target: gold.dim_customer
-- @stereotype: dim_scd2
-- @depends_on: silver.clean_customers, silver.fact_orders
-- ============================================================================

WITH
-- @cte: dedupe
-- @description: Remove duplicate customers, keep most recent
-- @grain: one row per customer_id
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) as rn
    FROM silver.clean_customers
),

-- @cte: enriched
-- @description: Add order metrics
-- @joins: silver.fact_orders
enriched AS (
    SELECT
        d.customer_id,
        d.email,           -- @tag: pii
        d.segment,         -- @enum: enterprise, smb, consumer
        SUM(o.total) as lifetime_value,  -- @metric: currency_usd
        COUNT(*) as order_count          -- @metric: count
    FROM deduplicated d
    LEFT JOIN silver.fact_orders o ON d.customer_id = o.customer_id
    WHERE d.rn = 1
    GROUP BY d.customer_id, d.email, d.segment
)

-- @output: gold.dim_customer
-- @pk: customer_id
-- @check: not_null(customer_id, email)
-- @check: unique(customer_id)
-- @check: range(lifetime_value, 0, null)
SELECT
    customer_id,
    email,
    segment,
    lifetime_value,
    order_count,
    CURRENT_TIMESTAMP as valid_from,
    NULL as valid_to,
    TRUE as is_current
FROM enriched;
```

### Extraction During Deployment

```bash
# Deploy to dev environment
mdde deploy pipelines/customer_360_daily.sql --env dev

# Output:
# ✓ Parsed atomic query
# ✓ Extracted metadata:
#   - Pipeline: customer_360_daily v1.2.0
#   - Owner: data-platform@company.com
#   - Dependencies: silver.clean_customers, silver.fact_orders
#   - Target: gold.dim_customer (dim_scd2)
#   - Quality checks: 3
#   - Tags: 4 columns tagged
# ✓ Registered in Airflow DAG
# ✓ Stored in MDDE metadata repository
# ✓ Published tags to Unity Catalog
# ✓ Updated lineage graph

# Deploy to prod (with approval)
mdde deploy pipelines/customer_360_daily.sql --env prod --approve
```

### ETL Dependency Extraction

```python
class DependencyExtractor:
    """Extract ETL dependencies from atomic queries."""

    def extract_dag(
        self,
        query_paths: List[Path],
    ) -> DAG:
        """
        Build DAG from collection of atomic queries.

        For each query:
        1. Parse @depends_on annotations
        2. Parse @target annotation
        3. Create edges: depends_on → target
        """
        pass

    def generate_airflow_dag(self, dag: DAG) -> str:
        """Generate Airflow DAG definition."""
        pass

    def generate_databricks_workflow(self, dag: DAG) -> str:
        """Generate Databricks Workflow JSON."""
        pass

    def detect_cycles(self, dag: DAG) -> List[Cycle]:
        """Detect circular dependencies."""
        pass
```

### Environment-Specific Extraction

```yaml
# environments/prod.yaml
environment: prod

extraction:
  # What to extract from atomic queries
  metadata:
    - lineage
    - ownership
    - quality_checks
    - dependencies

  # Where to publish
  targets:
    - type: mdde_repository
      connection: ${MDDE_PROD_CONNECTION}

    - type: unity_catalog
      workspace: ${DATABRICKS_PROD_WORKSPACE}
      tags: [pk, fk, pii, certified]

    - type: airflow
      dag_folder: /opt/airflow/dags/
      generate_dag: true

    - type: purview
      account: ${PURVIEW_ACCOUNT}
      publish_lineage: true

  # Validation before deployment
  validation:
    - query_must_execute: true
    - dependencies_must_exist: true
    - quality_checks_defined: true
    - owner_required: true
```

### Annotation Placement Strategy

Where to put each type of annotation:

```
┌─────────────────────────────────────────────────────────────────────┐
│  HEADER BLOCK (Pipeline-level)                                       │
│  ────────────────────────────────                                    │
│  @pipeline, @version, @owner, @schedule, @target, @stereotype        │
│  @depends_on (explicit dependencies), @sla, @certified               │
├─────────────────────────────────────────────────────────────────────┤
│  CTE COMMENTS (Transformation-level)                                 │
│  ──────────────────────────────────                                  │
│  @cte (name), @description, @grain, @joins, @business_rule           │
│  @filter, @aggregation, @window                                      │
├─────────────────────────────────────────────────────────────────────┤
│  COLUMN ANNOTATIONS (inline with SELECT)                             │
│  ───────────────────────────────────────                             │
│  -- @tag: pii, gdpr                                                  │
│  -- @metric: currency_usd                                            │
│  -- @enum: value1, value2                                            │
│  -- @derived: formula description                                    │
├─────────────────────────────────────────────────────────────────────┤
│  FINAL OUTPUT COMMENT (Target-level)                                 │
│  ─────────────────────────────────                                   │
│  @output (confirms target), @pk, @nk, @check (quality)               │
└─────────────────────────────────────────────────────────────────────┘
```

### Complete Annotated Example

```sql
-- ============================================================================
-- HEADER: Pipeline-Level Metadata
-- ============================================================================
-- @pipeline: customer_360_daily
-- @version: 1.2.0
-- @owner: data-platform@company.com
-- @schedule: 0 6 * * *
-- @target: gold.dim_customer
-- @stereotype: dim_scd2
-- @depends_on: silver.clean_customers, silver.fact_orders
-- @sla: freshness=6h, quality=99.5%
-- @certified: true
-- @description: |
--   Builds unified customer dimension with order metrics.
--   SCD Type 2 for historical tracking.
-- ============================================================================

WITH
-- ============================================================================
-- CTE: Deduplication
-- @cte: deduplicated
-- @description: Remove duplicate customer records, keep most recent
-- @grain: one row per customer_id
-- @filter: keeps only latest record per customer
-- ============================================================================
deduplicated AS (
    SELECT
        customer_id,
        email,
        first_name,
        last_name,
        segment,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC
        ) as rn
    FROM silver.clean_customers
),

-- ============================================================================
-- CTE: Order Enrichment
-- @cte: order_metrics
-- @description: Calculate customer order metrics
-- @grain: one row per customer_id
-- @joins: silver.fact_orders ON customer_id
-- @aggregation: SUM(order_total), COUNT(order_id), MIN/MAX(order_date)
-- ============================================================================
order_metrics AS (
    SELECT
        d.customer_id,
        COUNT(o.order_id) as total_orders,
        COALESCE(SUM(o.order_total), 0) as lifetime_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date
    FROM deduplicated d
    LEFT JOIN silver.fact_orders o ON d.customer_id = o.customer_id
    WHERE d.rn = 1
    GROUP BY d.customer_id
),

-- ============================================================================
-- CTE: Health Score Calculation
-- @cte: scored
-- @description: Calculate customer health score based on RFM model
-- @grain: one row per customer_id
-- @business_rule: health = recency(40%) + frequency(30%) + monetary(30%)
-- @business_context: enterprise_white_glove
-- ============================================================================
scored AS (
    SELECT
        d.customer_id,
        d.email,
        d.first_name,
        d.last_name,
        d.segment,
        m.total_orders,
        m.lifetime_value,
        m.first_order_date,
        m.last_order_date,
        -- Health score calculation
        CASE
            WHEN m.last_order_date > CURRENT_DATE - INTERVAL '30 days' THEN 40
            WHEN m.last_order_date > CURRENT_DATE - INTERVAL '90 days' THEN 28
            WHEN m.last_order_date > CURRENT_DATE - INTERVAL '180 days' THEN 16
            ELSE 4
        END +
        CASE
            WHEN m.total_orders > 10 THEN 30
            WHEN m.total_orders > 5 THEN 21
            WHEN m.total_orders > 2 THEN 12
            ELSE 3
        END +
        CASE
            WHEN m.lifetime_value > 10000 THEN 30
            WHEN m.lifetime_value > 5000 THEN 21
            WHEN m.lifetime_value > 1000 THEN 12
            ELSE 3
        END as health_score
    FROM deduplicated d
    JOIN order_metrics m ON d.customer_id = m.customer_id
    WHERE d.rn = 1
)

-- ============================================================================
-- FINAL OUTPUT: gold.dim_customer
-- @output: gold.dim_customer
-- @pk: customer_key (surrogate, auto-generated)
-- @nk: customer_id
-- @check: not_null(customer_id, email, segment)
-- @check: unique(customer_id, valid_from)
-- @check: range(health_score, 0, 100)
-- @check: enum(segment, 'enterprise', 'smb', 'consumer')
-- ============================================================================
SELECT
    -- Surrogate key (handled by target system)
    customer_id,                          -- @nk: natural key
    email,                                -- @tag: pii, gdpr, mask_on_export
    first_name,                           -- @tag: pii
    last_name,                            -- @tag: pii
    CONCAT(first_name, ' ', last_name)    -- @derived: concatenation of first_name + last_name
        as full_name,
    segment,                              -- @tag: dimension
                                          -- @enum: enterprise, smb, consumer
    total_orders,                         -- @tag: metric
                                          -- @aggregation: count
    lifetime_value,                       -- @tag: metric, currency_usd
                                          -- @aggregation: sum
    first_order_date,                     -- @tag: dimension, date
    last_order_date,                      -- @tag: dimension, date
    health_score,                         -- @tag: metric, derived
                                          -- @range: 0-100
                                          -- @business_context: enterprise_white_glove
    -- SCD2 columns
    CURRENT_TIMESTAMP as valid_from,
    CAST(NULL AS TIMESTAMP) as valid_to,
    TRUE as is_current
FROM scored;
```

### Annotation Reference

| Location | Annotation | Purpose |
|----------|------------|---------|
| **Header** | `@pipeline` | Pipeline identifier |
| | `@version` | Semantic version |
| | `@owner` | Contact email/team |
| | `@schedule` | Cron expression |
| | `@target` | Output table/view |
| | `@stereotype` | MDDE pattern (dim_scd2, etc.) |
| | `@depends_on` | Explicit dependencies |
| | `@sla` | Service level agreement |
| | `@certified` | Quality certification |
| | `@description` | Multi-line description |
| **CTE** | `@cte` | CTE identifier |
| | `@description` | What this CTE does |
| | `@grain` | Row granularity |
| | `@joins` | Tables joined |
| | `@filter` | Filter logic applied |
| | `@aggregation` | Aggregations performed |
| | `@window` | Window functions used |
| | `@business_rule` | Business logic applied |
| | `@business_context` | Interpretation context |
| **Column** | `@tag` | Classification tags (pii, metric, etc.) |
| | `@enum` | Valid values |
| | `@range` | Valid range |
| | `@derived` | Derivation formula |
| | `@aggregation` | How to aggregate |
| | `@currency` | Currency code |
| | `@format` | Display format |
| **Output** | `@output` | Confirms target table |
| | `@pk` | Primary key column(s) |
| | `@nk` | Natural key column(s) |
| | `@check` | Quality check expression |

### Benefits of Atomic Queries

| Benefit | Description |
|---------|-------------|
| **Independence** | Query runs without metadata store connection |
| **Portability** | Move SQL file between environments, metadata travels with it |
| **Auditability** | Git history shows all metadata changes |
| **Simplicity** | One file = one pipeline, complete definition |
| **Deployment automation** | CI/CD extracts and publishes metadata |
| **No drift** | Metadata always matches executing code |

---

## Benefits of Hybrid Approach

1. **No duplication** - Each metadata type defined once in best location
2. **Inheritance** - Pipelines reference structures, don't redefine
3. **Platform integration** - Tags published to governance tools
4. **AI-ready** - Structured metadata for LLM context
5. **Version control** - All sources are text files
6. **Flexibility** - Use what works for your workflow
7. **Clear ownership** - Each metadata type has defined owner
8. **Conflict detection** - Know when sources disagree
9. **Release tracking** - Version metadata changes over time
10. **Atomic queries** - Self-contained, extractable during deployment

---

## Implementation Priority

| Feature | Priority | Effort |
|---------|----------|--------|
| DDL Comment Parser | HIGH | Medium |
| CTE Metadata Parser | HIGH | Medium |
| Unity Catalog Tag Publisher | HIGH | Medium |
| Snowflake Tag Publisher | MEDIUM | Medium |
| Purview Tag Publisher | MEDIUM | Medium |
| Tag Mapping Config | HIGH | Low |
| Auto-publish on change | LOW | Medium |

---

## References

- [DBML - Database Markup Language](https://dbml.dbdiagram.io/)
- [Databricks Unity Catalog Tags](https://docs.databricks.com/data-governance/unity-catalog/tags.html)
- [Snowflake Object Tagging](https://docs.snowflake.com/en/user-guide/object-tagging)
- [Microsoft Purview Classifications](https://docs.microsoft.com/en-us/azure/purview/concept-classifications)
- MDDE ADR-332: Unity Catalog Integration
- MDDE ADR-359: Business Ontology
