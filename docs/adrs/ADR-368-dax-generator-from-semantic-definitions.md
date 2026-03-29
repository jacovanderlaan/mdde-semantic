# ADR-368: DAX Generator from Semantic Definitions

## Status
Proposed

## Context

Many people find DAX complicated. Its unique evaluation context model (filter context vs row context), context transition behavior, and function syntax create a steep learning curve.

**MDDE already defines logical transformations that generate SQL.** The same semantic definitions should generate DAX for Power BI, eliminating the need to write DAX manually.

### The Insight

From the "Ultimate DAX Formula Guide", we see that DAX follows predictable patterns:
- **Business Requirement** → **Formula Pattern** → **Expected Behavior**

MDDE's semantic layer already captures the business requirement. We just need to map it to DAX patterns automatically.

### Current MDDE Semantic Model

```yaml
# From ADR-301 Semantic Layer
metrics:
  - name: total_revenue
    type: SIMPLE
    expression: SUM(sales.amount)
    description: "Total sales revenue"

  - name: revenue_yoy_pct
    type: DERIVED
    expression: "(total_revenue - total_revenue_ly) / total_revenue_ly"
    description: "Year-over-year revenue growth"
```

This should generate both SQL and DAX automatically.

## Decision

Implement a **DAX Generator** that converts MDDE semantic definitions to DAX measures, calculated columns, and calculated tables.

### Core Principle: Define Once, Generate Everywhere

```
Business Logic (MDDE YAML)
         ↓
    ┌────┴────┐
    ↓         ↓
   SQL       DAX
(BigQuery)  (Power BI)
(Snowflake)
(DuckDB)
```

## Implementation

### 1. DAX Expression Generator (`src/mdde/dax/generator/`)

```python
class DAXGenerator:
    """Generate DAX from MDDE semantic definitions."""

    def generate_measure(self, metric: SemanticMetric) -> str:
        """Convert semantic metric to DAX measure."""

    def generate_calculated_column(self, attribute: Attribute) -> str:
        """Convert derived attribute to DAX calculated column."""

    def generate_calculated_table(self, entity: Entity) -> str:
        """Convert entity transformation to DAX calculated table."""
```

### 2. Expression Mapping

Map MDDE logical expressions to DAX:

| MDDE Expression | DAX Output |
|-----------------|------------|
| `SUM(sales.amount)` | `SUM(Sales[Amount])` |
| `COUNT(DISTINCT customer.id)` | `DISTINCTCOUNT(Customer[ID])` |
| `sales.amount > 0` | `Sales[Amount] > 0` |
| `COALESCE(a, b, 0)` | `COALESCE(Sales[A], Sales[B], 0)` |
| `date - 1 YEAR` | `SAMEPERIODLASTYEAR(Calendar[Date])` |
| `YTD(revenue)` | `TOTALYTD([Revenue], Calendar[Date])` |
| `RANK BY revenue DESC` | `RANKX(ALL(Products), [Revenue], , DESC)` |

### 3. Semantic Metric Types → DAX Patterns

From ADR-301 Semantic Layer:

```python
class MetricType(Enum):
    SIMPLE = "simple"        # Direct aggregation
    DERIVED = "derived"      # Calculated from other metrics
    CUMULATIVE = "cumulative"  # Running total
    RATIO = "ratio"          # Division of two metrics
    CONVERSION = "conversion"  # Funnel conversion rate
```

Each maps to specific DAX patterns:

```python
DAX_PATTERNS = {
    MetricType.SIMPLE: """
{name} = {aggregation}({table}[{column}])
""",

    MetricType.DERIVED: """
{name} =
VAR Base = {base_expression}
VAR Comparison = {comparison_expression}
RETURN {derived_expression}
""",

    MetricType.CUMULATIVE: """
{name} =
CALCULATE(
    {base_measure},
    FILTER(
        ALL({date_table}),
        {date_column} <= MAX({date_column})
    )
)
""",

    MetricType.RATIO: """
{name} =
VAR Numerator = {numerator_measure}
VAR Denominator = {denominator_measure}
RETURN DIVIDE(Numerator, Denominator)
""",

    MetricType.CONVERSION: """
{name} =
VAR StepA = {step_a_measure}
VAR StepB = {step_b_measure}
RETURN DIVIDE(StepB, StepA)
""",
}
```

### 4. Time Intelligence Auto-Generation

When metrics reference time, auto-generate time intelligence variants:

```yaml
# MDDE Definition
metrics:
  - name: revenue
    type: SIMPLE
    expression: SUM(sales.amount)
    time_dimension: order_date  # Triggers time intelligence
```

Auto-generates:

```dax
Revenue = SUM(Sales[Amount])

Revenue LY = CALCULATE([Revenue], SAMEPERIODLASTYEAR(Calendar[Date]))

Revenue YoY = [Revenue] - [Revenue LY]

Revenue YoY % = DIVIDE([Revenue YoY], [Revenue LY])

Revenue YTD = TOTALYTD([Revenue], Calendar[Date])

Revenue MTD = TOTALMTD([Revenue], Calendar[Date])

Revenue QTD = TOTALQTD([Revenue], Calendar[Date])

Revenue Rolling 12M =
CALCULATE(
    [Revenue],
    DATESINPERIOD(Calendar[Date], MAX(Calendar[Date]), -12, MONTH)
)
```

### 5. Context-Aware Generation

MDDE understands relationships and generates appropriate DAX:

```yaml
# MDDE Definition
entities:
  - name: orders
    attributes:
      - name: customer_segment
        derived: true
        expression: customer.segment  # Cross-table reference
```

Generates:

```dax
// Calculated Column (needs row context)
Customer Segment = RELATED(Customer[Segment])

// Or as Measure (uses filter context)
Customer Segment = SELECTEDVALUE(Customer[Segment])
```

### 6. Quality Constraints → DAX Validation Measures

MDDE quality constraints become validation measures:

```yaml
# MDDE Definition
quality_rules:
  - name: positive_amount
    entity: sales
    expression: amount > 0
    severity: error
```

Generates:

```dax
_DQ_Positive_Amount_Violations =
COUNTROWS(FILTER(Sales, Sales[Amount] <= 0))

_DQ_Positive_Amount_Valid =
IF([_DQ_Positive_Amount_Violations] = 0, "Pass", "Fail")
```

## Full Example

### MDDE Semantic Definition

```yaml
model: retail_analytics
description: "Retail sales analytics model"

entities:
  - name: sales
    physical_name: fact_sales
    attributes:
      - name: amount
        data_type: DECIMAL(18,2)
      - name: quantity
        data_type: INT
      - name: order_date
        data_type: DATE
      - name: customer_id
        data_type: INT
        references: customers.id

  - name: customers
    physical_name: dim_customer
    attributes:
      - name: id
        data_type: INT
        is_key: true
      - name: segment
        data_type: VARCHAR(50)
      - name: region
        data_type: VARCHAR(50)

metrics:
  - name: total_revenue
    type: SIMPLE
    expression: SUM(sales.amount)
    description: "Total sales revenue"
    time_dimension: sales.order_date

  - name: total_orders
    type: SIMPLE
    expression: COUNT(sales.id)
    description: "Number of orders"

  - name: avg_order_value
    type: RATIO
    numerator: total_revenue
    denominator: total_orders
    description: "Average order value"

  - name: revenue_by_segment
    type: SIMPLE
    expression: SUM(sales.amount)
    dimensions: [customers.segment]
    description: "Revenue broken down by customer segment"

hierarchies:
  - name: date_hierarchy
    dimension: order_date
    levels: [year, quarter, month, week, day]
```

### Generated DAX Output

```dax
// =============================================
// MDDE Generated DAX Measures
// Model: retail_analytics
// Generated: 2026-02-28
// =============================================

// -----------------------------------------
// Base Measures
// -----------------------------------------

Total Revenue =
SUM(Sales[Amount])

Total Orders =
COUNT(Sales[Order_ID])

Avg Order Value =
VAR Numerator = [Total Revenue]
VAR Denominator = [Total Orders]
RETURN DIVIDE(Numerator, Denominator)

// -----------------------------------------
// Time Intelligence (auto-generated)
// -----------------------------------------

Total Revenue LY =
CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(Calendar[Date]))

Total Revenue YoY =
[Total Revenue] - [Total Revenue LY]

Total Revenue YoY % =
VAR Current = [Total Revenue]
VAR Previous = [Total Revenue LY]
RETURN DIVIDE(Current - Previous, Previous)

Total Revenue YTD =
TOTALYTD([Total Revenue], Calendar[Date])

Total Revenue MTD =
TOTALMTD([Total Revenue], Calendar[Date])

Total Revenue Rolling 12M =
CALCULATE(
    [Total Revenue],
    DATESINPERIOD(Calendar[Date], MAX(Calendar[Date]), -12, MONTH)
)

// Same pattern for Total Orders...
Total Orders LY =
CALCULATE([Total Orders], SAMEPERIODLASTYEAR(Calendar[Date]))

// -----------------------------------------
// Segment Analysis
// -----------------------------------------

Revenue by Segment =
CALCULATE(
    [Total Revenue],
    ALLEXCEPT(Customer, Customer[Segment])
)

Revenue % of Segment =
VAR SegmentTotal = CALCULATE([Total Revenue], ALLEXCEPT(Customer, Customer[Segment]))
VAR GrandTotal = CALCULATE([Total Revenue], ALL(Customer))
RETURN DIVIDE(SegmentTotal, GrandTotal)

// -----------------------------------------
// Ranking
// -----------------------------------------

Customer Revenue Rank =
RANKX(
    ALL(Customer),
    [Total Revenue],
    ,
    DESC,
    DENSE
)

// -----------------------------------------
// Data Quality Validation
// -----------------------------------------

_DQ_Validation_Summary =
VAR PositiveAmount = IF(COUNTROWS(FILTER(Sales, Sales[Amount] <= 0)) = 0, 1, 0)
VAR ValidDates = IF(COUNTROWS(FILTER(Sales, ISBLANK(Sales[Order_Date]))) = 0, 1, 0)
RETURN
FORMAT(DIVIDE(PositiveAmount + ValidDates, 2), "0%") & " passed"
```

## Benefits

1. **Define Once**: Business logic in MDDE YAML
2. **Generate Everywhere**: SQL for warehouses, DAX for Power BI
3. **No DAX Expertise Required**: Users define business concepts, not syntax
4. **Consistency**: Same logic generates identical results across platforms
5. **Time Intelligence Free**: Auto-generated YoY, YTD, Rolling patterns
6. **Best Practices Built-In**: Generated DAX uses VAR/RETURN, DIVIDE, etc.
7. **Documentation Included**: Comments and descriptions from MDDE definitions

## API

```python
from mdde.dax import DAXGenerator
from mdde.semantic import SemanticModel

# Load semantic model
model = SemanticModel.load("models/retail_analytics.yaml")

# Generate DAX
generator = DAXGenerator()
dax_output = generator.generate_all(model)

# Write to file
dax_output.save("output/retail_analytics.dax")

# Or generate specific measures
revenue_dax = generator.generate_measure(model.get_metric("total_revenue"))
print(revenue_dax)
```

## Directory Structure

```
src/mdde/dax/
├── __init__.py
├── generator/
│   ├── __init__.py
│   ├── dax_generator.py     # Main generator class
│   ├── expression_mapper.py  # MDDE → DAX expression mapping
│   ├── time_intelligence.py  # Auto time intelligence generation
│   ├── context_handler.py    # Filter/row context decisions
│   └── templates.py          # DAX templates by metric type
├── patterns/
│   ├── __init__.py
│   ├── aggregation.py       # SUM, COUNT, etc.
│   ├── time_intel.py        # YoY, YTD, Rolling, etc.
│   ├── ranking.py           # RANKX, TOPN
│   └── ratio.py             # DIVIDE patterns
└── output/
    ├── __init__.py
    ├── dax_file.py          # .dax file writer
    ├── pbix_injector.py     # Inject into Power BI Desktop
    └── tmdl_exporter.py     # TMDL format (Tabular Model)
```

## Integration with Existing MDDE

### Semantic Layer Export

```python
# In src/mdde/semantic/exporter/
class PowerBIExporter:
    """Export semantic model to Power BI."""

    def export(self, model: SemanticModel) -> PowerBIOutput:
        # Generate DAX measures
        dax = DAXGenerator().generate_all(model)

        # Generate relationships
        relationships = self._generate_relationships(model)

        # Generate hierarchies
        hierarchies = self._generate_hierarchies(model)

        return PowerBIOutput(
            measures=dax,
            relationships=relationships,
            hierarchies=hierarchies,
        )
```

### Pipeline Integration

```yaml
# MDDE Pipeline Definition
pipeline:
  name: retail_bi_pipeline
  steps:
    - name: generate_sql
      type: sql_generator
      dialect: snowflake
      output: sql/

    - name: generate_dax
      type: dax_generator
      output: dax/
      options:
        include_time_intelligence: true
        include_data_quality: true
```

## Comparison: Manual DAX vs MDDE Generated

### Manual DAX (typical developer writes)

```dax
Revenue YoY % =
VAR CurrentRev = SUM(Sales[Amount])
VAR LastYearRev = CALCULATE(SUM(Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date]))
RETURN DIVIDE(CurrentRev - LastYearRev, LastYearRev)
```

### MDDE Definition

```yaml
metrics:
  - name: revenue
    expression: SUM(sales.amount)
    time_dimension: order_date
```

**Result**: Same DAX, but also:
- SQL for Snowflake/BigQuery/DuckDB
- Documentation auto-generated
- Data quality checks included
- All time variants (LY, YoY, YTD, MTD, Rolling) free

## References

- ADR-301: Semantic Layer Module
- ADR-338: Power BI to MDDE Converter
- Ultimate DAX Formula Guide by Himansh Upadhyay
- SQLBI DAX Patterns (daxpatterns.com)
