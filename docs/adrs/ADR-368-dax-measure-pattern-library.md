# ADR-368: DAX Measure Pattern Library

## Status
Proposed

## Context

Power BI DAX measures are critical analytics artifacts that need proper documentation, categorization, and quality analysis. Inspired by Himansh Upadhyay's "Ultimate DAX Formula Guide", this ADR proposes a comprehensive DAX measure management system for MDDE.

The guide demonstrates that effective DAX documentation requires:
1. **Business Requirement** - Why the measure exists
2. **Structure** - The formula pattern
3. **Step-by-step Logic** - How it works
4. **Visual Behaviour** - Expected output
5. **When to Use / When NOT to Use** - Guidance
6. **Performance Considerations** - Optimization
7. **Common Mistakes** - Anti-patterns
8. **Context Interaction** - Filter vs Row context

## Decision

Implement a DAX Measure Pattern Library with four components:

### 1. Measure Documentation (`src/mdde/dax/documentation/`)

Standardized measure documentation following the guide's pattern:

```python
@dataclass
class MeasureDocumentation:
    """Comprehensive measure documentation."""
    name: str
    business_requirement: str  # Why - business context
    formula: str               # The DAX formula
    structure: str             # Pattern structure explanation
    step_by_step: List[str]    # Logic breakdown
    visual_behaviour: str      # Expected output/usage
    when_to_use: List[str]     # Appropriate scenarios
    when_not_to_use: List[str] # Anti-patterns
    performance_notes: List[str]
    common_mistakes: List[str]
    context_type: ContextType  # FILTER, ROW, MIXED
    category: MeasureCategory  # Aggregation, Time Intelligence, etc.
    dependencies: List[str]    # Referenced measures/columns
    examples: List[MeasureExample]
```

### 2. Pattern Recognition (`src/mdde/dax/patterns/`)

Analyze DAX formulas to identify patterns:

```python
class DAXPatternAnalyzer:
    """Analyze DAX for known patterns and anti-patterns."""

    def analyze(self, dax: str) -> PatternAnalysis:
        """Identify patterns used in DAX formula."""

    def detect_anti_patterns(self, dax: str) -> List[AntiPattern]:
        """Find common mistakes and anti-patterns."""

    def suggest_improvements(self, dax: str) -> List[Improvement]:
        """Suggest optimizations based on patterns found."""
```

Pattern categories (from the guide):
- **Aggregation**: SUM, AVERAGE, COUNT, COUNTROWS, DISTINCTCOUNT
- **Iterator (X-Functions)**: SUMX, AVERAGEX, MINX, MAXX, COUNTX
- **Filter & Context**: CALCULATE, FILTER, ALL, ALLEXCEPT, ALLSELECTED
- **Logical**: IF, SWITCH, AND, OR, NOT, COALESCE, IFERROR
- **Mathematical**: DIVIDE, ABS, ROUND, CEILING, FLOOR
- **Time Intelligence**: DATEADD, SAMEPERIODLASTYEAR, TOTALYTD, etc.
- **Table Functions**: SUMMARIZE, ADDCOLUMNS, SELECTCOLUMNS, GENERATE
- **Ranking/Window**: RANKX, TOPN, PERCENTILE
- **Information**: ISBLANK, ISNUMBER, ISERROR, HASONEVALUE

### 3. Context Analyzer (`src/mdde/dax/context/`)

Understand filter context vs row context:

```python
class DAXContextAnalyzer:
    """Analyze context usage in DAX formulas."""

    def analyze_context(self, dax: str) -> ContextAnalysis:
        """Determine filter/row context usage."""

    def detect_context_transition(self, dax: str) -> List[ContextTransition]:
        """Find CALCULATE-triggered context transitions."""

    def validate_context_usage(self, dax: str) -> List[ContextWarning]:
        """Warn about potential context issues."""
```

Key context concepts:
- **Filter Context**: Applied by slicers, filters, visual context
- **Row Context**: Created by iterators (SUMX, ADDCOLUMNS, etc.)
- **Context Transition**: CALCULATE converts row → filter context

### 4. Performance Analyzer (`src/mdde/dax/performance/`)

Detect performance issues:

```python
class DAXPerformanceAnalyzer:
    """Analyze DAX for performance issues."""

    SLOW_PATTERNS = {
        "nested_iterators": "Nested SUMX/AVERAGEX can be O(n²)",
        "filter_all_table": "FILTER(table, ...) instead of FILTER(ALL(column), ...)",
        "calculate_in_calculated_column": "CALCULATE in calc columns is often unnecessary",
        "format_in_iterator": "FORMAT() inside iterators is expensive",
        "excessive_all": "ALL() on large tables is expensive",
        "no_variables": "Repeated expressions should use VAR/RETURN",
    }

    def analyze_performance(self, dax: str) -> PerformanceAnalysis:
        """Detect performance issues and suggest fixes."""
```

Performance rules from the guide:
- SUM faster than SUMX (use simple aggregation when possible)
- SUMMARIZECOLUMNS better than SUMMARIZE
- VAR/RETURN to avoid recalculating expressions
- KEEPFILTERS instead of removing filters with ALL
- TOPN with pre-aggregated data
- Avoid EARLIER on large tables (use variables)

## Implementation

### Types (`src/mdde/dax/types.py`)

```python
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Dict

class MeasureCategory(Enum):
    """Categories of DAX measures (from Ultimate DAX Guide)."""
    AGGREGATION = "aggregation"
    ITERATOR = "iterator"
    FILTER_CONTEXT = "filter_context"
    LOGICAL = "logical"
    MATHEMATICAL = "mathematical"
    STATISTICAL = "statistical"
    TIME_INTELLIGENCE = "time_intelligence"
    RELATIONSHIP = "relationship"
    TABLE_CONSTRUCTION = "table_construction"
    TEXT = "text"
    INFORMATION = "information"
    ERROR_HANDLING = "error_handling"
    RANKING_WINDOW = "ranking_window"
    PERFORMANCE = "performance"
    SECURITY_RLS = "security_rls"
    DYNAMIC_MEASURE = "dynamic_measure"
    DEBUGGING = "debugging"

class ContextType(Enum):
    """DAX evaluation context type."""
    FILTER = "filter"       # Filter context only
    ROW = "row"             # Row context only
    MIXED = "mixed"         # Both contexts
    TRANSITION = "transition"  # Context transition (CALCULATE)

class PerformanceSeverity(Enum):
    """Performance issue severity."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

@dataclass
class PatternMatch:
    """A pattern found in DAX formula."""
    pattern_name: str
    category: MeasureCategory
    location: tuple[int, int]  # start, end position
    description: str
    is_anti_pattern: bool = False

@dataclass
class PerformanceIssue:
    """A performance issue detected."""
    issue_type: str
    severity: PerformanceSeverity
    description: str
    suggestion: str
    location: Optional[tuple[int, int]] = None

@dataclass
class ContextTransition:
    """A context transition point."""
    location: int
    from_context: ContextType
    to_context: ContextType
    trigger: str  # Usually "CALCULATE"

@dataclass
class MeasureExample:
    """Example usage of a measure."""
    scenario: str
    visual_type: str  # "card", "table", "chart", etc.
    expected_output: str
```

### Pattern Library

Pre-built patterns based on the guide:

```python
MEASURE_PATTERNS = {
    # Aggregation Patterns
    "simple_sum": {
        "pattern": r"\bSUM\s*\(\s*\w+\[\w+\]\s*\)",
        "category": MeasureCategory.AGGREGATION,
        "description": "Simple column sum",
        "performance": "Fast - use when possible",
    },
    "iterator_sum": {
        "pattern": r"\bSUMX\s*\(",
        "category": MeasureCategory.ITERATOR,
        "description": "Row-by-row sum with expression",
        "performance": "Slower - use only when calculation needed per row",
    },

    # Context Patterns
    "calculate_basic": {
        "pattern": r"\bCALCULATE\s*\(",
        "category": MeasureCategory.FILTER_CONTEXT,
        "description": "Modify filter context",
        "context": ContextType.TRANSITION,
    },
    "all_table": {
        "pattern": r"\bALL\s*\(\s*\w+\s*\)",
        "category": MeasureCategory.FILTER_CONTEXT,
        "description": "Remove all filters from table",
        "performance": "Can be expensive on large tables",
    },

    # Time Intelligence
    "sameperiodlastyear": {
        "pattern": r"\bSAMEPERIODLASTYEAR\s*\(",
        "category": MeasureCategory.TIME_INTELLIGENCE,
        "description": "Compare to same period last year",
    },
    "totalytd": {
        "pattern": r"\bTOTALYTD\s*\(",
        "category": MeasureCategory.TIME_INTELLIGENCE,
        "description": "Year-to-date total",
    },
}

ANTI_PATTERNS = {
    "nested_if": {
        "pattern": r"\bIF\s*\([^)]*\bIF\s*\(",
        "description": "Nested IF - use SWITCH instead",
        "suggestion": "Replace with SWITCH(TRUE(), condition1, result1, ...)",
    },
    "filter_all_table": {
        "pattern": r"\bFILTER\s*\(\s*\w+\s*,",  # FILTER(Table, ...) vs FILTER(ALL(Column), ...)
        "description": "FILTER on entire table is slow",
        "suggestion": "Use FILTER(ALL(Column), ...) or direct column filter",
    },
    "calculate_in_calccolumn": {
        "pattern": r"=\s*CALCULATE\s*\(",  # Starting with CALCULATE suggests calc column
        "description": "CALCULATE in calculated column often unnecessary",
        "suggestion": "Consider if row context already provides correct values",
    },
}
```

## Directory Structure

```
src/mdde/dax/
├── __init__.py
├── types.py                 # Core types and enums
├── documentation/
│   ├── __init__.py
│   ├── measure_doc.py       # MeasureDocumentation class
│   ├── doc_generator.py     # Generate docs from measures
│   └── templates/           # Documentation templates
├── patterns/
│   ├── __init__.py
│   ├── analyzer.py          # DAXPatternAnalyzer
│   ├── library.py           # Built-in pattern definitions
│   └── matcher.py           # Pattern matching logic
├── context/
│   ├── __init__.py
│   ├── analyzer.py          # DAXContextAnalyzer
│   └── transition.py        # Context transition detection
├── performance/
│   ├── __init__.py
│   ├── analyzer.py          # DAXPerformanceAnalyzer
│   └── rules.py             # Performance rules
└── exporter/
    ├── __init__.py
    ├── markdown.py          # Export to Markdown (like the guide)
    └── json.py              # Export to JSON
```

## Integration with Existing MDDE

### Power BI Importer Enhancement

```python
# In src/mdde/importer/powerbi/mdde_converter.py
def import_with_measure_analysis(pbix_path: str) -> ImportResult:
    """Import Power BI model with DAX analysis."""
    # Existing import logic...

    # NEW: Analyze all measures
    for measure in model.measures:
        analysis = DAXPatternAnalyzer().analyze(measure.expression)
        context = DAXContextAnalyzer().analyze_context(measure.expression)
        perf = DAXPerformanceAnalyzer().analyze_performance(measure.expression)

        # Attach analysis to measure metadata
        measure.metadata["patterns"] = analysis.patterns
        measure.metadata["context_type"] = context.primary_context
        measure.metadata["performance_issues"] = perf.issues
```

### Semantic Layer Integration

```python
# In src/mdde/semantic/model.py
class SemanticMetric:
    """Enhanced with DAX documentation."""

    def generate_documentation(self) -> MeasureDocumentation:
        """Auto-generate documentation from metric definition."""
        analyzer = DAXPatternAnalyzer()
        analysis = analyzer.analyze(self.expression)

        return MeasureDocumentation(
            name=self.name,
            business_requirement=self.description or "TODO: Add business requirement",
            formula=self.expression,
            category=analysis.primary_category,
            context_type=DAXContextAnalyzer().analyze_context(self.expression).primary_context,
            # ... auto-populate where possible
        )
```

## Example Output

Given a measure:
```dax
Revenue YoY % =
VAR CurrentRevenue = SUM(Sales[Revenue])
VAR LastYearRevenue = CALCULATE(SUM(Sales[Revenue]), SAMEPERIODLASTYEAR(Calendar[Date]))
RETURN DIVIDE(CurrentRevenue - LastYearRevenue, LastYearRevenue)
```

Analysis output:
```yaml
name: Revenue YoY %
category: time_intelligence
context_type: transition

patterns_found:
  - name: var_return
    category: performance
    description: Uses variables for intermediate calculations
  - name: calculate_basic
    category: filter_context
    description: Modifies filter context
  - name: sameperiodlastyear
    category: time_intelligence
    description: Year-over-year comparison
  - name: divide_safe
    category: mathematical
    description: Safe division (handles divide by zero)

performance_analysis:
  score: 95/100
  notes:
    - "Good: Uses VAR/RETURN for performance"
    - "Good: Uses DIVIDE for safe division"
    - "Good: Time intelligence function optimized"

context_analysis:
  primary_context: filter
  transitions:
    - location: 65
      from: filter
      to: filter
      trigger: CALCULATE
      note: "CALCULATE with SAMEPERIODLASTYEAR shifts date filter"

auto_documentation:
  business_requirement: "Calculate year-over-year revenue growth percentage"
  step_by_step:
    - "Calculate current revenue using filter context"
    - "Calculate last year revenue by shifting date filter"
    - "Compute percentage change safely"
  visual_behaviour: "Shows % change vs same period last year"
  when_to_use:
    - "Year-over-year comparisons"
    - "Growth analysis dashboards"
  performance_notes:
    - "Variables prevent double calculation of SUM"
    - "DIVIDE handles edge cases automatically"
```

## Benefits

1. **Standardized Documentation**: Same quality as the Ultimate DAX Guide
2. **Auto-Analysis**: Detect patterns and issues automatically
3. **Performance Optimization**: Catch slow patterns before deployment
4. **Learning Tool**: Explain DAX behavior to new team members
5. **Governance**: Ensure measures follow best practices
6. **Integration**: Works with existing Power BI import and semantic layer

## References

- Ultimate DAX Formula Guide by Himansh Upadhyay
- MDDE Semantic Layer (ADR-301)
- Power BI to MDDE Converter (ADR-338)
- SQLBI DAX Guide (https://dax.guide)
