# I Built an Ontology Firewall for Metadata-Driven Data Engineering — Here's the Architecture

**Why AI agents need semantic guardrails before touching your data models**

---

Three weeks into our enterprise data platform migration, an AI agent approved a schema change that dropped a column used by 47 downstream reports.

The agent had analyzed the column's usage patterns correctly. It understood the SQL. It even generated a migration script. What it didn't understand was that `customer_status_cd` was a **Key Data Element** governed by a 30-day deprecation policy — a rule that existed in our data governance documentation but nowhere the agent could access it.

That's not a hallucination problem. That's a **semantic grounding** problem.

---

## The Problem with AI Agents and Data Models

Modern AI agents are remarkably capable at code generation, documentation, and even complex refactoring tasks. But they share a fundamental limitation: they predict what's *likely* based on patterns, not what's *correct* based on business rules.

In metadata-driven data engineering (MDDE), this gap is critical:

- **Entity lifecycle states** determine what changes are allowed
- **Role-based authorization** limits who can make breaking changes
- **Governance policies** enforce deprecation periods, naming conventions, and impact analysis
- **Breaking change detection** protects downstream consumers

None of these constraints exist in the code. They exist in business rules, governance frameworks, and organizational policies — exactly the kind of tacit knowledge that AI agents struggle to access.

---

## The Ontology Firewall Pattern

Inspired by Pankaj Kumar's work on [Ontology Firewalls for Microsoft Copilot](https://github.com/cloudbadal007/copilot-ontology-firewall), I built a semantic validation layer for MDDE that intercepts every model change and validates it against three criteria before allowing execution:

```
Model Change Request
       ↓
┌─────────────────────────────────────────┐
│          ONTOLOGY FIREWALL              │
├─────────────────────────────────────────┤
│  1. Entity Check    → Is this defined?  │
│  2. State Check     → Valid transition? │
│  3. Governance Check → Permitted action?│
└─────────────────────────────────────────┘
       ↓ ALL PASS
   Execute Change
       ↓ ANY FAIL
   Plain-English Rejection
```

The key insight: the firewall doesn't make the AI smarter. It makes AI-assisted changes **safe enough to trust in production**.

---

## Stage 1: Entity Type Validation

The first check confirms the entity type is formally defined in the ontology:

```python
class EntityType(Enum):
    """MDDE entity types (OWL classes)."""

    MODEL = "Model"
    ENTITY = "Entity"
    ATTRIBUTE = "Attribute"
    RELATIONSHIP = "RelationshipDef"

    # Stereotypes
    SOURCE_TABLE = "SourceTable"
    HUB = "Hub"
    LINK = "Link"
    SATELLITE = "Satellite"
    DIMENSION = "Dimension"
    FACT = "Fact"
```

When an AI agent attempts to modify something that doesn't match a known entity type, the firewall blocks it immediately:

```python
result = firewall.validate(
    entity_type="VendorList_Draft_FINAL",  # Not in ontology
    current_state="draft",
    action="approve",
    user_role="modeler",
)
# Result:
# approved: False
# stage: "entity"
# reason: "'VendorList_Draft_FINAL' is not a recognized entity type..."
```

This prevents the classic problem of AI agents acting on incorrectly classified or ambiguously named objects — exactly like the "$1.8M vendor contract" incident from Pankaj's original article.

---

## Stage 2: State Machine Validation

MDDE models follow a lifecycle:

```
DRAFT → PROPOSED → IN_REVIEW → APPROVED → PUBLISHED → DEPRECATED → ARCHIVED
```

Each state allows specific actions:

```python
STATE_MACHINE = {
    ModelState.DRAFT: [
        "edit", "delete", "submit_for_review",
        "add_attribute", "remove_attribute", "modify_attribute"
    ],
    ModelState.PUBLISHED: [
        "deprecate", "create_new_version",
        "add_non_breaking_change", "breaking_change"  # breaking_change requires data_owner
    ],
    ModelState.DEPRECATED: [
        "archive", "restore"
    ],
}
```

An AI agent cannot skip steps. You can't `publish` directly from `draft`. You can't `delete` a `published` entity. The state machine enforces the workflow:

```python
result = firewall.validate(
    entity_type="Entity",
    current_state="published",
    action="remove_attribute",
    user_role="admin",
)
# Result:
# approved: False
# stage: "state"
# reason: "Action 'remove_attribute' is not permitted for Entity
#          in state 'published'. Allowed actions: deprecate,
#          create_new_version, add_non_breaking_change, breaking_change."
```

The rejection message lists valid alternatives — information the AI agent can relay directly to the user.

---

## Stage 3: Role-Based Authorization

Even valid state transitions require appropriate authorization. The firewall implements a role hierarchy with action-specific limits:

```python
ROLE_HIERARCHY = {
    Role.ADMIN: [Role.ADMIN, Role.DATA_OWNER, Role.DATA_STEWARD,
                 Role.APPROVER, Role.REVIEWER, Role.MODELER, Role.VIEWER],
    Role.DATA_OWNER: [Role.DATA_OWNER, Role.APPROVER, Role.REVIEWER,
                      Role.MODELER, Role.VIEWER],
    Role.MODELER: [Role.MODELER, Role.VIEWER],
    Role.VIEWER: [Role.VIEWER],
}

AUTHORIZATION_RULES = {
    "edit": {Role.MODELER: None},  # Unlimited
    "delete": {Role.MODELER: 1, Role.DATA_STEWARD: 10, Role.ADMIN: None},
    "approve": {Role.APPROVER: 50, Role.DATA_OWNER: None},
    "breaking_change": {Role.DATA_OWNER: None},  # Only data owners
}
```

A `modeler` can delete one entity at a time. Bulk deletions require `data_steward` or higher. Breaking changes on published entities require `data_owner` approval:

```python
result = firewall.validate(
    entity_type="Entity",
    current_state="in_review",
    action="approve",
    user_role="modeler",
    affected_count=75,  # More than approver limit of 50
)
# Result:
# approved: False
# stage: "governance"
# reason: "Role 'approver' can only affect up to 50 entities
#          with action 'approve', but 75 entities are affected.
#          This action requires 'data_owner' role for 75 entities."
```

---

## Model Governance: Policy Enforcement

Beyond the three-stage firewall, we implement **OntoGuard-style policy validation** for domain-specific rules:

```python
class ModelGovernance:
    def __init__(self):
        self._policies = {}
        self._register_default_policies()

    def _register_default_policies(self):
        # Breaking changes on published entities
        self.register_policy(GovernancePolicy(
            name="published_breaking_change",
            description="Breaking changes require data owner approval",
            check_fn=self._check_published_breaking_change,
            severity=Severity.CRITICAL,
            applies_to=BREAKING_CHANGE_TYPES,
        ))

        # Naming conventions
        self.register_policy(GovernancePolicy(
            name="naming_convention",
            description="Names must follow snake_case",
            check_fn=self._check_naming_convention,
            severity=Severity.ERROR,
        ))

        # Sensitive data classification
        self.register_policy(GovernancePolicy(
            name="sensitive_data",
            description="PII attributes need classification",
            check_fn=self._check_sensitive_data,
            severity=Severity.WARNING,
        ))

        # Deprecation periods
        self.register_policy(GovernancePolicy(
            name="deprecation_period",
            description="30-day deprecation before removal",
            check_fn=self._check_deprecation_period,
            severity=Severity.CRITICAL,
        ))
```

When an AI agent attempts to remove a published attribute:

```python
result = governance.validate_change(
    change_type=ChangeType.REMOVE_ATTRIBUTE,
    entity_id="customer",
    entity_state="published",
    attribute_name="customer_status_cd",
    downstream_consumers=["report_daily", "api_customers", "dashboard_main"],
)

print(governance.explain(result))
```

Output:
```
Governance validation FAILED

1 violation(s):

  [CRITICAL] published_breaking_change
  Breaking change 'remove_attribute' on published entity 'customer'
  is not allowed. This change affects 3 downstream consumers.
  Action required: Deprecate the entity first, then create a new version.
  Or obtain Data Owner approval for immediate breaking change.
  Affected: report_daily, api_customers, dashboard_main

This is a BREAKING CHANGE affecting 3 consumers.
```

The explanation is human-readable. The AI agent can relay it to the user verbatim.

---

## Discovery Agent: AI-Powered Schema-to-Model Generation

The flip side of governance is **discovery**. When onboarding legacy databases with cryptic naming conventions, we use Claude to infer semantic meaning:

```python
class DiscoveryAgent:
    """AI-powered schema-to-model generation."""

    async def discover_from_ddl(self, ddl_path: str) -> DiscoveryResult:
        # Parse DDL
        tables = self._parse_ddl(ddl_content)

        # Generate semantic mappings (local + Claude)
        mappings = await self._generate_mappings(tables)

        # Export to MDDE YAML
        return DiscoveryResult(
            tables_found=len(tables),
            mappings=mappings,
        )
```

The agent recognizes common legacy patterns:

```python
LEGACY_PATTERNS = {
    "TBL_": "table",      # TBL_CUST_001 → customer
    "DIM_": "dimension",  # DIM_DATE → date_dimension
    "FCT_": "fact",       # FCT_SALES → sales_fact
    "CD_": "code",        # CD_STATUS → status_code
    "AMT_": "amount",     # AMT_TOTAL → total_amount
    "_HDR": "_header",    # ORDER_HDR → order_header
    "_DTL": "_detail",    # ORDER_DTL → order_detail
}
```

For complex cases, it calls Claude with a structured prompt:

```python
prompt = """Analyze this legacy database table and infer business meaning.

Table: TBL_CUST_ACCT_ELIG_001X
Columns: ["CUST_ID", "ACCT_NBR", "ELIG_CD", "EFF_DT", "TERM_DT", "STAT_FLG"]

Return JSON with inferred_name, description, category, and column mappings."""
```

Result:
```json
{
    "inferred_name": "customer_account_eligibility",
    "description": "Tracks customer eligibility status for accounts over time",
    "category": "transaction",
    "stereotype": "satellite",
    "columns": [
        {"original": "CUST_ID", "inferred": "customer_id", "description": "Customer identifier"},
        {"original": "ACCT_NBR", "inferred": "account_number", "description": "Account number"},
        {"original": "ELIG_CD", "inferred": "eligibility_code", "description": "Eligibility status code"},
        {"original": "EFF_DT", "inferred": "effective_date", "description": "Date eligibility begins"},
        {"original": "TERM_DT", "inferred": "termination_date", "description": "Date eligibility ends"},
        {"original": "STAT_FLG", "inferred": "status_flag", "description": "Active/inactive indicator"}
    ]
}
```

This generates MDDE-compliant YAML that passes through the firewall validation:

```yaml
entity:
  id: customer_account_eligibility
  name: customer_account_eligibility
  description: Tracks customer eligibility status for accounts over time
  stereotype: satellite
  model_id: discovered
  tags: []
  _discovery:
    original_name: TBL_CUST_ACCT_ELIG_001X
    category: transaction
    confidence: 0.85
    discovered_at: "2026-03-23T14:30:00"

attributes:
  - id: customer_id
    name: customer_id
    description: Customer identifier
    data_type: integer
    is_key: true
```

---

## The Architecture

Here's how it all fits together:

```
┌─────────────────────────────────────────────────────────────────┐
│                        MDDE GenAI Layer                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │  Discovery  │  │  Semantic   │  │     OWL Ontology        │ │
│  │   Agent     │  │   Mapper    │  │   (mdde.ttl)            │ │
│  │             │  │  (Claude)   │  │                         │ │
│  │ - Schema    │  │             │  │ - Entity types          │ │
│  │   inspect   │► │ - Infer     │► │ - State machines        │ │
│  │ - DDL parse │  │   names     │  │ - Governance rules      │ │
│  │ - Profile   │  │ - Generate  │  │ - Role permissions      │ │
│  └─────────────┘  │   docs      │  └───────────┬─────────────┘ │
│                   └─────────────┘              │               │
│                                                ▼               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   Model Firewall                        │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐   │   │
│  │  │Entity Check │►│State Check  │►│ Governance      │   │   │
│  │  │             │ │             │ │ Check           │   │   │
│  │  │Is this a    │ │Valid state  │ │Breaking change? │   │   │
│  │  │known model? │ │transition?  │ │Approval needed? │   │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                  Policy Enforcement                      │   │
│  │  - Breaking change detection                            │   │
│  │  - Naming convention validation                         │   │
│  │  - Sensitive data classification                        │   │
│  │  - Deprecation period enforcement                       │   │
│  │  - Impact analysis requirements                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Insights

### 1. Ontology as Version-Controlled Business Rules

The OWL ontology isn't just a schema — it's a **versioned document of business rules**. When approval thresholds change, when new roles are added, when deprecation policies evolve, you update the ontology. The firewall inherits the new rules automatically. No code changes required.

### 2. Plain-English Rejections for AI Agents

Every rejection includes a human-readable explanation:

```
"Action 'remove_attribute' is not permitted for Entity in state
'published'. Allowed actions: deprecate, create_new_version,
add_non_breaking_change."
```

The AI agent can relay this directly to the user. No translation needed.

### 3. Fail Fast, Explain Clearly

The three-stage pipeline fails on the first violation. This gives users precise feedback about exactly where the problem occurred:

- **Entity stage**: "This isn't a recognized model type"
- **State stage**: "You can't do this action in this lifecycle state"
- **Governance stage**: "You don't have permission for this action"

### 4. Self-Healing Property

When your business evolves, you update the ontology — not the code. State machines, role hierarchies, and authorization rules are all declarative. The firewall adapts automatically.

---

## The "Thin Wrapper" Crisis

Pankaj Kumar identifies a pattern I've seen repeatedly: the **thin wrapper** approach that most AI integrations use today:

```python
# Dangerous: No semantic validation
def process_model_change(data):
    prompt = f"Analyze this model and suggest changes: {data}"
    response = llm.complete(prompt)
    database.execute(response.action)  # Direct execution!
```

The problem: The LLM might generate changes that confuse entity types, violate lifecycle states, or ignore business rules. The output will look correct — fluent language, clean formatting — but the underlying logic is wrong.

This is exactly what happened with `customer_status_cd`. The agent understood SQL. It generated a valid migration. But it didn't understand that this was a **Key Data Element** governed by deprecation policy.

The Ontology Firewall transforms this into:

```python
# Safe: Semantic validation before execution
def process_model_change(data):
    proposed_action = llm.analyze(data)

    # Validate against ontology BEFORE execution
    result = firewall.validate(
        entity_type=proposed_action.entity_type,
        current_state=proposed_action.state,
        action=proposed_action.action,
        user_role=current_user.role,
    )

    if not result.approved:
        return f"Blocked: {result.reason}"

    database.execute(proposed_action)
```

**Retrieval grounds facts. Ontology grounds actions.**

---

## What's Next: OntologyOps

The firewall is the first layer. Production deployments reveal additional challenges:

### Schema Drift Detection

What happens when a DBA renames a column and your ontology is now out of sync?

```bash
$ mdde ai governance schema-drift --connection $PROD_DB

Schema Drift Report:
  [ERROR] Missing column: customer.customer_status_cd
    Ontology expects this column but database doesn't have it

  [WARNING] Type mismatch: order.created_at
    Ontology: timestamp
    Database: string
```

### Breaking Change Detection

Before merging a PR that modifies entity definitions:

```bash
$ mdde ai governance breaking-changes --base main

Breaking Changes Detected:
  [CRITICAL] Removed attribute: customer.customer_status_cd
    Impact: 3 downstream consumers (report_daily, api_v2, dashboard)
    Action: Deprecate first, wait 30 days, then remove
```

### Semantic Versioning

Track changes at the entity level, not line level:

```bash
$ mdde ai governance diff v1.0.0 v1.1.0

Changes:
  [+] Added entity: subscription
  [~] Modified: customer.loyalty_tier (new attribute)
  [-] Removed: legacy_order (deprecated 45 days ago)
```

These capabilities are planned for ADR-410: OntologyOps.

---

## What This Means for AI-Assisted Data Engineering

If you're using AI agents for data modeling, schema evolution, or metadata management, ask yourself:

**Do your agents understand:**
- What lifecycle state each model is in?
- What actions are valid from that state?
- Who is authorized to make breaking changes?
- Which attributes are governed by deprecation policies?
- What downstream consumers will be affected?

If the answer is "no" to any of these, you have a semantic grounding gap. You're one bad agent action away from the scenario I described at the start.

The Ontology Firewall pattern closes that gap. It doesn't make AI agents smarter. It makes them **safe enough to trust** with your production metadata.

---

## CI/CD Integration

The real power of the Ontology Firewall emerges when integrated into your CI/CD pipeline. Every PR that modifies entity definitions gets validated automatically:

```python
from mdde.genai.governance import GovernanceIntegration, create_github_check_output

# Initialize with the PR author's role
integration = GovernanceIntegration(user_role="modeler")

# Validate PR changes
result = integration.validate_pr_changes(
    entities_added=["new_customer_segment"],
    entities_modified=["customer"],
    entities_removed=["legacy_order"],
    entity_states={
        "customer": "published",
        "legacy_order": "deprecated",
    },
)

# Generate GitHub Actions output
if not result.can_merge:
    output = create_github_check_output(result)
    print(output["text"])
```

Output when blocking a PR:

```markdown
## Governance Check: Blocked

### Structural Analysis
- Level: `breaking`
- Breaking changes: 1

**Breaking Changes:**
- `customer`: Entity 'customer' modification requires approval

### Governance Violations
- **state**: Action 'edit' on published entity requires data_owner approval.
```

### GitHub Actions Integration

Add governance checks to your workflow:

```yaml
# .github/workflows/governance.yml
name: Model Governance

on:
  pull_request:
    paths:
      - 'models/**/*.yaml'

jobs:
  governance-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Detect changed entities
        id: changes
        run: |
          echo "added=$(git diff --name-only --diff-filter=A origin/main | grep models/)" >> $GITHUB_OUTPUT
          echo "modified=$(git diff --name-only --diff-filter=M origin/main | grep models/)" >> $GITHUB_OUTPUT
          echo "removed=$(git diff --name-only --diff-filter=D origin/main | grep models/)" >> $GITHUB_OUTPUT

      - name: Run governance check
        run: |
          mdde governance pr-check \
            --added ${{ steps.changes.outputs.added }} \
            --modified ${{ steps.changes.outputs.modified }} \
            --removed ${{ steps.changes.outputs.removed }} \
            --states "$(cat .governance/entity-states.json)" \
            --github
```

The check produces three possible outcomes:

| Conclusion | Meaning |
|------------|---------|
| `success` | All governance rules passed |
| `neutral` | Requires approval from specific role |
| `failure` | Blocked by governance policy |

### Combining with Breaking Change Analysis

The governance integration wraps the existing `BreakingChangeAnalyzer`:

```python
from mdde.genai.governance import GovernanceIntegration

integration = GovernanceIntegration(user_role="modeler")

# Pass structural analysis to governance
result = integration.check_changes(
    breaking_result=breaking_analyzer.analyze(old_model, new_model),
    entity_states={"customer": "published"},
    downstream_consumers={"customer": ["report_daily", "api_v2"]},
)

# Combined result includes both structural and governance analysis
print(f"Structural level: {result.structural_level}")
print(f"Governance approved: {result.governance_approved}")
print(f"Can merge: {result.can_merge}")
print(f"Required role: {result.required_role}")
```

This ensures that:
1. **Structural changes** (column removals, type changes) are detected
2. **Governance rules** (lifecycle states, role authorization) are enforced
3. **Policy violations** (deprecation periods, naming conventions) are flagged

All in a single CI check.

---

## CLI Integration

The firewall is also available via command line:

```bash
# Validate a model change
$ mdde ai governance validate --action remove_attribute --state published --role modeler

Firewall Validation: BLOCKED
Stage: 2. State Check

Reason: Action 'remove_attribute' is not permitted for Entity in state
'published'. Allowed actions: deprecate, create_new_version,
add_non_breaking_change, breaking_change.

# Check allowed actions for a role and state
$ mdde ai governance check --state draft --role modeler

Allowed actions for Entity
State: draft
Role: modeler

Actions: add_attribute, delete, edit, modify_attribute, remove_attribute, submit_for_review

# List governance policies
$ mdde ai governance policies

Governance Policies
============================================================

published_breaking_change [critical] (enabled)
  Breaking changes on published entities require data owner approval

naming_convention [error] (enabled)
  Entity and attribute names must follow naming conventions

sensitive_data [warning] (enabled)
  Sensitive data attributes require classification and masking rules

deprecation_period [critical] (enabled)
  Published entities must be deprecated before removal

# Discover entities from DDL
$ mdde ai governance discover schema.sql --output models/

Discovery complete!
Tables found: 15
Relationships: 8
Duration: 1.23s

Generated 15 YAML files in models/

# Validate PR changes (for CI/CD)
$ mdde governance pr-check --removed customer --states '{"customer": "published"}' --github

{
  "conclusion": "failure",
  "title": "Governance: Blocked (1 violations)",
  "summary": "**Structural Level**: none\n**Governance Approved**: False",
  "text": "## Governance Check: Blocked\n\n### Governance Violations\n- **state**: Action 'delete' is not permitted for Entity in state 'published'."
}

# Same check with JSON output
$ mdde governance pr-check --added new_entity --modified customer --json

{
  "can_merge": true,
  "requires_approval": false,
  "structural": {"level": "none", "breaking_count": 0},
  "governance": {"approved": true, "stage": "all_passed", "violations": []},
  "policy": {"valid": true, "violations": 0, "warnings": 0}
}
```

---

## Try It Yourself

The implementation is available in MDDE's GenAI governance module:

```python
from mdde.genai.governance import (
    OntologyFirewall,
    ModelGovernance,
    DiscoveryAgent,
    GovernanceIntegration,
    create_github_check_output,
)

# Create firewall
firewall = OntologyFirewall()

# Validate a change
result = firewall.validate(
    entity_type="Entity",
    current_state="published",
    action="remove_attribute",
    user_role="modeler",
    affected_count=5,
)

if not result.approved:
    print(result.reason)
    print(f"Allowed actions: {result.allowed_actions}")

# For CI/CD pipelines
integration = GovernanceIntegration(user_role="modeler")
pr_result = integration.validate_pr_changes(
    entities_added=["new_entity"],
    entities_removed=["old_entity"],
    entity_states={"old_entity": "deprecated"},
)
print(f"Can merge: {pr_result.can_merge}")
```

The pattern is generalizable beyond MDDE. If you're building AI agents that touch enterprise systems — contracts, approvals, configurations, deployments — consider adding an ontology layer between the agent and the execution.

**Retrieval grounds facts. Ontology grounds actions.**

---

## Credits

This implementation was inspired by Pankaj Kumar's excellent work on ontology-driven AI agents. His [Medium series](https://medium.com/@pankajkumar_74471) and [open-source implementations](https://github.com/cloudbadal007) are essential reading:

### Key Articles

- **[Why Every AI Agent Needs an Ontology](https://medium.com/@pankajkumar_74471)** - The foundational case for semantic grounding. Ontologies solve trust (explainability), interoperability (agents talking), and scalability (composable capabilities).

- **[OntoGuard: Ontology Firewall in 48 Hours](https://medium.com/@pankajkumar_74471)** - The $4.6M refund mistake that inspired semantic validation. A database column rename caused 2,300 incorrect refunds in 90 seconds.

- **[The Ontology Firewall: Why Enterprise AI Agents Are Failing](https://medium.com/@pankajkumar_74471)** - Deep dive into the "thin wrapper crisis" and why probabilistic AI breaks in production.

- **[OntologyOps: Versioning, Testing, and Deployment](https://medium.com/@pankajkumar_74471)** - The discipline of managing ontologies as production infrastructure. Version control, breaking change detection, and CI/CD.

### Open Source

- [copilot-ontology-firewall](https://github.com/cloudbadal007/copilot-ontology-firewall) - Microsoft Copilot integration
- [ontologyops](https://github.com/cloudbadal007/ontologyops) - Version control for ontologies
- [ontology-mcp-self-healing](https://github.com/cloudbadal007/ontology-mcp-self-healing) - Schema drift detection
- [legacy-to-logic](https://github.com/cloudbadal007/legacy-to-logic) - Schema-to-ontology generation
- [ontoguard-ai](https://github.com/cloudbadal007/ontoguard-ai) - OntoGuard implementation

### The Key Insight

Pankaj's framing crystallizes the architecture:

> "Retrieval grounds facts. Ontology grounds actions."

RAG tells you what's true. Ontology tells you what's allowed. Both are necessary for production AI.

---

*Generated with [Claude Code](https://claude.ai/code)*
