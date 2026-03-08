# Phase 6: Alert Infrastructure - Research

**Researched:** 2026-03-08
**Domain:** Alert formatting, delivery failure handling, YAML-driven configuration, circuit breaker
**Confidence:** HIGH

## Summary

Phase 6 builds the shared alert infrastructure layer that Phase 7 (Alert Channels: Teams, Email) will consume. The scope is strictly the formatting engine (Jinja2 templates), YAML configuration parsing for an `alerts:` section, failure handling policies for the existing `_send_alert` method, and a circuit breaker to stop retrying dead channels.

The existing codebase already has a stub `_send_alert` in `fabric_connector.py` (lines 531-582) that returns `bool` but the caller on line 529 ignores the return value entirely -- this is the bug ALRT-06 addresses. The current retry logic is a simple exponential backoff loop without any circuit-breaking capability. The YAML configs currently have no `alerts:` section -- they only contain `validation_name`, `expectations`, `data_source`, and `quality_thresholds`.

**Primary recommendation:** Build a standalone `dq_framework/alerting.py` module with an `AlertFormatter` (Jinja2), an `AlertConfig` (YAML parsing), a `CircuitBreaker` state machine, and an `AlertDispatcher` base class with failure policy support. Keep it decoupled from channel implementations (Teams/Email come in Phase 7).

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| ALRT-03 | Implement alert message formatting layer shared by Teams and email channels (Jinja2 templates) | Jinja2 templating patterns, template structure for validation summaries |
| ALRT-05 | Add YAML-driven alert configuration (alerts: section in existing config YAML) | YAML schema design, ConfigLoader extension patterns |
| ALRT-06 | Fix existing _send_alert return value handling (caller ignores failures -- implement failure policies) | Current code analysis of fabric_connector.py _send_alert/handle_validation_failure |
| ALRT-07 | Implement alert retry with circuit breaker (stop retrying after N consecutive failures) | Circuit breaker pattern (closed/open/half-open states) |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| jinja2 | >=3.1.0 | Template rendering for alert messages | Already a transitive dependency via Great Expectations; de facto Python templating standard |
| pyyaml | >=6.0 | YAML config parsing for alerts section | Already a direct dependency in pyproject.toml |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| dataclasses | stdlib | Structured config objects (AlertConfig, ChannelConfig) | Always -- type-safe config over raw dicts |
| enum | stdlib | Failure policy enum (WARN, RAISE, FALLBACK), circuit breaker states | Always -- explicit state machine |
| logging | stdlib | Alert delivery logging | Always |
| time | stdlib | Circuit breaker cooldown tracking | Always |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Hand-rolled circuit breaker | pybreaker or circuitbreaker PyPI packages | Overkill for single-purpose alert delivery; adds external dependency for ~50 lines of code |
| Jinja2 templates | f-strings or string.Template | f-strings cannot be stored externally or customized by users; Jinja2 supports loops, conditionals, filters needed for variable-length failure lists |
| dataclasses for config | Pydantic | Pydantic adds a dependency; dataclasses are stdlib and sufficient for config validation |

**Installation:**
```bash
# No new dependencies needed -- jinja2 is already available via great-expectations
# pyyaml is already a direct dependency
pip install -e ".[dev]"
```

## Architecture Patterns

### Recommended Project Structure
```
dq_framework/
    alerting/
        __init__.py          # Public API: AlertFormatter, AlertConfig, AlertDispatcher, CircuitBreaker
        formatter.py         # Jinja2 template rendering (ALRT-03)
        config.py            # YAML alerts: section parsing + dataclasses (ALRT-05)
        dispatcher.py        # Base dispatcher with failure policies (ALRT-06)
        circuit_breaker.py   # Circuit breaker state machine (ALRT-07)
        templates/           # Default Jinja2 templates shipped with package
            summary.txt.j2   # Plain-text summary template
            summary.html.j2  # HTML summary template (for email in Phase 7)
    ...existing modules...
```

**Alternative (simpler):** Single `dq_framework/alerting.py` file if total code stays under ~300 lines. The subpackage approach is preferred because Phase 7 will add `channels/teams.py` and `channels/email.py` under the same namespace.

### Pattern 1: AlertFormatter (Jinja2 Templates)
**What:** Renders validation results into human-readable alert messages using Jinja2 templates.
**When to use:** Every alert delivery, regardless of channel.
**Example:**
```python
from dataclasses import dataclass
from jinja2 import Environment, PackageLoader, select_autoescape

class AlertFormatter:
    """Renders validation results into alert messages using Jinja2 templates."""

    def __init__(self, template_dir: str | None = None):
        if template_dir:
            from jinja2 import FileSystemLoader
            self._env = Environment(
                loader=FileSystemLoader(template_dir),
                autoescape=select_autoescape(["html"]),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        else:
            self._env = Environment(
                loader=PackageLoader("dq_framework.alerting", "templates"),
                autoescape=select_autoescape(["html"]),
                trim_blocks=True,
                lstrip_blocks=True,
            )

    def render(self, template_name: str, results: dict) -> str:
        """Render a validation result dict into a formatted message."""
        template = self._env.get_template(template_name)
        return template.render(
            suite_name=results.get("suite_name", "unknown"),
            batch_name=results.get("batch_name", "unknown"),
            success=results.get("success", False),
            success_rate=results.get("success_rate", 0),
            evaluated_checks=results.get("evaluated_checks", 0),
            failed_checks=results.get("failed_checks", 0),
            failed_expectations=results.get("failed_expectations", []),
            severity_stats=results.get("severity_stats", {}),
            threshold_failures=results.get("threshold_failures", []),
            timestamp=results.get("timestamp", ""),
        )
```

### Pattern 2: CircuitBreaker State Machine
**What:** Tracks consecutive failures per channel and stops retrying when a threshold is exceeded.
**When to use:** Wraps every alert delivery call.
**Example:**
```python
import time
from enum import Enum

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failures exceeded threshold, rejecting calls
    HALF_OPEN = "half_open"  # Cooldown elapsed, allowing one test call

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, cooldown_seconds: float = 300.0):
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._failure_count = 0
        self._state = CircuitState.CLOSED
        self._last_failure_time: float | None = None

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN and self._last_failure_time:
            if time.monotonic() - self._last_failure_time >= self._cooldown_seconds:
                self._state = CircuitState.HALF_OPEN
        return self._state

    def allow_request(self) -> bool:
        return self.state != CircuitState.OPEN

    def record_success(self) -> None:
        self._failure_count = 0
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._failure_count >= self._failure_threshold:
            self._state = CircuitState.OPEN
```

### Pattern 3: Failure Policies (ALRT-06)
**What:** Explicit enum-driven handling of alert delivery failures instead of silent ignoring.
**When to use:** In the dispatcher when `_send_alert` (or its replacement) returns failure.
**Example:**
```python
from enum import Enum

class FailurePolicy(Enum):
    WARN = "warn"        # Log warning and continue pipeline
    RAISE = "raise"      # Raise AlertDeliveryError to halt pipeline
    FALLBACK = "fallback"  # Try next configured channel

class AlertDeliveryError(Exception):
    """Raised when alert delivery fails and policy is RAISE."""
    pass
```

### Pattern 4: YAML Alert Configuration (ALRT-05)
**What:** An `alerts:` section in existing YAML config files with channel routing and policies.
**Example YAML:**
```yaml
validation_name: causeway_financial_validation
expectations:
  - ...

# New alerts section (ALRT-05)
alerts:
  enabled: true
  failure_policy: warn          # warn | raise | fallback
  channels:
    - type: teams
      webhook_url: "${TEAMS_WEBHOOK_URL}"  # env var substitution
      enabled: true
    - type: email
      smtp_host: smtp.office365.com
      smtp_port: 587
      from_addr: dq-alerts@hs2.org.uk
      to_addrs:
        - data-team@hs2.org.uk
      enabled: true
  circuit_breaker:
    failure_threshold: 5
    cooldown_seconds: 300
  templates:
    summary: summary.txt.j2     # override default template
```

**Dataclass mapping:**
```python
from dataclasses import dataclass, field

@dataclass
class ChannelConfig:
    type: str                # "teams" | "email"
    enabled: bool = True
    # Channel-specific fields stored as extra kwargs
    settings: dict = field(default_factory=dict)

@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    cooldown_seconds: float = 300.0

@dataclass
class AlertConfig:
    enabled: bool = True
    failure_policy: str = "warn"
    channels: list[ChannelConfig] = field(default_factory=list)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    templates: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict | None) -> "AlertConfig":
        """Parse the alerts: section from YAML config."""
        if not data:
            return cls(enabled=False)
        # ... parsing logic
```

### Anti-Patterns to Avoid
- **Silent failure swallowing:** The current `_send_alert` returns `False` but the caller on line 529 of `fabric_connector.py` does `self._send_alert(results)` without checking the return. Never ignore delivery status.
- **Hardcoding channel logic in the runner:** Alert formatting and dispatch must be in the alerting module, not embedded in `FabricDataQualityRunner`. The runner should call `dispatcher.send(results)`.
- **Retry without circuit breaking:** The current stub retries 3 times with exponential backoff but never stops trying across subsequent validation runs. A dead webhook will add latency to every run.
- **Storing secrets in YAML:** Webhook URLs and SMTP passwords must come from environment variables, not config files. Use `${ENV_VAR}` substitution or keep secrets out entirely.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Template rendering | Custom string formatting with f-strings | Jinja2 `Environment` + `.j2` template files | Variable-length failure lists, conditional severity sections, HTML vs text output |
| YAML parsing | Manual dict key checking | dataclass `from_dict` with validation | Type safety, default values, clear error messages |
| Env var substitution in YAML | Custom regex replacer | `os.path.expandvars()` or simple `${VAR}` replacement | Standard pattern, handles missing vars |

**Key insight:** The circuit breaker IS worth hand-rolling here (50 lines, no external dependency needed for 3 states and a counter). Jinja2 templates are NOT worth hand-rolling (loops, conditionals, escaping, file loading).

## Common Pitfalls

### Pitfall 1: Jinja2 Not Installed Separately
**What goes wrong:** Developer assumes Jinja2 needs adding to pyproject.toml dependencies.
**Why it happens:** Jinja2 is not a direct dependency but comes via `great-expectations`.
**How to avoid:** Verify import works. Consider adding `jinja2>=3.1.0` as an explicit dependency in pyproject.toml anyway for clarity (it won't add bloat since GX pins it).
**Warning signs:** Import errors in environments where GX is installed differently.

### Pitfall 2: Circuit Breaker State Not Persisted
**What goes wrong:** Circuit breaker resets every time the runner is instantiated.
**Why it happens:** State is in-memory only.
**How to avoid:** This is actually fine for the use case. Fabric notebook cells re-run the full pipeline. Persistent state would require file/database storage which is overkill. Document that circuit breaker state is per-process.
**Warning signs:** N/A -- this is the correct behavior for batch pipelines.

### Pitfall 3: Template Loading in Packaged vs Editable Installs
**What goes wrong:** `PackageLoader` fails to find templates in editable (`pip install -e .`) installs.
**Why it happens:** Package data resolution differs between install modes.
**How to avoid:** Include `templates/` in `package_data` in pyproject.toml. Use `importlib.resources` as fallback. Test in both editable and installed modes.
**Warning signs:** `TemplateNotFound` errors only in CI or production.

### Pitfall 4: Breaking Existing Config Validation
**What goes wrong:** Adding `alerts:` to YAML makes `ConfigLoader.validate()` reject configs.
**Why it happens:** Current validation is strict about expected keys (though actually it only checks for `validation_name` and `expectations` presence).
**How to avoid:** The current validator does NOT reject unknown keys, so adding `alerts:` is safe. But the AlertConfig parser should be separate from ConfigLoader validation -- parse it from the loaded config dict after normal validation.
**Warning signs:** Existing tests failing after adding alerts section to configs.

### Pitfall 5: Thread Safety of Circuit Breaker
**What goes wrong:** Race conditions in concurrent validation runs.
**Why it happens:** Shared mutable state without locks.
**How to avoid:** For this framework (batch pipeline, single-threaded), thread safety is not a concern. If `BatchProfiler` parallelism ever includes alert dispatch, add `threading.Lock`. Document the single-threaded assumption.
**Warning signs:** N/A for current architecture.

## Code Examples

### Default Plain-Text Alert Template (summary.txt.j2)
```jinja2
DATA QUALITY ALERT: {{ suite_name }}
{{ "=" * 50 }}
Status: {{ "PASSED" if success else "FAILED" }}
Batch: {{ batch_name }}
Timestamp: {{ timestamp }}
Success Rate: {{ "%.1f"|format(success_rate) }}%
Checks: {{ evaluated_checks }} evaluated, {{ failed_checks }} failed

{% if threshold_failures %}
Threshold Violations:
{% for failure in threshold_failures %}
  - {{ failure }}
{% endfor %}
{% endif %}

{% if failed_expectations %}
Failed Expectations:
{% for exp in failed_expectations %}
  - [{{ exp.severity | upper }}] {{ exp.expectation }} on column '{{ exp.column }}'
{% endfor %}
{% endif %}

{% if severity_stats %}
Severity Breakdown:
{% for severity, stats in severity_stats.items() %}
  {% if stats.total > 0 %}
  {{ severity | upper }}: {{ stats.passed }}/{{ stats.total }} passed
  {% endif %}
{% endfor %}
{% endif %}
```

### AlertDispatcher Base Class
```python
from abc import ABC, abstractmethod

class AlertChannel(ABC):
    """Base class for alert delivery channels (Phase 7 implements concrete channels)."""

    @abstractmethod
    def send(self, message: str, subject: str, severity: str) -> bool:
        """Send an alert message. Returns True on success."""

class AlertDispatcher:
    """Orchestrates formatting + delivery across channels with failure handling."""

    def __init__(self, config: AlertConfig, formatter: AlertFormatter):
        self._config = config
        self._formatter = formatter
        self._channels: dict[str, AlertChannel] = {}
        self._breakers: dict[str, CircuitBreaker] = {}

    def register_channel(self, name: str, channel: AlertChannel) -> None:
        self._channels[name] = channel
        cb_config = self._config.circuit_breaker
        self._breakers[name] = CircuitBreaker(
            failure_threshold=cb_config.failure_threshold,
            cooldown_seconds=cb_config.cooldown_seconds,
        )

    def dispatch(self, results: dict) -> dict[str, bool]:
        """Send alerts to all enabled channels, respecting circuit breakers and failure policy."""
        outcomes = {}
        for channel_cfg in self._config.channels:
            if not channel_cfg.enabled or channel_cfg.type not in self._channels:
                continue
            name = channel_cfg.type
            breaker = self._breakers[name]
            if not breaker.allow_request():
                logger.warning(f"Circuit open for channel '{name}', skipping")
                outcomes[name] = False
                continue
            # ... format + send + record success/failure + apply policy
        return outcomes
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| O365 Connectors for Teams webhooks | Power Automate Workflows with Adaptive Cards | April 2026 retirement | Teams channel in Phase 7 must use Workflows format |
| Inline alert formatting | Template-based formatting (Jinja2) | Industry standard | Allows user customization without code changes |
| Silent failure + retry | Circuit breaker + explicit failure policies | Resilience patterns matured | Prevents cascade delays in batch pipelines |

**Deprecated/outdated:**
- O365 Connector webhooks for Teams: Retiring April 2026. Phase 7 already plans for Workflows format (tracked in STATE.md decisions).
- The current `_send_alert` stub in `fabric_connector.py` will be replaced, not extended.

## Open Questions

1. **Environment variable substitution in YAML**
   - What we know: Webhook URLs and SMTP credentials should not be hardcoded in YAML
   - What's unclear: Whether to use `os.path.expandvars()`, a custom `${VAR}` resolver, or require all secrets via constructor args
   - Recommendation: Use `os.environ.get()` at config load time with `${VAR_NAME}` syntax in YAML. Simple, predictable, no new dependencies.

2. **Where to store default templates**
   - What we know: Jinja2 `PackageLoader` or `importlib.resources` can load from package data
   - What's unclear: Whether `pyproject.toml` package_data config is sufficient for both editable and installed modes
   - Recommendation: Use `PackageLoader` with `dq_framework.alerting` package. Add `[tool.setuptools.package-data]` to pyproject.toml. Test in editable install.

3. **Should AlertConfig be part of ConfigLoader.validate() or separate?**
   - What we know: Current `ConfigLoader.validate()` only checks `validation_name` and `expectations` keys
   - What's unclear: Whether to extend ConfigLoader or keep alert config parsing fully separate
   - Recommendation: Keep separate. `AlertConfig.from_dict(config.get("alerts"))` called by the dispatcher, not by ConfigLoader. This maintains backward compatibility and separation of concerns. Phase 10 (INTG-02) will wire it together.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest >= 9.0.0 with pytest-mock >= 3.15.0 |
| Config file | pyproject.toml `[tool.pytest.ini_options]` |
| Quick run command | `pytest tests/test_alerting.py -x --no-cov` |
| Full suite command | `pytest --cov=dq_framework --cov-fail-under=60` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ALRT-03 | Jinja2 templates render alert messages with validation summary, failed expectations, severity | unit | `pytest tests/test_alerting.py::TestAlertFormatter -x --no-cov` | No -- Wave 0 |
| ALRT-05 | YAML config supports alerts: section with channel config and routing | unit | `pytest tests/test_alerting.py::TestAlertConfig -x --no-cov` | No -- Wave 0 |
| ALRT-06 | Alert delivery failures handled with explicit policies (warn/raise/fallback) | unit | `pytest tests/test_alerting.py::TestFailurePolicy -x --no-cov` | No -- Wave 0 |
| ALRT-07 | Circuit breaker stops retrying after N failures and recovers after cooldown | unit | `pytest tests/test_alerting.py::TestCircuitBreaker -x --no-cov` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_alerting.py -x --no-cov`
- **Per wave merge:** `pytest --cov=dq_framework --cov-fail-under=60`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_alerting.py` -- covers ALRT-03, ALRT-05, ALRT-06, ALRT-07
- [ ] `dq_framework/alerting/templates/summary.txt.j2` -- default template for formatter tests
- [ ] `[tool.setuptools.package-data]` in pyproject.toml -- ensure templates are included in package

## Sources

### Primary (HIGH confidence)
- Codebase analysis: `fabric_connector.py` lines 490-582 -- existing `_send_alert` stub and `handle_validation_failure`
- Codebase analysis: `config_loader.py` -- current YAML validation (only checks `validation_name` + `expectations`)
- Codebase analysis: `constants.py` -- severity levels (critical/high/medium/low)
- Codebase analysis: `validator.py` -- validation result dict structure with `failed_expectations`, `severity_stats`
- Codebase analysis: `storage.py` -- established abstraction pattern (ABC + concrete implementations)
- pyproject.toml -- `pyyaml>=6.0` already a dependency; `jinja2` available via GX

### Secondary (MEDIUM confidence)
- [Jinja2 templating best practices](https://betterstack.com/community/guides/scaling-python/jinja-templating/) -- template organization patterns
- [Grafana OnCall alert templates](https://grafana.com/docs/oncall/latest/configure/jinja2-templating/) -- alert-specific Jinja2 patterns
- [PyBreaker circuit breaker](https://github.com/danielfm/pybreaker) -- state machine reference design
- [circuitbreaker PyPI](https://pypi.org/project/circuitbreaker/) -- v2.1.3, alternative implementation

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- Jinja2 and PyYAML are already in the dependency tree; no new packages needed
- Architecture: HIGH -- Pattern follows established storage.py abstraction; codebase conventions are clear
- Pitfalls: HIGH -- Based on direct code analysis of existing _send_alert stub and config validation
- Circuit breaker: HIGH -- Well-understood pattern with 3 states; hand-rolling is appropriate for this scope

**Research date:** 2026-03-08
**Valid until:** 2026-04-08 (stable domain, no fast-moving dependencies)
