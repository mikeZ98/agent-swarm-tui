"""Guardrail & error alerts: routing violations, schema errors, gateway failures.

``alert_from_exception`` classifies exceptions the app catches itself (e.g. a
rejected ``dispatch_task``); ``alert_from_failure`` classifies the
orchestrator's ``AgentFailed`` events by their ``error_type``. Retryable
gateway conditions (429, timeouts, dropped connections) are warnings;
everything else is an error.
"""

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import ClassVar, Final
from uuid import UUID

from pydantic import AwareDatetime, Field, FiniteFloat, ValidationError
from rich.text import Text
from textual.app import ComposeResult
from textual.widget import Widget
from textual.widgets import RichLog

from agent_swarm_tui.client import GatewayRateLimitError
from agent_swarm_tui.core import AgentFailed, SwarmEvent
from agent_swarm_tui.schemas import StrictModel, SwarmTopology
from agent_swarm_tui.widgets._style import format_duration, one_line

MAX_ALERT_CHARS: Final = 1024
MAX_ALERT_LINES: Final = 1000


class AlertSeverity(StrEnum):
    """How loud an alert is."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class AlertCategory(StrEnum):
    """What kind of guardrail or failure raised the alert."""

    ROUTING = "routing"
    SCHEMA = "schema"
    STATE = "state"
    RATE_LIMIT = "rate-limit"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    GATEWAY = "gateway"
    PROTOCOL = "protocol"
    AGENT = "agent"
    SYSTEM = "system"


# Exception class name -> category; looked up along the MRO, so subclasses
# inherit their parent's category.
_CATEGORIES: Final[Mapping[str, AlertCategory]] = MappingProxyType(
    {
        "RoutingError": AlertCategory.ROUTING,
        "AgentNotFoundError": AlertCategory.ROUTING,
        "ValidationError": AlertCategory.SCHEMA,
        "InvalidStateTransitionError": AlertCategory.STATE,
        "GatewayRateLimitError": AlertCategory.RATE_LIMIT,
        "GatewayTimeoutError": AlertCategory.TIMEOUT,
        "GatewayConnectionError": AlertCategory.CONNECTION,
        "GatewayProtocolError": AlertCategory.PROTOCOL,
        "GatewayHTTPError": AlertCategory.GATEWAY,
        "GatewayStreamError": AlertCategory.GATEWAY,
        "GatewayClientError": AlertCategory.GATEWAY,
    }
)
_RETRYABLE: Final = frozenset(
    {AlertCategory.RATE_LIMIT, AlertCategory.TIMEOUT, AlertCategory.CONNECTION}
)
_SEVERITY_STYLE: Final[Mapping[AlertSeverity, tuple[str, str]]] = MappingProxyType(
    {
        AlertSeverity.INFO: ("\u25cf", "dodger_blue2"),
        AlertSeverity.WARNING: ("⚠", "bold yellow3"),
        AlertSeverity.ERROR: ("✖", "bold red3"),
    }
)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Alert(StrictModel):
    """One alert line."""

    severity: AlertSeverity
    category: AlertCategory
    message: str = Field(min_length=1, max_length=MAX_ALERT_CHARS)
    agent_name: str | None = Field(default=None, max_length=64)
    retry_after_s: FiniteFloat | None = Field(default=None, ge=0.0)
    timestamp: AwareDatetime = Field(default_factory=_utcnow)


def categorize(error_type: str) -> AlertCategory:
    """Category for an exception class name (``AGENT`` when unknown)."""
    return _CATEGORIES.get(error_type, AlertCategory.AGENT)


def _clip(message: str) -> str:
    return one_line(message, MAX_ALERT_CHARS) or "(no details)"


def alert_from_exception(exc: BaseException, *, agent_name: str | None = None) -> Alert:
    """Classify an exception caught by the app."""
    category = next(
        (_CATEGORIES[cls.__name__] for cls in type(exc).__mro__ if cls.__name__ in _CATEGORIES),
        AlertCategory.AGENT,
    )
    if isinstance(exc, ValidationError):
        message = f"{exc.title}: {exc.error_count()} validation error(s)"
        if exc.error_count():
            first = exc.errors()[0]
            where = ".".join(str(part) for part in first["loc"])
            message += f"; {first['msg']}" + (f" ({where})" if where else "")
    else:
        message = str(exc) or type(exc).__name__
    retryable = category in _RETRYABLE or getattr(exc, "retryable", False) is True
    return Alert(
        severity=AlertSeverity.WARNING if retryable else AlertSeverity.ERROR,
        category=category,
        message=_clip(message),
        agent_name=agent_name,
        retry_after_s=exc.retry_after_s if isinstance(exc, GatewayRateLimitError) else None,
    )


def alert_from_failure(event: AgentFailed, *, agent_name: str | None = None) -> Alert:
    """Classify an orchestrator ``AgentFailed`` event."""
    category = categorize(event.error_type)
    retryable = event.retryable or category in _RETRYABLE
    return Alert(
        severity=AlertSeverity.WARNING if retryable else AlertSeverity.ERROR,
        category=category,
        message=_clip(event.error),
        agent_name=agent_name,
    )


def alert_line(alert: Alert) -> Text:
    """Render one alert: time, severity glyph, category, agent and message."""
    glyph, style = _SEVERITY_STYLE[alert.severity]
    line = Text(f"{alert.timestamp.astimezone():%H:%M:%S} ", style="dim")
    line.append(f"{glyph} {alert.category.value.upper():<10}", style=style)
    if alert.agent_name:
        line.append(f" {alert.agent_name}:", style="bold")
    line.append(f" {alert.message}")
    if alert.retry_after_s is not None:
        line.append(f"  (retry in {format_duration(alert.retry_after_s)})", style="dim")
    return line


class AlertsPanel(Widget):
    """Scrolling list of alerts with per-severity counts in the border."""

    DEFAULT_CSS: ClassVar[str] = """
    AlertsPanel {
        border: round $warning;
        border-title-color: $warning;
    }
    AlertsPanel > RichLog {
        background: transparent;
        scrollbar-size-vertical: 1;
    }
    """

    def __init__(
        self,
        *,
        name: str | None = None,
        id: str | None = None,  # noqa: A002 - Textual's widget API
        classes: str | None = None,
    ) -> None:
        """Create an empty panel."""
        super().__init__(name=name, id=id, classes=classes)
        self._names: dict[UUID, str] = {}
        self._counts: Counter[AlertSeverity] = Counter()
        self._alerts: list[Alert] = []
        self._update_title()

    def compose(self) -> ComposeResult:
        """Compose the alert log."""
        yield RichLog(wrap=True, markup=False, highlight=False, max_lines=MAX_ALERT_LINES)

    @property
    def alerts(self) -> tuple[Alert, ...]:
        """Every alert added so far (newest last, capped at ``MAX_ALERT_LINES``)."""
        return tuple(self._alerts)

    def count(self, severity: AlertSeverity) -> int:
        """Return how many ``severity`` alerts arrived since the last clear."""
        return self._counts[severity]

    def load_topology(self, topology: SwarmTopology) -> None:
        """Learn agent names for ``AgentFailed`` alerts."""
        self._names = {agent.id: agent.name for agent in topology.agents}

    def add_alert(self, alert: Alert) -> None:
        """Append ``alert``."""
        self._alerts.append(alert)
        del self._alerts[:-MAX_ALERT_LINES]
        self._counts[alert.severity] += 1
        self._update_title()
        if self.is_mounted:
            self.query_one(RichLog).write(alert_line(alert), expand=True)

    def notify_exception(self, exc: BaseException, *, agent_id: UUID | None = None) -> Alert:
        """Classify and append an exception; return the alert."""
        name = self._names.get(agent_id) if agent_id is not None else None
        alert = alert_from_exception(exc, agent_name=name)
        self.add_alert(alert)
        return alert

    def info(self, message: str) -> None:
        """Append an informational system notice."""
        self.add_alert(
            Alert(
                severity=AlertSeverity.INFO, category=AlertCategory.SYSTEM, message=_clip(message)
            )
        )

    def apply_event(self, event: SwarmEvent) -> None:
        """Consume a swarm event (only ``AgentFailed`` raises an alert)."""
        if isinstance(event, AgentFailed):
            self.add_alert(alert_from_failure(event, agent_name=self._names.get(event.agent_id)))

    def clear_alerts(self) -> None:
        """Drop all alerts and reset the counters."""
        self._alerts.clear()
        self._counts.clear()
        self._update_title()
        if self.is_mounted:
            self.query_one(RichLog).clear()

    def on_mount(self) -> None:
        """Replay alerts added before mounting."""
        log = self.query_one(RichLog)
        for alert in self._alerts:
            log.write(alert_line(alert), expand=True)

    def _update_title(self) -> None:
        self.border_title = "Alerts"
        warnings, errors = self._counts[AlertSeverity.WARNING], self._counts[AlertSeverity.ERROR]
        self.border_subtitle = f"⚠ {warnings}  ✖ {errors}" if warnings or errors else ""
