from __future__ import annotations

import math
import time
from collections import deque
from datetime import datetime, timezone
from threading import RLock
from typing import Any

TOPICS = ("release_metrics", "impact_forecast", "incident_signals")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def num(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def timestamp(value: Any) -> datetime | None:
    try:
        if isinstance(value, datetime):
            result = value
        elif isinstance(value, (float, int)):
            result = datetime.fromtimestamp(value / 1000, timezone.utc)
        else:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


class DashboardState:
    def __init__(self):
        self.lock = RLock()
        self.mode, self.release = "HEALTHY", "2.3.7"
        self.kafka_connected = False
        self.last_delivery = None
        self.ready = {t: False for t in TOPICS}
        self.seen = {t: None for t in TOPICS}
        self.latest = {}
        self.field_times = {}
        self.actual = self.forecast = self.latency = self.gmv = self.loss = None
        self.severity = "WAITING"
        self.updated = None
        self.history = deque(maxlen=120)
        self.markers = deque(maxlen=20)
        self.peak = self.protected = None
        self.incident = False
        self.error = None
        self.transition_at = datetime.now(timezone.utc)
        self.recovery_windows = 0

    def delivery(self, success: bool):
        with self.lock:
            self.kafka_connected = success
            self.error = None if success else "Kafka delivery failed; retrying"
            if success:
                self.last_delivery = time.monotonic()

    def pipeline(self, topic: str, ready: bool):
        with self.lock:
            self.ready[topic] = ready

    def transition(self, mode: str, version: str, action: str):
        with self.lock:
            self.transition_at = datetime.now(timezone.utc)
            self.mode, self.release = mode, version
            self.markers.append({"ts": self.transition_at.isoformat(), "action": action, "version": version})
            self.actual = self.forecast = self.latency = self.gmv = self.loss = None
            self.severity = "WAITING"
            self.recovery_windows = 0
            self.field_times.clear()
            if mode == "BAD_RELEASE":
                self.peak = self.protected = None
                self.incident = False

    def _set(self, field, value, ts):
        if ts >= self.field_times.get(field, self.transition_at):
            setattr(self, field, value)
            self.field_times[field] = ts

    def ingest(self, topic: str, row: dict[str, Any]):
        with self.lock:
            if topic not in self.ready or row.get("service") != "checkout-service":
                return
            ts = timestamp(row.get("ts") or row.get("window_end"))
            if ts is None or ts < self.transition_at or row.get("version") != self.release:
                return
            # Reject duplicates, replays and out-of-order windows on each output.
            if topic in self.latest and ts <= self.latest[topic]:
                return
            self.latest[topic] = ts
            self.seen[topic] = time.monotonic()
            self.updated = max(self.latest.values()).isoformat()
            actual = num(row.get("failure_rate"))
            forecast = num(row.get("forecast_failure_rate"))
            if topic == "release_metrics":
                self._set("actual", actual, ts)
                self._set("latency", num(row.get("avg_latency_ms")), ts)
            if topic in ("impact_forecast", "incident_signals"):
                self._set("forecast", forecast, ts)
                self._set("gmv", num(row.get("projected_gmv_at_risk_hour")), ts)
            if topic in ("release_metrics", "impact_forecast"):
                point = next((p for p in self.history if p["ts"] == ts.isoformat()), None)
                if point is None:
                    point = {"ts": ts.isoformat(), "actual": None, "forecast": None}
                    self.history.append(point)
                point["actual" if topic == "release_metrics" else "forecast"] = actual if topic == "release_metrics" else forecast
            if topic == "incident_signals":
                self.severity = str(row.get("severity") or "WARMING_UP").upper()
                loss = num(row.get("expected_revenue_loss_hour"))
                self._set("loss", loss, ts)
                if self.mode == "BAD_RELEASE" and self.severity in ("WARNING", "HIGH", "CRITICAL") and loss is not None:
                    self.incident = True
                    self.peak = max(self.peak or 0, loss)
                if self.mode == "RECOVERY":
                    elapsed = (ts - self.transition_at).total_seconds()
                    healthy = (elapsed >= 35 and self.severity == "HEALTHY" and
                               actual is not None and actual < .04 and forecast is not None and forecast < .04)
                    self.recovery_windows = self.recovery_windows + 1 if healthy else 0
                    if self.recovery_windows >= 3 and self.incident and self.peak is not None and loss is not None:
                        self.protected = max(0, self.peak - max(0, loss))
                    elif not healthy:
                        self.protected = None

    def snapshot(self):
        with self.lock:
            clock = time.monotonic()
            fresh = {t: seen is not None and clock - seen < 55 for t, seen in self.seen.items()}
            live = all(self.ready.values()) and all(fresh.values())
            connected = self.kafka_connected and self.last_delivery is not None and clock - self.last_delivery < 30
            return {"simulator_mode": self.mode, "current_release": self.release,
                    "kafka_connected": connected, "flink_connected": live,
                    "pipeline_topics": dict(self.ready), "pipeline_fresh": fresh,
                    "pipeline_status": "LIVE" if live else "WAITING FOR CONFLUENT FLINK PIPELINE",
                    "current_failure_rate": self.actual, "forecast_failure_rate": self.forecast,
                    "avg_latency_ms": self.latency, "projected_gmv_at_risk_hour": self.gmv,
                    "expected_revenue_loss_hour": self.loss, "severity": self.severity,
                    "last_update": self.updated, "incident_observed": self.incident,
                    "recovery_windows": self.recovery_windows,
                    "revenue_protected": self.protected if live else None,
                    "producer_error": self.error}

    def series(self):
        with self.lock:
            return {"points": sorted(self.history, key=lambda p: p["ts"]), "markers": list(self.markers)}
