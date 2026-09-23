from __future__ import annotations
from collections import deque
from datetime import datetime, timezone
from threading import RLock
from typing import Any

def now() -> str:
    return datetime.now(timezone.utc).isoformat()

def num(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None

class DashboardState:
    def __init__(self):
        self.lock = RLock()
        self.mode, self.release = "HEALTHY", "2.3.7"
        self.kafka_connected = False
        self.ready = {t: False for t in ("release_metrics", "impact_forecast", "incident_signals")}
        self.seen = {t: False for t in self.ready}
        self.actual = self.forecast = self.latency = self.gmv = self.loss = None
        self.severity = "HEALTHY"
        self.updated = None
        self.history = deque(maxlen=120)
        self.markers = deque(maxlen=20)
        self.peak = self.protected = None
        self.incident = False
        self.error = None

    def transition(self, mode: str, version: str, action: str):
        with self.lock:
            self.mode, self.release = mode, version
            self.markers.append({"ts": now(), "action": action, "version": version})
            self.actual = self.forecast = self.latency = self.gmv = self.loss = None
            self.severity = "HEALTHY"
            if mode == "BAD_RELEASE":
                self.peak = self.protected = None
                self.incident = False

    def ingest(self, topic: str, row: dict[str, Any]):
        with self.lock:
            self.seen[topic] = True
            if row.get("service") not in (None, "checkout-service") or row.get("version") not in (None, self.release):
                return
            ts = row.get("ts") or row.get("window_end") or now()
            if isinstance(ts, datetime):
                ts = ts.isoformat()
            self.updated = str(ts)
            if topic == "release_metrics":
                self.actual = num(row.get("failure_rate"))
                self.latency = num(row.get("avg_latency_ms"))
                if self.actual is not None:
                    self.history.append({"ts": self.updated, "actual": self.actual, "forecast": None})
            elif topic == "impact_forecast":
                self.forecast = num(row.get("forecast_failure_rate"))
                self.gmv = num(row.get("projected_gmv_at_risk_hour"))
                if self.forecast is not None:
                    self.history.append({"ts": self.updated, "actual": None, "forecast": self.forecast})
            else:
                self.severity = str(row.get("severity") or "HEALTHY").upper()
                self.actual = num(row.get("failure_rate"))
                self.forecast = num(row.get("forecast_failure_rate"))
                self.gmv = num(row.get("projected_gmv_at_risk_hour"))
                self.loss = num(row.get("expected_revenue_loss_hour"))
                if self.mode == "BAD_RELEASE" and self.severity != "HEALTHY":
                    self.incident = True
                    if self.loss is not None:
                        self.peak = max(self.peak or 0, self.loss)
                if self.mode == "RECOVERY" and self.incident and self.severity == "HEALTHY" and self.peak is not None:
                    self.protected = self.peak

    def snapshot(self):
        with self.lock:
            live = all(self.ready.values()) and all(self.seen.values())
            return {"simulator_mode": self.mode, "current_release": self.release,
                    "kafka_connected": self.kafka_connected, "flink_connected": live,
                    "pipeline_topics": dict(self.ready),
                    "pipeline_status": "LIVE" if live else "WAITING FOR CONFLUENT FLINK PIPELINE",
                    "current_failure_rate": self.actual, "forecast_failure_rate": self.forecast,
                    "avg_latency_ms": self.latency, "projected_gmv_at_risk_hour": self.gmv,
                    "expected_revenue_loss_hour": self.loss, "severity": self.severity,
                    "last_update": self.updated, "incident_observed": self.incident,
                    "revenue_protected": self.protected, "producer_error": self.error}

    def series(self):
        with self.lock:
            return {"points": list(self.history), "markers": list(self.markers)}
