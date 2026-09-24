import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
from threading import Event
from app.state import DashboardState
from app.simulator import Simulator
from app.consumer import decode

class LocalBehavior(unittest.TestCase):
    def test_revenue_requires_real_incident_and_sustained_recovery(self):
        state = DashboardState()
        base = datetime.now(timezone.utc)
        state.transition("BAD_RELEASE", "2.4.1", "DEPLOY")
        state.transition_at = base
        state.ingest("incident_signals", {"ts": base + timedelta(seconds=10),
            "service": "checkout-service", "version": "2.4.1", "severity": "CRITICAL",
            "failure_rate": .15, "forecast_failure_rate": .16, "expected_revenue_loss_hour": 14800})
        state.transition("RECOVERY", "2.3.7", "ROLLBACK")
        state.transition_at = base + timedelta(seconds=20)
        def healthy(seconds):
            return {"ts": base + timedelta(seconds=seconds), "service": "checkout-service",
                "version": "2.3.7", "severity": "HEALTHY", "failure_rate": .02,
                "forecast_failure_rate": .02, "expected_revenue_loss_hour": 1200}
        # A delayed stable window from before rollback cannot count as recovery.
        state.ingest("incident_signals", healthy(15))
        self.assertEqual(state.recovery_windows, 0)
        for seconds in (60, 70):
            state.ingest("incident_signals", healthy(seconds))
            self.assertIsNone(state.protected)
        state.ingest("incident_signals", healthy(70))
        self.assertEqual(state.recovery_windows, 2)
        state.ingest("incident_signals", healthy(80))
        self.assertEqual(state.protected, 13600)

    def test_stale_pipeline_and_delivery_are_not_connected(self):
        state = DashboardState()
        with patch("app.state.time.monotonic", return_value=100):
            state.delivery(True)
            for topic in state.ready:
                state.pipeline(topic, True)
                state.ingest(topic, {"service": "checkout-service", "version": "2.3.7",
                    "ts": datetime.now(timezone.utc) + timedelta(seconds=1)})
            self.assertTrue(state.snapshot()["flink_connected"])
            self.assertTrue(state.snapshot()["kafka_connected"])
        with patch("app.state.time.monotonic", return_value=160):
            self.assertFalse(state.snapshot()["flink_connected"])
            self.assertFalse(state.snapshot()["kafka_connected"])

    def test_nonfinite_metrics_are_not_json_numbers(self):
        from app.state import num
        self.assertIsNone(num(float("nan")))
        self.assertIsNone(num(float("inf")))

    def test_bad_release_has_higher_failures(self):
        state = DashboardState()
        simulator = Simulator(None, state, Event())
        normal = [simulator.event() for _ in range(3000)]
        state.transition("BAD_RELEASE", "2.4.1", "DEPLOY")
        bad = [simulator.event() for _ in range(3000)]
        healthy_rate = sum(not x["success"] for x in normal) / len(normal)
        bad_rate = sum(not x["success"] for x in bad) / len(bad)
        self.assertLess(healthy_rate, .04)
        self.assertGreater(bad_rate, .12)
        self.assertLess(bad_rate, .20)
        segment = [x for x in bad if x["region"] == "BR" and x["payment_method"] == "PIX"]
        self.assertGreater(sum(not x["success"] for x in segment)/len(segment), bad_rate)

    def test_json_output(self):
        self.assertEqual(decode(b'{"service":"checkout-service"}', "release_metrics", lambda *args: None),
                         {"service": "checkout-service"})

if __name__ == "__main__":
    unittest.main()
