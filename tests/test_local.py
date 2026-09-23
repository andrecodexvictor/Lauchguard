import unittest
from threading import Event
from app.state import DashboardState
from app.simulator import Simulator
from app.consumer import decode

class LocalBehavior(unittest.TestCase):
    def test_revenue_requires_real_incident_and_recovery(self):
        state = DashboardState()
        state.transition("BAD_RELEASE", "2.4.1", "DEPLOY")
        state.ingest("incident_signals", {"service": "checkout-service", "version": "2.4.1",
                                           "severity": "CRITICAL", "expected_revenue_loss_hour": 14800})
        state.transition("RECOVERY", "2.3.7", "ROLLBACK")
        state.ingest("incident_signals", {"service": "checkout-service", "version": "2.4.1",
                                           "severity": "CRITICAL", "expected_revenue_loss_hour": 999999})
        self.assertIsNone(state.snapshot()["revenue_protected"])
        state.ingest("incident_signals", {"service": "checkout-service", "version": "2.3.7",
                                           "severity": "HEALTHY", "expected_revenue_loss_hour": 1200})
        self.assertEqual(state.snapshot()["revenue_protected"], 14800)

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
