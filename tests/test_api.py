import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from app import main
from app.state import DashboardState


class ControlAPI(unittest.TestCase):
    def setUp(self):
        self.state = DashboardState()
        self.producer = Mock()
        self.simulator = SimpleNamespace(thread=SimpleNamespace(is_alive=lambda: True), recovery_start=None)
        self.patches = [patch.object(main, 'state', self.state),
                        patch.object(main, 'producer', self.producer),
                        patch.object(main, 'simulator', self.simulator)]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)
        # Deliberately no lifespan: exercise routes with an explicitly mocked broker.
        self.client = TestClient(main.app)

    def test_routes_and_acknowledged_actions(self):
        for route in ('/', '/health', '/api/status', '/api/history'):
            self.assertEqual(self.client.get(route).status_code, 200)
        self.assertEqual(self.client.post('/api/deploy').status_code, 200)
        args, kwargs = self.producer.send.call_args
        self.assertEqual(args[0], 'deployment_events')
        self.assertEqual(args[2]['version'], '2.4.1')
        self.assertTrue(kwargs['sync'])
        self.assertEqual(self.state.mode, 'BAD_RELEASE')
        self.assertEqual(self.client.post('/api/deploy').status_code, 409)
        self.assertEqual(self.client.post('/api/rollback').status_code, 200)
        self.assertEqual(self.producer.send.call_args.args[2]['action'], 'ROLLBACK')
        self.assertEqual(self.state.mode, 'RECOVERY')

    def test_failed_delivery_preserves_release(self):
        self.producer.send.side_effect = RuntimeError('offline')
        self.assertEqual(self.client.post('/api/deploy').status_code, 503)
        self.assertEqual(self.state.release, '2.3.7')
        self.assertEqual(len(self.state.markers), 0)
