from __future__ import annotations
import logging
import random
import time
import uuid
from threading import Event, Thread, RLock
from app.kafka import EventProducer
from app.state import DashboardState

log = logging.getLogger(__name__)

class Simulator:
    def __init__(self, producer: EventProducer, state: DashboardState, stop: Event, control_lock=None):
        self.producer, self.state, self.stop = producer, state, stop
        self.control_lock = control_lock or RLock()
        self.rng = random.Random(2417)
        self.recovery_start = None
        self.thread = Thread(target=self.run, daemon=True, name="checkout-simulator")

    def start(self):
        self.thread.start()

    def event(self):
        with self.state.lock:
            mode = self.state.mode
        region = self.rng.choices(["BR", "US", "EU", "LATAM"], weights=[40, 25, 20, 15])[0]
        method = self.rng.choices(["PIX", "CARD", "PAYPAL"], weights=[40, 45, 15])[0]
        plan = self.rng.choices(["free", "pro", "enterprise"], weights=[35, 45, 20])[0]
        if mode == "BAD_RELEASE":
            fail = .29 if region == "BR" and method == "PIX" else .12
            latency = self.rng.randint(900, 2200)
        elif mode == "RECOVERY":
            decay = max(0, 1 - (time.monotonic() - (self.recovery_start or time.monotonic())) / 35)
            fail = .02 + decay * (.27 if region == "BR" and method == "PIX" else .10)
            latency = int(self.rng.randint(150, 350) + decay * self.rng.randint(750, 1650))
        else:
            fail, latency = .02, self.rng.randint(150, 350)
        lo, hi = {"free": (30, 150), "pro": (100, 500), "enterprise": (500, 3000)}[plan]
        return {"event_id": str(uuid.uuid4()), "customer_id": "customer-%05d" % self.rng.randrange(10000),
                "service": "checkout-service", "region": region, "plan": plan,
                "payment_method": method, "success": self.rng.random() >= fail,
                "amount": round(self.rng.uniform(lo, hi), 2), "latency_ms": latency}

    def run(self):
        while not self.stop.is_set():
            start = time.monotonic()
            try:
                for _ in range(self.rng.randint(5, 15)):
                    if self.stop.is_set():
                        return
                    with self.control_lock:
                        row = self.event()
                        self.producer.send("checkout_events", row["customer_id"], row)
            except Exception as exc:
                log.error("Checkout publish failed: %s", type(exc).__name__)
                self.state.delivery(False)
            self.stop.wait(max(.1, 1 - (time.monotonic() - start)))
