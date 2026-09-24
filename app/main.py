from __future__ import annotations
import logging
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event, Thread, RLock
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from app.consumer import OutputConsumers
from app.kafka import EventProducer
from app.simulator import Simulator
from app.state import DashboardState
from launchguard.config import load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)
state, stop = DashboardState(), Event()
control_lock = RLock()
producer: EventProducer | None = None
simulator: Simulator | None = None
bootstrap_thread: Thread | None = None

def deployment(version: str, action: str):
    if producer is None:
        raise HTTPException(503, "Kafka producer unavailable")
    try:
        producer.send("deployment_events", "checkout-service",
                      {"deployment_id": str(uuid.uuid4()), "service": "checkout-service",
                       "version": version, "action": action}, sync=True)
        state.delivery(True)
    except Exception as exc:
        log.error("Deployment delivery failed: %s", type(exc).__name__)
        state.delivery(False)
        raise HTTPException(503, "Deployment event could not be delivered to Kafka") from None

def bootstrap():
    global simulator
    while not stop.is_set():
        try:
            deployment("2.3.7", "DEPLOY")
            state.transition("HEALTHY", "2.3.7", "DEPLOY")
            if stop.is_set():
                return
            simulator = Simulator(producer, state, stop, control_lock)
            simulator.start()
            return
        except HTTPException:
            log.warning("Waiting for initial Kafka deployment delivery")
            stop.wait(10)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, bootstrap_thread, simulator
    stop.clear()
    settings = load_settings()
    simulator = None
    producer = EventProducer(settings, on_delivery=state.delivery)
    outputs = OutputConsumers(settings, state, stop)
    outputs.start()
    bootstrap_thread = Thread(target=bootstrap, daemon=True, name="initial-deployment")
    bootstrap_thread.start()
    try:
        yield
    finally:
        stop.set()
        if bootstrap_thread:
            bootstrap_thread.join(timeout=18)
        if simulator:
            simulator.thread.join(timeout=5)
        outputs.close()
        if producer:
            producer.close()

app = FastAPI(title="LaunchGuard", lifespan=lifespan)

@app.get("/")
def index():
    return FileResponse(Path(__file__).resolve().parent.parent / "static" / "index.html")

@app.get("/api/status")
def status():
    return state.snapshot()

@app.get("/api/history")
def history():
    return state.series()

@app.post("/api/deploy")
def deploy():
    with control_lock:
        if simulator is None or not simulator.thread.is_alive():
            raise HTTPException(503, "Waiting for stable deployment")
        if state.mode != "HEALTHY":
            raise HTTPException(409, "Deploy requires healthy mode")
        deployment("2.4.1", "DEPLOY")
        state.transition("BAD_RELEASE", "2.4.1", "DEPLOY")
    return {"status": "deploying", "version": "2.4.1"}

@app.post("/api/rollback")
def rollback():
    with control_lock:
        if state.mode != "BAD_RELEASE":
            raise HTTPException(409, "Rollback requires bad release")
        deployment("2.3.7", "ROLLBACK")
        state.transition("RECOVERY", "2.3.7", "ROLLBACK")
        if simulator:
            simulator.recovery_start = time.monotonic()
    return {"status": "rollback_started", "version": "2.3.7"}

@app.get("/health")
def health():
    return {"app": "ok", "simulator": bool(simulator and simulator.thread.is_alive()), **state.snapshot()}
