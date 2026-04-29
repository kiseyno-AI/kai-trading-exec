from fastapi import FastAPI

app = FastAPI(title="kai-trading-exec", version="0.1.0")


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "service": "kai-trading-exec",
        "mode": "bootstrap",
    }


@app.get("/runtime/status")
def runtime_status() -> dict[str, object]:
    return {
        "service": "kai-trading-exec",
        "mode": "bootstrap",
        "ready": False,
        "message": "Execution runtime bootstrap scaffold only. See TASKS.md and ARCHITECTURE.md.",
        "safety": {
            "paused": True,
            "kill_switch": False,
        },
    }
