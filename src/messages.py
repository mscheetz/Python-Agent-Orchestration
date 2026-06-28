import json
from datetime import datetime, timezone
from uuid import uuid4
from typing import Any

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def make_task(agent_id: str, goal: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_id": str(uuid4()),
        "agent_id": agent_id,
        "goal": goal,
        "payload": payload,
        "created_at": utc_now(),
    }

def make_completion(task: dict[str, Any], redis_key: str) -> dict[str, Any]:
    return {
        "task_id": task["task_id"],
        "agent_id": task["agent_id"],
        "status": "complete",
        "redis_key": redis_key,
        "completed_at": utc_now(),
    }

def encode(message: dict[str, Any]) -> bytes:
    return json.dumps(message).encode("utf-8")

def decode(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode("utf-8"))