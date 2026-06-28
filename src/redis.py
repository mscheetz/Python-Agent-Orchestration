import redis
from src.config import REDIS_URL
import json
from typing import Any

def client() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)

def save_agent_output(agent_id: str, task_id: str, output: dict[str, Any]) -> str:
    key = f"agents:{agent_id}:tasks:{task_id}:output"
    client().set(key, json.dumps(output), ex=60*60)
    return key

def load_json(key: str) -> dict[str, Any]:
    value = client().get(key)
    return json.loads(value) if value else {}