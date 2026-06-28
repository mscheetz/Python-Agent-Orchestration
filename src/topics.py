from src.config import AGENTS

def agent_topic(agent_id: str) -> str:
    if agent_id not in AGENTS:
        raise ValueError(f"Unknown agent_id: '{agent_id}'")
    return f"agent-{agent_id.split('-')[1]}-tasks"