import os
import json
import httpx
import traceback
import atexit
import asyncio
from openai import AsyncOpenAI, APIConnectionError, APIStatusError

OPENROUTER_BASE_URL = os.environ["OPENROUTER_BASE_URL"]
OPENROUTER_API_KEY = os.environ["OPENROUTER_API_KEY"]
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-v4-flash")

http_client = httpx.AsyncClient(
    http2=False,
    timeout=httpx.Timeout(90.0, connect=30.0),
)

client = AsyncOpenAI(
    base_url=OPENROUTER_BASE_URL,
    api_key=OPENROUTER_API_KEY,
    http_client=http_client,
    max_retries=2,
)

AGENT_SYSTEM_PROMPTS = {
    "agent-1": "You are a summarization agent. Summarize the user request clearly and briefly.",
    "agent-2": "You are an action-item extraction agent. Extract concrete next steps and TODOs.",
    "agent-3": "You are a risk and priority analysis agent. Identify risks, blockers, and priority.",
    "agent-4": "You are a recommendation agent. Produce a practical final recommendation.",
}

async def choose_agents(user_text: str) -> dict:
    print("Determining agents to use")
    try:        
        response = await client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an agent router. Decide which agents are needed. "
                        "Return only valid JSON with keys: agents, reasoning. "
                        "Available agents: "
                        "agent-1=summarization, "
                        "agent-2=action items, "
                        "agent-3=risk and priority analysis, "
                        "agent-4=recommendation."
                    ),
                },
                {
                    "role": "user",
                    "content": user_text,
                },
            ],
        )

        return json.loads(response.choices[0].message.content)
    except APIConnectionError as e:
        print("Connection error:", repr(e))
        traceback.print_exc()
        raise
    except APIStatusError as e:
        print("Status:", e.status_code)
        print(e.response.text)
        raise
    except Exception:
        traceback.print_exc()
        raise

async def run_agent_llm(agent_id: str, task: dict) -> dict:
    print(f"[{agent_id}] sending prompt to llm")

    try:
        response = await client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": AGENT_SYSTEM_PROMPTS[agent_id],
                },
                {
                    "role": "user",
                    "content": json.dumps(task["payload"], indent=2),
                },
            ],
            extra_body={"reasoning": {"enabled": True}}
        )

        print(f"[{agent_id}] response from llm completed")

        return {
            "agent_id": agent_id,
            "goal": task["goal"],
            "result": response.choices[0].message.content,
        }
    except APIConnectionError as e:
        print("Connection error:", repr(e))
        traceback.print_exc()
        raise
    except APIStatusError as e:
        print("Status:", e.status_code)
        print(e.response.text)
        raise
    except Exception:
        traceback.print_exc()
        raise


async def synthesize_final_answer(conversation_id: str, user_text: str, agent_outputs: list[dict]) -> str:
    print("Generating final answer")
    
    try:
        response = await client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are the orchestrator. Combine the outputs from multiple agents "
                        "into one concise final answer."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "conversation_id": conversation_id,
                            "user_text": user_text,
                            "agent_outputs": agent_outputs,
                        },
                        indent=2,
                    ),
                },
            ],
            extra_body={"reasoning": {"enabled": True}}
        )

        return response.choices[0].message.content
    except APIConnectionError as e:
        print("Connection error:", repr(e))
        traceback.print_exc()
        raise
    except APIStatusError as e:
        print("Status:", e.status_code)
        print(e.response.text)
        raise
    except Exception:
        traceback.print_exc()
        raise

@atexit.register
def cleanup():
    try:
        asyncio.run(http_client.aclose())
    except Exception:
        pass