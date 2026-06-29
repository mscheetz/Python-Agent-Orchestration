from confluent_kafka import Consumer, Producer

import asyncio
import random
import sys
import time

from src.config import COMPLETION_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from src.llm import run_agent_llm
from src.topics import agent_topic
from src.redis import save_agent_output
from src.messages import make_completion, encode, decode, utc_now

def summarize_request(text: str) -> dict:
    return {
        "summary": f"The user is asking about: {text}",
        "key_topic": "Kafka orchestration with AI agents",
    }

def extract_action_items(text: str) -> dict:
    return {
        "action_items": [
            "Create Kafka topics for each agent",
            "Have the orchestrator dispatch tasks",
            "Have agents save outputs to Redis",
            "Have agents publish completion events",
            "Have the orchestrator collect completions",
        ]
    }

def score_risk_and_priority(text: str) -> dict:
    risks = []

    if "kafka" in text.lower():
        risks.append("Kafka consumer offsets and topic configuration must be handled carefully")

    if "redis" in text.lower():
        risks.append("Redis output keys should be deterministic and easy to inspect")

    return {
        "priority": "high",
        "risk_score": 0.42,
        "risks": risks,
    }

def prepare_recommendation(text: str) -> dict:
    return {
        "recommendation": (
            "Use Kafka for task dispatch and completion events, Redis for storing agent outputs, "
            "and keep the orchestrator responsible for correlation by conversation_id and task_id."
        ),
        "next_step": "Add conversation-level aggregation after all agents complete.",
    }

AGENT_HANDLERS = {
    "agent-1": summarize_request,
    "agent-2": extract_action_items,
    "agent-3": score_risk_and_priority,
    "agent-4": prepare_recommendation,
}

def run_agent(agent_id: str, task: dict) -> dict:
    text = task["payload"]["text"]
    handler = AGENT_HANDLERS[agent_id]

    result = handler(text)

    return {
        "task_id": task["task_id"],
        "agent_id": task["agent_id"],
        "conversation_id": task["payload"]["conversation_id"],
        "goal": task["goal"],
        "input_payload": task["payload"],
        "result": result,
        "generated_at": utc_now(),
    }

async def handle_task(agent_id: str, task: dict, producer: Producer, consumer: Consumer, msg) -> None:
    conversation_id = task["payload"]["conversation_id"]

    print(f"[{agent_id}] received task {task['task_id']} on conversation {conversation_id}")

    llm_result = await run_agent_llm(agent_id, task)

    output = {
        "task_id": task["task_id"],
        "agent_id": agent_id,
        "conversation_id": conversation_id,
        "goal": task["goal"],
        "input_payload": task["payload"],
        "result": llm_result["result"],
        "generated_at": utc_now(),
    }

    redis_key = save_agent_output(agent_id, task["task_id"], output)

    completion = make_completion(task, redis_key)

    producer.produce(
        COMPLETION_TOPIC,
        key=conversation_id,
        value=encode(completion),
    )
    producer.flush()

    consumer.commit(msg)

    print(f"[{agent_id}] saved output to Redis at {redis_key}; completion sent")


async def main(agent_id: str) -> None:
    topic = agent_topic(agent_id)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"{agent_id}-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    consumer.subscribe([topic])
    print(f"[{agent_id} listening on {topic}]")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"[{agent_id}] Kafka error: {msg.error()}")
                continue

            try:
                task = decode(msg.value())
            except json.JSONDecodeError:
                print(f"[{agent_id}] skipping non-JSON message: {msg.value()!r}")
                consumer.commit(msg)
                continue

            try:
                await handle_task(agent_id, task, producer, consumer, msg)
            except Exception as ex:
                print(f"[{agent_id}] failed task {task.get('task_id')}: {ex}")
                
    finally:
        producer.flush()
        consumer.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m app.agent <agent-1|agent-2|agent-3|agent-4>")
    
    asyncio.run(main(sys.argv[1]))
