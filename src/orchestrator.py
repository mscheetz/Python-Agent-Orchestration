import asyncio
import json
import time
from uuid import uuid4
from confluent_kafka import Consumer, Producer
from src.config import AGENTS, COMPLETION_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from src.llm import synthesize_final_answer
from src.messages import make_task, decode, encode
from src.redis import load_json
from src.topics import agent_topic

USER_TEXT = "Kafka architecture simulating AI agents."

def dispatch_tasks(producer: Producer, conversation_id: str, user_text: str) -> dict[str, dict]:
    dispatched = {}
    goals = {
        "agent-1": "summarize the user request",
        "agent-2": "extract action items",
        "agent-3": "score risk and priority",
        "agent-4": "prepare final recommendation",
    }

    for agent_id in AGENTS:
        task = make_task(
            agent_id=agent_id,
            goal=goals[agent_id],
            payload={
                "conversation_id": conversation_id,
                "text": user_text,
            },
        )
        topic = agent_topic(agent_id)
        producer.produce(topic, key=conversation_id, value=encode(task))
        dispatched[task["task_id"]] = task
        print(f"[orchestrator] sent {task['task_id']} to {topic}")

    producer.flush()
    return dispatched

async def wait_for_completions(expected_task_ids: set[str], conversation_id: str) -> list[dict]:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"orchestrator-completion-consumer-{conversation_id}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([COMPLETION_TOPIC])

    completed = set()
    agent_outputs = []

    print(f"[orchestrator] waiting for {len(expected_task_ids)} completions on {COMPLETION_TOPIC}")

    try:
        while completed != expected_task_ids:
            msg = consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(0)
                continue

            if msg.error():
                print(f"[orchestrator] Kafka error: {msg.error()}")
                continue

            try:
                completion = decode(msg.value())
            except json.JSONDecodeError:
                print(f"[orchestrator] skipping non-JSON message: {msg.value()!r}")
                consumer.commit(msg)
                continue

            payload_conversation_id = completion.get("conversation_id")

            if payload_conversation_id != conversation_id:
                continue

            task_id = completion["task_id"]
            if task_id not in expected_task_ids or task_id in completed:
                continue

            output = load_json(completion["redis_key"])
            agent_outputs.append(output)
            completed.add(task_id)
            
            consumer.commit(msg)

            print("\n[orchestrator] completion received")
            print(f"  conversation: {payload_conversation_id}")
            print(f"  agent: {completion['agent_id']}")
            print(f"  task: {task_id}")
            print(f"  redis_key: {completion['redis_key']}")
            print(f"  output: {output['result']}")

        print("\n[orchestrator] all agents complete")
        return agent_outputs

    finally:
        consumer.close()

async def main() -> None:
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    # Small delay so all agent consumers are subscribed
    time.sleep(5)

    conversation_id = f"conversation-{uuid4()}"

    dispatched = dispatch_tasks(
        producer=producer, 
        conversation_id=conversation_id,
        user_text=USER_TEXT
    )

    agent_outputs = await wait_for_completions(
        expected_task_ids=set(dispatched.keys()),
        conversation_id=conversation_id
    )

    print(f"[orchestrator] getting final answer based on {len(agent_outputs)} agent outputs")

    final_answer = await synthesize_final_answer(
        conversation_id=conversation_id,
        user_text=USER_TEXT,
        agent_outputs=agent_outputs
    )

    print("\n[orchestrator] final synthesized answer")
    print(final_answer)

if __name__ == "__main__":
    asyncio.run(main())