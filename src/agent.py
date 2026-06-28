from confluent_kafka import Consumer, Producer

import random
import sys
import time

from src.config import COMPLETION_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from src.topics import agent_topic
from src.redis import save_agent_output
from src.messages import make_completion, encode, decode, utc_now

def simulate_ai_agent(task: dict) -> dict:
    """Simulated LLM/agent toolchain call."""
    time.sleep(random.uniform(0.3, 1.2))
    return {
        "task_id": task["task_id"],
        "agent_id": task["agent_id"],
        "goal": task["goal"],
        "input_payload": task["payload"],
        "result": f"{task['agent_id']} finished: {task['goal']}",
        "confidence": round(random.uniform(0.75, 0.99), 2),
        "generated_at": utc_now(),
    }

def main(agent_id: str) -> None:
    topic = agent_topic(agent_id)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"{agent_id}-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([topic])
    print(f"[{agent_id} listening on {topic}]")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"[{agent_id}] Kafka error: {msg.error()}")

            task = decode(msg.value())

            print(f"[{agent_id}] received task {task['task_id']}")

            output = simulate_ai_agent(task)
            redis_key = save_agent_output(agent_id, task["task_id"], output)

            completion = make_completion(task, redis_key)
            producer.produce(COMPLETION_TOPIC, key=task["task_id"], value=encode(completion))
            producer.flush()
            consumer.commit(msg)

            print(f"[{agent_id}] saved output to Redis at {redis_key}; completion sent")

    finally:
        consumer.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m app.agent <agent-1|agent-2|agent-3|agent-4>")
    main(sys.argv[1])
