import time
from confluent_kafka import Consumer, Producer
from src.config import AGENTS, COMPLETION_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from src.messages import make_task, decode, encode
from src.redis import load_json
from src.topics import agent_topic

def dispatch_tasks(producer: Producer) -> dict[str, dict]:
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
                "conversation_id": "demo-conversation-001",
                "text": "Customer asks for a Kafka architecture simulating AI agents.",
            },
        )
        topic = agent_topic(agent_id)
        producer.produce(topic, key=task["task_id"], value=encode(task))
        dispatched[task["task_id"]] = task
        print(f"[orchestrator] sent {task['task_id']} to {topic}")

    producer.flush()
    return dispatched

def wait_for_completions(expected_task_ids: set[str]) -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "orchestrator-completion-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([COMPLETION_TOPIC])

    completed = set()
    print(f"[orchestrator] waiting for {len(expected_task_ids)} completions on {COMPLETION_TOPIC}")

    try:
        while completed != expected_task_ids:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[orchestrator] Kafka error: {msg.error()}")
                continue

            completion = decode(msg.value())
            task_id = completion["task_id"]
            if task_id not in expected_task_ids or task_id in completed:
                continue

            completed.add(task_id)
            output = load_json(completion["redis_key"])
            print("\n[orchestrator] completion received")
            print(f"  agent: {completion['agent_id']}")
            print(f"  task: {task_id}")
            print(f"  redis_key: {completion['redis_key']}")
            print(f"  output: {output['result']} | confidence={output['confidence']}")

        print("\n[orchestrator] all agents complete")
    finally:
        consumer.close()


def main() -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    # Small delay so all agent consumers are subscribed
    time.sleep(5)
    dispatched = dispatch_tasks(producer)
    wait_for_completions(set(dispatched.keys()))


if __name__ == "__main__":
    main()