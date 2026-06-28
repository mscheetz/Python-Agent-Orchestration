import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

AGENTS = ["agent-1", "agent-2", "agent-3", "agent-4"]
COMPLETION_TOPIC = "agent-completions"