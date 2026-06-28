# Orchestration

A sample event-driven orchestration framework demonstrating how an orchestrator coordinates multiple AI agents using **Apache Kafka** for messaging and **Redis** for shared state.

## Architecture

The application consists of:

* **Orchestrator**

  * Creates work items
  * Publishes tasks to Kafka (one topic per agent)
  * Waits for agent completion events
  * Tracks overall workflow completion

* **4 Agents**

  * Consume tasks from their dedicated Kafka topic
  * Simulate AI processing
  * Store results in Redis
  * Publish a completion event back to the orchestrator

* **Kafka**

  * Message broker for asynchronous communication
  * One task topic per agent
  * Shared completion topic for agent status

* **Redis**

  * Stores each agent's output
  * Serves as a simple shared data store

## Message Flow

```text
                 +----------------+
                 |  Orchestrator  |
                 +----------------+
                    |    |    |    |
                    |    |    |    |
        ------------     |     -------------
       |                 |                 |
agent-1-tasks   agent-2-tasks   agent-3-tasks   agent-4-tasks
       |                 |                 |
   +--------+       +--------+       +--------+       +--------+
   |Agent 1 |       |Agent 2 |       |Agent 3 |       |Agent 4 |
   +--------+       +--------+       +--------+       +--------+
       |                 |                 |               |
       +------- Save Results to Redis -----+---------------+
       |                 |                 |               |
       +-----------------------------------------------+
                       agent-completions
                              |
                              v
                      +----------------+
                      |  Orchestrator  |
                      +----------------+
```

## Topics

| Topic               | Producer     | Consumer     |
| ------------------- | ------------ | ------------ |
| `agent-1-tasks`     | Orchestrator | Agent 1      |
| `agent-2-tasks`     | Orchestrator | Agent 2      |
| `agent-3-tasks`     | Orchestrator | Agent 3      |
| `agent-4-tasks`     | Orchestrator | Agent 4      |
| `agent-completions` | All Agents   | Orchestrator |

## Setup

Create and activate a virtual environment:

```bash
python3 -m venv env
source env/bin/activate
```

## Start the Environment

Build the Docker images:

```bash
docker compose build
```

Start Kafka, Redis, the orchestrator, and all agents:

```bash
docker compose up
```

Or rebuild and start everything:

```bash
docker compose up --build
```

Run in detached mode:

```bash
docker compose up -d
```

## Expected Output

When running successfully, the logs will show the workflow progressing through each stage:

```text
[orchestrator] Sent task to agent-1
[orchestrator] Sent task to agent-2
[orchestrator] Sent task to agent-3
[orchestrator] Sent task to agent-4

[agent-1] Processing task...
[agent-2] Processing task...
[agent-3] Processing task...
[agent-4] Processing task...

[agent-1] Saved result to Redis
[agent-1] Published completion

...

[orchestrator] Received completion from agent-4
[orchestrator] Workflow complete
```

## Redis

Each agent stores its processed output in Redis using the task ID as the key.

Example:

```text
task:12345 -> {
    "agent": "agent-1",
    "result": "Processed output"
}
```

## Stopping

Stop all services and remove containers, networks, and volumes:

```bash
docker compose down -v
```
