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
        ------------     |     -------------------------
       |                 |                 |           | 
agent-1-tasks   agent-2-tasks   agent-3-tasks   agent-4-tasks
       |                 |                 |           | 
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
agent-1-1  | [agent-1 listening on agent-1-tasks]
agent-2-1  | [agent-2 listening on agent-2-tasks]
agent-3-1  | [agent-3 listening on agent-3-tasks]
agent-4-1  | [agent-4 listening on agent-4-tasks]

orchestrator-1  | [orchestrator] sent be5b1841-345a-4f48-8b7a-9cfd8eba597b to agent-1-tasks
orchestrator-1  | [orchestrator] sent da0dbe7c-cada-479b-abf1-74f98d345567 to agent-2-tasks
orchestrator-1  | [orchestrator] sent 824a71a1-8cff-4278-8be9-665fde54a688 to agent-3-tasks
orchestrator-1  | [orchestrator] sent 57e2b778-3826-4284-9cad-b431725f6627 to agent-4-tasks
orchestrator-1  | [orchestrator] waiting for 4 completions on agent-completions

agent-1-1       | [agent-1] received task be5b1841-345a-4f48-8b7a-9cfd8eba597b on conversation conversation-001
agent-2-1       | [agent-2] received task da0dbe7c-cada-479b-abf1-74f98d345567 on conversation conversation-001
agent-3-1       | [agent-3] received task 824a71a1-8cff-4278-8be9-665fde54a688 on conversation conversation-001
agent-4-1       | [agent-4] received task 57e2b778-3826-4284-9cad-b431725f6627 on conversation conversation-001

...

agent-1-1       | [agent-1] saved output to Redis at agents:agent-1:tasks:be5b1841-345a-4f48-8b7a-9cfd8eba597b:output; completion sent
agent-2-1       | [agent-2] saved output to Redis at agents:agent-2:tasks:da0dbe7c-cada-479b-abf1-74f98d345567:output; completion sent
agent-3-1       | [agent-3] saved output to Redis at agents:agent-3:tasks:824a71a1-8cff-4278-8be9-665fde54a688:output; completion sent
agent-4-1       | [agent-4] saved output to Redis at agents:agent-4:tasks:57e2b778-3826-4284-9cad-b431725f6627:output; completion sent

...

orchestrator-1  | [orchestrator] completion received
orchestrator-1  |   conversation: conversation-001
orchestrator-1  |   agent: agent-3
orchestrator-1  |   task: 824a71a1-8cff-4278-8be9-665fde54a688
orchestrator-1  |   redis_key: agents:agent-3:tasks:824a71a1-8cff-4278-8be9-665fde54a688:output
orchestrator-1  |   output: agent-3 finished: score risk and priority | confidence=0.94
orchestrator-1  | 
orchestrator-1  | [orchestrator] all agents complete
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
