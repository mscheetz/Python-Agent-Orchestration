# Orchestration

A sample event-driven orchestration framework demonstrating how a controller coordinates multiple AI agents using **Apache Kafka** for messaging, **Redis** for shared state, and **FastAPI** as the public API.

## Architecture

The application consists of:

* **Controller (FastAPI)**

  * Exposes a REST API
  * Accepts user requests
  * Invokes the orchestration workflow
  * Returns the synthesized response

* **Orchestrator**

  * Creates work items
  * Publishes tasks to Kafka (one topic per agent)
  * Waits for agent completion events
  * Retrieves agent outputs from Redis
  * Synthesizes a final response using the LLM

* **4 Agents**

  * Consume tasks from their dedicated Kafka topic
  * Execute a specialized AI role
  * Store results in Redis
  * Publish completion events

* **Kafka**

  * Event bus for asynchronous communication
  * One task topic per agent
  * Shared completion topic

* **Redis**

  * Shared state store
  * Persists agent outputs

---

# Architecture

```text
                HTTP POST
                    |
                    v
             +----------------+
             |   Controller   |
             |    FastAPI     |
             +----------------+
                    |
                    v
             +----------------+
             |  Orchestrator  |
             +----------------+
                    |
      +-------------+-------------+-------------+
      |             |             |             |
      v             v             v             v 
agent-1       agent-2       agent-3       agent-4
  topic          topic          topic         topic
      |             |             |             |
      v             v             v             v
 +---------+   +---------+   +---------+   +---------+
 | Agent 1 |   | Agent 2 |   | Agent 3 |   | Agent 4 |
 +---------+   +---------+   +---------+   +---------+
      |             |             |             |
      +-------------+-------------+-------------+
                    |
               Store Results
                  in Redis
                    |
                    v
          agent-completions topic
                    |
                    v
             +----------------+
             |  Orchestrator  |
             +----------------+
                    |
             Final LLM Synthesis
                    |
                    v
             HTTP Response
```

---

# Agent Responsibilities

| Agent   | Responsibility               |
| ------- | ---------------------------- |
| Agent 1 | Summarize the user request   |
| Agent 2 | Extract action items         |
| Agent 3 | Analyze risks and priorities |
| Agent 4 | Produce recommendations      |

---

# Kafka Topics

| Topic               | Producer     | Consumer     |
| ------------------- | ------------ | ------------ |
| `agent-1-tasks`     | Orchestrator | Agent 1      |
| `agent-2-tasks`     | Orchestrator | Agent 2      |
| `agent-3-tasks`     | Orchestrator | Agent 3      |
| `agent-4-tasks`     | Orchestrator | Agent 4      |
| `agent-completions` | Agents       | Orchestrator |

---

# API

## Health

```http
GET /health
```

Response

```json
{
  "status": "ok"
}
```

---

## Conversation

```http
POST /api/orchestrator
Content-Type: application/json
```

Body

```json
{
  "text": "Kafka architecture simulating AI agents."
}
```

Example using curl

```bash
curl -X POST http://localhost:8000/api/orchestrator \
  -H "Content-Type: application/json" \
  -d '{
        "text":"Kafka architecture simulating AI agents."
      }'
```

Example response

```json
{
  "answer": "..."
}
```

---

# Setup

Create a virtual environment

```bash
python3 -m venv env
source env/bin/activate
```

Install dependencies

```bash
pip install -r requirements.txt
```

---

# OpenRouter

Create `.env` from `.env.sample`.

Create an API key on OpenRouter.

Populate:

```text
OPENROUTER_API_KEY=...
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_MODEL=deepseek/deepseek-v4-flash
```

---

# Running

Build

```bash
docker compose build
```

Start

```bash
docker compose up
```

Rebuild

```bash
docker compose up --build
```

Detached

```bash
docker compose up -d
```

---

# Example Workflow

```text
Client
  |
POST /conversation
  |
Controller
  |
Orchestrator
  |
Dispatch 4 Kafka Tasks
  |
+----------+----------+----------+----------+
|          |          |          |
Agent 1    Agent 2    Agent 3    Agent 4
|          |          |          |
Redis      Redis      Redis      Redis
 \          |          |         /
  \         |          |        /
   +--------+----------+-------+
            |
agent-completions
            |
Orchestrator
            |
LLM synthesis
            |
HTTP response
```

---

# Redis

Each agent stores its output under a Redis key.

Example:

```text
agents:agent-1:tasks:<task-id>:output
```

Value

```json
{
  "task_id": "...",
  "agent_id": "agent-1",
  "goal": "summarize the user request",
  "result": "...",
  "generated_at": "..."
}
```

Inspect Redis

```bash
docker exec -it redis-box redis-cli
```

List keys

```redis
KEYS *
```

Retrieve a value

```redis
GET agents:agent-1:tasks:<task-id>:output
```

---

# Development Notes

The project uses:

* Apache Kafka for asynchronous agent communication
* Redis for shared state
* FastAPI for the REST interface
* OpenRouter as the LLM provider
* Docker Compose for local development

For this project, all services currently run using Docker host networking because the development machine exhibited Docker bridge networking issues affecting TLS traffic to OpenRouter. Using host networking provides reliable communication between Kafka, Redis, the controller, and the agents during local development.

---

# Stopping

Stop all services

```bash
docker compose down
```

Remove containers, networks, and volumes

```bash
docker compose down -v
```
