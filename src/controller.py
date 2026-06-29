from pydantic import BaseModel
from fastapi import FastAPI
from src.orchestrator import run_conversation

app = FastAPI()


class OrchestrationRequest(BaseModel):
    text: str


class OrchestrationResponse(BaseModel):
    answer: str

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.post("/api/orchestrator", response_model=OrchestrationResponse)
async def conversation(req: OrchestrationRequest):
    print(f"New request from controller")

    answer = await run_conversation(req.text)
    return OrchestrationResponse(answer=answer)
