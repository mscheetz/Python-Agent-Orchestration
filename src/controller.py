from pydantic import BaseModel
from fastapi import FastAPI
from src.orchestrator import run_conversation

app = FastAPI()


class ConversationRequest(BaseModel):
    text: str


class ConversationResponse(BaseModel):
    answer: str


@app.post("/conversation", response_model=ConversationResponse)
async def conversation(req: ConversationRequest):
    print(f"New request from controller")
    
    answer = await run_conversation(req.text)
    return ConversationResponse(answer=answer)


@app.get("/health")
async def health():
    return {"status": "ok"}