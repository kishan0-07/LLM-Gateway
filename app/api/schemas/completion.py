from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: str = Field(..., examples=["user"])
    content: str = Field(..., examples=["Explain token budgeting in 3 sentences."])
    name: str | None = None


class CompletionCreateRequest(BaseModel):
    model_config = {"extra": "forbid"}
    model: str = Field(..., examples=["openai/gpt-oss-20b"])
    messages: list[ChatMessage] = Field(..., min_length=1)
    max_tokens: int | None = Field(None, ge=1, le=131_072)
    stream: bool = False  # parsed here, routed to StreamCompletion on Days 8-9

    


class UsageResponse(BaseModel):
    input_tokens: int
    output_tokens: int
    cost_usd: str  # string to avoid floating-point display drift in JSON


class CompletionCreateResponse(BaseModel):
    gateway_request_id: int
    content: str
    provider: str
    model: str
    usage: UsageResponse

class StreamChunk(BaseModel):
    type: str  # "delta", "usage", "error", "done"
    content: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost_usd: str | None = None  # only on the final "usage" chunk