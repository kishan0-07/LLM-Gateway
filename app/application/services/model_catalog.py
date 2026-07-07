from dataclasses import dataclass


@dataclass(frozen=True)
class ModelInfo:
    provider: str
    tokenizer_hint: str
    input_per_1m: float
    output_per_1m: float
    context_limit: int

_CATALOG: dict[str, ModelInfo] = {
    "openai/gpt-oss-20b": ModelInfo("groq", "o200k_harmony", 0.075, 0.30, context_limit=131_072),
    "openai/gpt-oss-120b": ModelInfo("groq", "o200k_harmony", 0.15, 0.60, context_limit=131_072),
    "gpt-5.4-mini": ModelInfo("openai", "o200k_base", 0.75, 4.50, context_limit=400_000), 
}


def get(model: str) -> ModelInfo:
    if model not in _CATALOG:
        raise KeyError(f"model '{model}' not in ModelCatalog — add it before routing to it")
    return _CATALOG[model]


def estimate_cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    info = get(model)
    return (input_tokens / 1_000_000) * info.input_per_1m + (output_tokens / 1_000_000) * info.output_per_1m