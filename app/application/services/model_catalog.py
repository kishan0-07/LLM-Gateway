from dataclasses import dataclass


@dataclass(frozen=True)
class ModelInfo:
    provider: str
    tokenizer_hint: str
    input_per_1m: float
    output_per_1m: float


_CATALOG: dict[str, ModelInfo] = {
    "openai/gpt-oss-20b": ModelInfo(provider="groq", tokenizer_hint="o200k_base", input_per_1m=0.075, output_per_1m=0.30),
    "openai/gpt-oss-120b": ModelInfo(provider="groq", tokenizer_hint="o200k_base", input_per_1m=0.15, output_per_1m=0.60),
    "gpt-5.4-mini": ModelInfo(provider="openai", tokenizer_hint="o200k_base", input_per_1m=0.75, output_per_1m=4.50),
}


def get(model: str) -> ModelInfo:
    if model not in _CATALOG:
        raise KeyError(f"model '{model}' not in ModelCatalog — add it before routing to it")
    return _CATALOG[model]


def allowed_models() -> list[str]:
    return list(_CATALOG.keys())