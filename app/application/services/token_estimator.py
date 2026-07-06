from app.application.services import model_catalog


class TokenEstimator:
    def estimate_input_tokens(self, messages: list[dict]) -> int:
        text = " ".join(m.get("content", "") for m in messages)
        return max(1, len(text) // 4)  # chars/4 heuristic — swap for a real tokenizer per tokenizer_hint when accuracy matters more than speed

    def estimate_max_output_tokens(self, model: str, requested_max_tokens: int | None) -> int:
        info = model_catalog.get(model)
        if requested_max_tokens is not None:
            return min(requested_max_tokens, info.context_limit)
        return min(1024, info.context_limit)