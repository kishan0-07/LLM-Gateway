import tiktoken
from app.application.services import model_catalog


class TokenEstimator:
    def __init__(self):
        self._tokenizer_cache = {}

    def _get_encoder(self, tokenizer_hint: str):
        if tokenizer_hint not in self._tokenizer_cache:
            try:
                self._tokenizer_cache[tokenizer_hint] = tiktoken.get_encoding(tokenizer_hint)
            except ValueError:
                self._tokenizer_cache[tokenizer_hint] = tiktoken.get_encoding("cl100k_base")
        return self._tokenizer_cache[tokenizer_hint]

    def estimate_input_tokens(self, messages: list[dict], model: str) -> int:
        model_info = model_catalog.get(model)
        encoder = self._get_encoder(model_info.tokenizer_hint)

        num_tokens = 0
        for message in messages:
            num_tokens += 4
            for key, value in message.items():
                if isinstance(value, str):
                    num_tokens += len(encoder.encode(value))
                if key == "name":
                    num_tokens += 1

        num_tokens += 3
        return num_tokens

    def estimate_max_output_tokens(self, model: str, requested_max_tokens: int | None) -> int:
        info = model_catalog.get(model)
        if requested_max_tokens is not None:
            return min(requested_max_tokens, info.context_limit)
        return min(1024, info.context_limit)