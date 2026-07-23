import tiktoken
from app.application.services import model_catalog

DEFAULT_MAX_OUTPUT_TOKENS = 8192


class TokenEstimator:
    def __init__(self):
        self._tokenizer_cache = {}

    def _get_encoder(self, tokenizer_hint: str):
        if tokenizer_hint not in self._tokenizer_cache:
            try:
                self._tokenizer_cache[tokenizer_hint] = tiktoken.get_encoding(
                    tokenizer_hint
                )
            except ValueError:
                self._tokenizer_cache[tokenizer_hint] = tiktoken.get_encoding(
                    "cl100k_base"
                )
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

    def estimate_max_output_tokens(
        self, model: str, requested_max_tokens: int | None
    ) -> int:
        info = model_catalog.get(model)
        if requested_max_tokens is not None:
            return min(requested_max_tokens, info.context_limit)
        return min(1024, info.context_limit)

    def output_cap(
        self, messages: list[dict], model: str, requested: int | None
    ) -> int:
        info = model_catalog.get(model)
        input_tokens = self.estimate_input_tokens(messages, model)
        remaining_context = max(0, info.context_limit - input_tokens)
        requested_or_default = (
            requested if requested is not None else DEFAULT_MAX_OUTPUT_TOKENS
        )
        cap = min(requested_or_default, remaining_context)
        if cap < 1:
            raise ValueError("input exceeds the model context window")
        return cap

    def estimate_output_tokens_for_text(self, *, text: str, model: str) -> int:
        """Encode the complete accumulated text and return exact token count."""
        tokenizer_hint = model_catalog.get(model).tokenizer_hint
        return len(self._get_encoder(tokenizer_hint).encode(text))
