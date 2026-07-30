import tiktoken
from app.application.services import model_catalog

DEFAULT_MAX_OUTPUT_TOKENS = 8192


class TokenEstimator:
    def __init__(self):
        self._tokenizer_cache: dict[str, tiktoken.Encoding] = {}

    @staticmethod
    def _encode_user_text(
        encoder: tiktoken.Encoding,
        text: str,
    ) -> list[int]:
        """Treat provider control-token spellings as untrusted literal text."""
        return encoder.encode(text, disallowed_special=())

    def _get_encoder(self, tokenizer_hint: str) -> tiktoken.Encoding:
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
                    num_tokens += len(self._encode_user_text(encoder, value))
                if key == "name":
                    num_tokens += 1

        num_tokens += 3
        return num_tokens

    def output_cap(
        self, messages: list[dict], model: str, requested: int | None
    ) -> int:
        info = model_catalog.get(model)
        input_tokens = self.estimate_input_tokens(messages, model)
        remaining_context = max(0, info.context_limit - input_tokens)
        requested_or_default = (
            requested if requested is not None else DEFAULT_MAX_OUTPUT_TOKENS
        )
        # Cap against requested, remaining context, AND the model's max output limit
        cap = min(requested_or_default, remaining_context, info.max_output_tokens)
        if cap < 1:
            raise ValueError("input exceeds the model context window")
        return cap

    def count_output_tokens(self, *, text: str, model: str) -> int:
        tokenizer_hint = model_catalog.get(model).tokenizer_hint
        encoder = self._get_encoder(tokenizer_hint)
        return len(self._encode_user_text(encoder, text))

    def estimate_output_tokens_for_text(self, *, text: str, model: str) -> int:
        """Compatibility wrapper for existing accounting code."""
        return self.count_output_tokens(text=text, model=model)

    def truncate_output_text(
        self,
        *,
        text: str,
        model: str,
        max_tokens: int,
    ) -> tuple[str, int, bool]:
        """Return a UTF-8-safe prefix, retained token count, and truncation flag."""
        if max_tokens < 1:
            raise ValueError("max_tokens must be positive")

        tokenizer_hint = model_catalog.get(model).tokenizer_hint
        encoder = self._get_encoder(tokenizer_hint)
        token_ids = self._encode_user_text(encoder, text)

        if len(token_ids) <= max_tokens:
            return text, len(token_ids), False

        bounded_bytes = encoder.decode_bytes(token_ids[:max_tokens])
        bounded_text = bounded_bytes.decode("utf-8", errors="ignore")
        retained_tokens = len(self._encode_user_text(encoder, bounded_text))

        return bounded_text, retained_tokens, True
