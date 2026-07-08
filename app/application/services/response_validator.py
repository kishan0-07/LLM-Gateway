class ResponseValidator:
    def is_valid(self, content: str | None) -> bool:
        if content is None:
            return False
        stripped = content.strip()
        if len(stripped) < 2:
            return False
        return True