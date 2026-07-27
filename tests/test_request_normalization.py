from app.api.routes.completions import _provider_messages
from app.api.schemas.completion import CompletionCreateRequest


def test_absent_optional_message_name_is_not_sent_to_providers() -> None:
    body = CompletionCreateRequest.model_validate(
        {
            "model": "openai/gpt-oss-20b",
            "messages": [{"role": "user", "content": "hello"}],
        }
    )

    assert _provider_messages(body) == [{"role": "user", "content": "hello"}]


def test_present_message_name_is_preserved() -> None:
    body = CompletionCreateRequest.model_validate(
        {
            "model": "openai/gpt-oss-20b",
            "messages": [
                {
                    "role": "user",
                    "content": "hello",
                    "name": "caller",
                }
            ],
        }
    )

    assert _provider_messages(body) == [
        {
            "role": "user",
            "content": "hello",
            "name": "caller",
        }
    ]
