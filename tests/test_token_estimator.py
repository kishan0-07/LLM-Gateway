import pytest

from app.application.services import model_catalog
from app.application.services.token_estimator import TokenEstimator


@pytest.mark.parametrize(
    "model",
    ["openai/gpt-oss-20b", "openai/gpt-oss-120b"],
)
def test_harmony_control_token_text_is_counted_as_literal_text(model: str) -> None:
    estimator = TokenEstimator()
    text = "A user typed this literal sequence: <|message|>"

    input_tokens = estimator.estimate_input_tokens(
        [{"role": "user", "content": text}],
        model,
    )
    output_tokens = estimator.estimate_output_tokens_for_text(
        text=text,
        model=model,
    )

    assert input_tokens > 0
    assert output_tokens > 0


def test_count_output_tokens_matches_existing_accounting_method() -> None:
    estimator = TokenEstimator()
    text = "Count this output exactly."
    model = "gpt-5.4-mini"

    assert estimator.count_output_tokens(
        text=text,
        model=model,
    ) == estimator.estimate_output_tokens_for_text(
        text=text,
        model=model,
    )


def test_truncate_below_cap_preserves_text() -> None:
    estimator = TokenEstimator()
    text, count, truncated = estimator.truncate_output_text(
        text="hello",
        model="gpt-5.4-mini",
        max_tokens=100,
    )

    assert text == "hello"
    assert count > 0
    assert truncated is False


def test_truncate_exactly_at_cap_is_not_truncated() -> None:
    estimator = TokenEstimator()
    model = "gpt-5.4-mini"
    original = "bounded output"
    cap = estimator.count_output_tokens(text=original, model=model)

    text, count, truncated = estimator.truncate_output_text(
        text=original,
        model=model,
        max_tokens=cap,
    )

    assert text == original
    assert count == cap
    assert truncated is False


def test_truncate_known_token_ids_above_cap() -> None:
    estimator = TokenEstimator()
    model = "gpt-5.4-mini"
    encoder = estimator._get_encoder(model_catalog.get(model).tokenizer_hint)
    token_ids = encoder.encode(
        "one two three four five six seven eight",
        disallowed_special=(),
    )
    assert len(token_ids) > 3
    source = encoder.decode(token_ids)

    text, count, truncated = estimator.truncate_output_text(
        text=source,
        model=model,
        max_tokens=3,
    )

    assert truncated is True
    assert count <= 3
    assert estimator.count_output_tokens(text=text, model=model) == count


@pytest.mark.parametrize("max_tokens", [0, -1])
def test_truncate_rejects_nonpositive_cap(max_tokens: int) -> None:
    estimator = TokenEstimator()

    with pytest.raises(ValueError, match="max_tokens must be positive"):
        estimator.truncate_output_text(
            text="hello",
            model="gpt-5.4-mini",
            max_tokens=max_tokens,
        )


def test_truncate_emoji_boundary_is_valid_utf8() -> None:
    estimator = TokenEstimator()

    text, count, truncated = estimator.truncate_output_text(
        text="🎉" * 50,
        model="gpt-5.4-mini",
        max_tokens=5,
    )

    assert truncated is True
    assert count <= 5
    assert "\ufffd" not in text
    text.encode("utf-8", errors="strict")


def test_harmony_control_token_spelling_remains_literal_on_truncation() -> None:
    estimator = TokenEstimator()
    text = "<|start|>user<|message|>hello"

    count = estimator.count_output_tokens(
        text=text,
        model="openai/gpt-oss-20b",
    )
    bounded, retained, _ = estimator.truncate_output_text(
        text=text,
        model="openai/gpt-oss-20b",
        max_tokens=max(1, count - 1),
    )

    assert count > 0
    assert retained <= max(1, count - 1)
    assert "\ufffd" not in bounded
