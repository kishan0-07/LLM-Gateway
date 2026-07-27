import pytest

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
