import pytest
import tiktoken

from app.application.services import model_catalog


@pytest.mark.parametrize(
    "model",
    ["openai/gpt-oss-20b", "openai/gpt-oss-120b"],
)
def test_gpt_oss_models_use_harmony_tokenizer(model):
    assert model_catalog.get(model).tokenizer_hint == "o200k_harmony"
    assert tiktoken.get_encoding("o200k_harmony").name == "o200k_harmony"
