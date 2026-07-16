import pytest
from unittest.mock import MagicMock
from app.application.services.routing_engine import RoutingEngine
from app.infrastructure.providers.base import ProviderMetadata


def _make_provider(name: str) -> MagicMock:
    """Create a mock provider with proper metadata."""
    provider = MagicMock()
    provider.metadata = ProviderMetadata(
        name=name,
        models=[],
        supports_streaming_usage=True,
        tokenizer_hint="mock",
        pricing={},
    )
    return provider


def test_plan_returns_primary_first():
    """The primary provider for gpt-5.4-mini is openai; it must be the first candidate."""
    openai = _make_provider("openai")
    groq = _make_provider("groq")
    engine = RoutingEngine(providers={"openai": openai, "groq": groq})

    candidates = engine.plan("gpt-5.4-mini")
    assert len(candidates) >= 1
    assert candidates[0].provider is openai
    assert candidates[0].model == "gpt-5.4-mini"
    assert candidates[0].priority == 0


def test_plan_includes_fallbacks_from_different_provider():
    """Fallbacks must come from a different provider than the primary."""
    openai = _make_provider("openai")
    groq = _make_provider("groq")
    engine = RoutingEngine(providers={"openai": openai, "groq": groq})

    candidates = engine.plan("gpt-5.4-mini")
    fallbacks = [c for c in candidates if c.priority > 0]
    assert len(fallbacks) >= 1
    # All fallbacks must be from groq (since primary is openai)
    for fb in fallbacks:
        assert fb.provider is groq


def test_plan_groq_model_primary_is_groq():
    openai = _make_provider("openai")
    groq = _make_provider("groq")
    engine = RoutingEngine(providers={"openai": openai, "groq": groq})

    candidates = engine.plan("openai/gpt-oss-20b")
    assert candidates[0].provider is groq
    assert candidates[0].model == "openai/gpt-oss-20b"


def test_plan_fallbacks_sorted_by_cost():
    """Fallback candidates must be sorted by combined cost (cheapest first)."""
    openai = _make_provider("openai")
    groq = _make_provider("groq")
    engine = RoutingEngine(providers={"openai": openai, "groq": groq})

    candidates = engine.plan("gpt-5.4-mini")
    fallbacks = [c for c in candidates if c.priority > 0]

    if len(fallbacks) >= 2:
        # Verify ascending priority order
        for i in range(len(fallbacks) - 1):
            assert fallbacks[i].priority < fallbacks[i + 1].priority


def test_plan_unknown_model_raises_key_error():
    """Requesting a model not in the catalog must raise KeyError."""
    engine = RoutingEngine(providers={"openai": _make_provider("openai")})
    with pytest.raises(KeyError, match="not-a-real-model"):
        engine.plan("not-a-real-model")


def test_plan_missing_provider_skips_primary():

    groq = _make_provider("groq")
    # Only groq is wired, but gpt-5.4-mini needs openai as primary
    engine = RoutingEngine(providers={"groq": groq})

    candidates = engine.plan("gpt-5.4-mini")
    # Primary (openai) is missing, but groq models should be fallbacks
    assert len(candidates) >= 1
    for c in candidates:
        assert c.provider is groq