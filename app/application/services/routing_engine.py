from dataclasses import dataclass
from app.application.services import model_catalog
from app.infrastructure.providers.base import BaseProvider


@dataclass
class RouteCandidate:
    provider: BaseProvider
    model: str
    priority: int  # lower = tried first


class RoutingEngine:
    def __init__(self, providers: dict[str, BaseProvider]):
        self._providers = providers

    def plan(self, model: str) -> list[RouteCandidate]:
        primary_info = model_catalog.get(model)
        primary_provider_name = primary_info.provider

        candidates = []

        # Primary candidate
        if primary_provider_name in self._providers:
            candidates.append(
                RouteCandidate(
                    provider=self._providers[primary_provider_name],
                    model=model,
                    priority=0,
                )
            )

        # Fallback candidates — every other model from a different provider, sorted by cost
        fallbacks = []
        for catalog_model, info in model_catalog.all_models().items():
            if info.provider == primary_provider_name:
                continue  # skip same-provider models (if Groq 20b fails, Groq 120b probably will too)
            if info.provider not in self._providers:
                continue  # provider not wired
            cost = (
                info.input_per_1m + info.output_per_1m
            )  # rough combined cost for sorting
            fallbacks.append((cost, catalog_model, info))

        fallbacks.sort(key=lambda x: x[0])
        for i, (_, fallback_model, info) in enumerate(fallbacks):
            candidates.append(
                RouteCandidate(
                    provider=self._providers[info.provider],
                    model=fallback_model,
                    priority=i + 1,
                )
            )

        return candidates
