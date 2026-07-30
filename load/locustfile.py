"""Bounded, SSE-aware Locust harness for GatewayLLM.

Configuration comes from ``GATEWAY_BASE_URL``, ``GATEWAY_API_KEY``, and the
optional ``GATEWAY_MODEL`` environment variable. The harness never logs API
keys, prompt bodies, model output, or raw provider errors.
"""

from __future__ import annotations

import os
import random
import time

from locust import HttpUser, between, task

from load.harness import (
    apply_stream_timing,
    configured_host,
    new_trace_id,
    observe_stream,
    valid_nonstream_response,
)

DEFAULT_MODEL = "openai/gpt-oss-20b"
PROMPTS = (
    "What is a hash table?",
    "Explain TCP briefly.",
    "What is Big O notation?",
    "Define the ACID properties.",
    "What is a mutex?",
)


class GatewayUser(HttpUser):
    """80% normalized completions and 20% fully consumed SSE streams."""

    host = configured_host()
    wait_time = between(0.5, 2.0)

    def on_start(self) -> None:
        self.api_key = os.environ["GATEWAY_API_KEY"]
        self.model = os.getenv("GATEWAY_MODEL", DEFAULT_MODEL)

    def payload(self, *, stream: bool) -> dict[str, object]:
        return {
            "model": self.model,
            "messages": [{"role": "user", "content": random.choice(PROMPTS)}],
            "max_tokens": 100,
            "stream": stream,
        }

    def headers(self, *, stream: bool) -> dict[str, str]:
        return {
            "X-API-Key": self.api_key,
            "X-Trace-ID": new_trace_id(stream=stream),
            "Content-Type": "application/json",
        }

    @task(80)
    def non_stream(self) -> None:
        with self.client.post(
            "/v1/chat/completions",
            headers=self.headers(stream=False),
            json=self.payload(stream=False),
            name="/v1/chat/completions [non-stream]",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.failure(f"HTTP {response.status_code}")
                return
            try:
                data = response.json()
            except ValueError:
                response.failure("invalid JSON response")
                return
            if not valid_nonstream_response(data):
                response.failure("invalid normalized response")
                return
            response.success()

    @task(20)
    def stream(self) -> None:
        started_at = time.perf_counter()
        with self.client.post(
            "/v1/chat/completions",
            headers=self.headers(stream=True),
            json=self.payload(stream=True),
            stream=True,
            name="/v1/chat/completions [stream]",
            catch_response=True,
        ) as response:
            if response.status_code != 200:
                response.request_meta["response_time"] = (
                    time.perf_counter() - started_at
                ) * 1000
                response.failure(f"HTTP {response.status_code}")
                return

            observation = observe_stream(
                response.iter_lines(),
                started_at=started_at,
            )
            # Locust's default stream=True timer ends after headers. Replace it
            # before the context exits so the primary stream metric ends at
            # [DONE] (or the observed protocol failure).
            apply_stream_timing(response, observation)

            if not observation.success:
                response.failure(f"SSE error: {observation.error}")
                return

            response.success()
            if observation.ttft_ms is not None:
                self.environment.events.request.fire(
                    request_type="SSE",
                    name="/v1/chat/completions [stream TTFT]",
                    response_time=observation.ttft_ms,
                    response_length=0,
                    response=None,
                    context={},
                    exception=None,
                )
