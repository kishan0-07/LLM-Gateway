import json
import os
import time
import uuid

import httpx


def main() -> None:
    api_key = os.environ["GATEWAY_CHAOS_API_KEY"]
    model = os.getenv("GATEWAY_CHAOS_MODEL", "openai/gpt-oss-20b")
    trace_id = f"day20-sse-{uuid.uuid4().hex}"
    started = time.perf_counter()
    first_delta_at: float | None = None
    done_at: float | None = None
    delta_count = 0
    content_type = ""
    accel_header = ""

    with httpx.stream(
        "POST",
        "http://127.0.0.1/v1/chat/completions",
        headers={
            "X-API-Key": api_key,
            "X-Trace-ID": trace_id,
        },
        json={
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": (
                        "Explain five practical rules for reliable API design "
                        "in about 300 words."
                    ),
                }
            ],
            "max_tokens": 512,
            "stream": True,
        },
        timeout=httpx.Timeout(90.0, read=90.0),
    ) as response:
        if response.status_code != 200:
            raise AssertionError(response.read().decode())

        content_type = response.headers.get("content-type", "")
        accel_header = response.headers.get("x-accel-buffering", "")
        assert content_type.startswith("text/event-stream"), content_type
        assert accel_header.lower() == "no", response.headers
        assert response.headers["x-trace-id"] == trace_id

        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            data = line[6:]
            now = time.perf_counter()
            if data == "[DONE]":
                done_at = now
                break
            event = json.loads(data)
            if event.get("type") == "error":
                raise AssertionError(
                    {
                        "trace_id": trace_id,
                        "delta_count": delta_count,
                        "event": event,
                    }
                )
            if event.get("type") == "delta" and first_delta_at is None:
                first_delta_at = now
            if event.get("type") == "delta":
                delta_count += 1

    assert first_delta_at is not None, "no delta arrived through Nginx"
    assert done_at is not None, "[DONE] was not received"
    assert first_delta_at < done_at, "stream was not observed incrementally"

    print(
        json.dumps(
            {
                "trace_id": trace_id,
                "content_type": content_type,
                "x_accel_buffering": accel_header,
                "first_delta_ms": int((first_delta_at - started) * 1000),
                "total_ms": int((done_at - started) * 1000),
                "delta_count": delta_count,
            }
        )
    )


if __name__ == "__main__":
    main()
