import json
import os
import uuid

import httpx


def main() -> None:
    api_key = os.environ["GATEWAY_CHAOS_API_KEY"]
    model = os.getenv("GATEWAY_CHAOS_MODEL", "openai/gpt-oss-20b")
    trace_id = f"day20-disconnect-{uuid.uuid4().hex}"
    saw_delta = False

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
                        "Write a detailed 1,000-word explanation of database "
                        "transactions and isolation levels."
                    ),
                }
            ],
            "max_tokens": 1024,
            "stream": True,
        },
        timeout=httpx.Timeout(90.0, read=90.0),
    ) as response:
        if response.status_code != 200:
            raise AssertionError(response.read().decode())

        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            data = line[6:]
            if data == "[DONE]":
                raise AssertionError(
                    "provider completed before disconnect; rerun with a longer prompt"
                )
            event = json.loads(data)
            if event.get("type") == "error":
                raise AssertionError(event)
            if event.get("type") == "delta":
                saw_delta = True
                break

    assert saw_delta, "no delta arrived before disconnect"
    print(json.dumps({"trace_id": trace_id, "disconnected_after_delta": True}))


if __name__ == "__main__":
    main()
