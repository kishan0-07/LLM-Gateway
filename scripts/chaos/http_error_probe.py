import argparse
import json
import os
import uuid

import httpx


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1/v1/chat/completions")
    parser.add_argument("--expected-status", type=int, required=True)
    parser.add_argument("--expected-code", required=True)
    parser.add_argument("--trace-id")
    args = parser.parse_args()

    api_key = os.environ["GATEWAY_CHAOS_API_KEY"]
    model = os.getenv("GATEWAY_CHAOS_MODEL", "openai/gpt-oss-20b")
    trace_id = args.trace_id or f"day20-{uuid.uuid4().hex}"
    response = httpx.post(
        args.url,
        headers={
            "X-API-Key": api_key,
            "X-Trace-ID": trace_id,
        },
        json={
            "model": model,
            "messages": [{"role": "user", "content": "Reply with OK."}],
            "max_tokens": 8,
            "stream": False,
        },
        timeout=15.0,
    )

    payload = response.json()
    assert response.status_code == args.expected_status, response.text
    assert payload["error"]["code"] == args.expected_code, payload
    assert payload["error"]["trace_id"] == trace_id, payload
    assert response.headers["x-trace-id"] == trace_id

    print(
        json.dumps(
            {
                "trace_id": trace_id,
                "status": response.status_code,
                "error_code": payload["error"]["code"],
            }
        )
    )


if __name__ == "__main__":
    main()
