import time
from dotenv import load_dotenv
from groq import Groq
import os

load_dotenv()

api_key = os.getenv("GROQ_API_KEY")

if not api_key:
    raise RuntimeError("GROQ_API_KEY not found in .env")

client = Groq(api_key=api_key)

MODELS = [
    "openai/gpt-oss-20b",
    "openai/gpt-oss-120b",
]

PROMPT = "explain resursion in DSA"

for model in MODELS:
    print(f"\n{'=' * 70}")
    print(f"Testing {model}")
    print("=" * 70)

    try:
        start = time.perf_counter()

        stream = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": PROMPT,
                }
            ],
            stream=True,
        )

        full_response = ""
        usage = None

        for chunk in stream:
            if (
                chunk.choices
                and chunk.choices[0].delta
                and chunk.choices[0].delta.content
            ):
                token = chunk.choices[0].delta.content
                full_response += token
                print(token, end="", flush=True)

            if chunk.usage:
                usage = chunk.usage

        elapsed = time.perf_counter() - start

        print("\n")

        if usage:
            print("Statistics")
            print(f"Elapsed Time      : {elapsed:.2f} seconds")
            print(f"Prompt Tokens    : {usage.prompt_tokens}")
            print(f"Completion Tokens: {usage.completion_tokens}")
            print(f"Total Tokens     : {usage.total_tokens}")
            print(f"ALL : {usage}")
        else:
            print("Usage statistics were not returned.")

    except Exception as e:
        print(f"\nERROR ({model})")
        print(type(e).__name__)
        print(e)
