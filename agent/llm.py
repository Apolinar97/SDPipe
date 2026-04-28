import os
from typing import Protocol, runtime_checkable

from openai import OpenAI


@runtime_checkable
class LLMClient(Protocol):
    def generate(self, system: str, user_message: str) -> str: ...


class OpenAIClient:
    def __init__(self, model: str):
        self.model = model
        self.client = OpenAI(
            api_key=os.getenv("LLM_API_KEY"),
        )

    def generate(self, system: str, user_message: str) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_message},
            ],
            reasoning_effort="medium",
        )
        content = response.choices[0].message.content
        if content is None:
            raise ValueError("OpenAI returned an empty response")
        return content


class MockLLMClient:
    def generate(self, system: str, user_message: str) -> str:
        return f"This is a mock response. System: {system}  user_message:{user_message}"


def get_llm_client() -> LLMClient:
    provider = os.getenv("LLM_PROVIDER")
    model = os.getenv("LLM_MODEL")
    if not provider or not model:
        raise ValueError("LLM provider or model not specified")

    if provider == "openai":
        return OpenAIClient(model=model)
    if provider == "mock":
        return MockLLMClient()
    raise ValueError(f"Unsupported LLM provider: {provider}")
