import os
from typing import Any

from agent_framework.openai import OpenAIChatClient


class ChatClientFactory:
    _custom_factory = None

    @classmethod
    def set_custom_factory(cls, factory_func):
        cls._custom_factory = factory_func

    @classmethod
    def create_client(cls) -> Any:
        if cls._custom_factory:
            return cls._custom_factory()

        return cls._create_default_client()

    @staticmethod
    def _create_default_client() -> Any:
        api_key = os.getenv("OPENAI_API_KEY")
        model_id = os.getenv("OPENAI_MODEL", "gpt-4")

        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
        azure_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")

        if azure_endpoint and azure_api_key and azure_deployment:
            from agent_framework.openai import OpenAIChatCompletionClient

            return OpenAIChatCompletionClient(
                azure_endpoint=azure_endpoint,
                api_key=azure_api_key,
                model=azure_deployment,
                api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
            )

        if api_key:
            return OpenAIChatClient(model_id=model_id, api_key=api_key)

        raise ValueError(
            "No chat client configuration found. Set either OPENAI_API_KEY or Azure OpenAI credentials."
        )
