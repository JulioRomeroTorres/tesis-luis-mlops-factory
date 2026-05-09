import pathlib
from typing import Any

import prompty
from jinja2 import Template


class PromptManager:
    PROMPTS_DIRECTORY = pathlib.Path(__file__).parent / "prompts"

    def __init__(self):
        self._prompt_cache = {}

    def load_prompt(self, filename: str):
        if filename not in self._prompt_cache:
            prompt_path = self.PROMPTS_DIRECTORY / filename
            self._prompt_cache[filename] = prompty.load(str(prompt_path))
        return self._prompt_cache[filename]

    def render_instructions(self, filename: str, **kwargs: Any) -> str:
        prompt = self.load_prompt(filename)

        template_content = prompt.content

        lines = template_content.split("\n")
        system_lines = []
        in_system = False

        for line in lines:
            if line.strip() == "system:":
                in_system = True
                continue
            elif line.strip().startswith("user:"):
                break
            elif in_system:
                system_lines.append(line)

        system_template = "\n".join(system_lines).strip()

        template = Template(system_template)
        return template.render(**kwargs)

    def get_model_config(self, filename: str) -> dict[str, Any]:
        prompt = self.load_prompt(filename)
        model_config = {}
        if hasattr(prompt, "model") and prompt.model:
            if hasattr(prompt.model, "api"):
                model_config["api"] = prompt.model.api

            parameters = {}
            if hasattr(prompt.model, "parameters") and prompt.model.parameters:
                if isinstance(prompt.model.parameters, dict):
                    parameters = prompt.model.parameters

            model_config["parameters"] = parameters
            model_config.update(parameters)

        return model_config


_prompt_manager = PromptManager()


def get_prompt_manager() -> PromptManager:
    return _prompt_manager
