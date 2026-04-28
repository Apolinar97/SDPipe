from pathlib import Path


def build_sql_prompt() -> str:
    prompt_path = Path(__file__).parent / "sql_generator_system_prompt.md"
    return prompt_path.read_text()


def build_analysis_prompt() -> str:
    prompt_path = Path(__file__).parent / "analysis_system_prompt.md"
    return prompt_path.read_text()
