import os
from contextlib import contextmanager
from typing import Optional

import analysis
import service
import sqlparse
import typer
from prompt_toolkit import prompt
from prompt_toolkit.completion import FuzzyWordCompleter
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.syntax import Syntax
from rich.table import Table
from typing_extensions import Annotated

_console = Console()


def _sql_theme() -> str:
    return os.getenv("SDPIPE_SQL_THEME", "monokai")


def _print_sql(sql: str) -> None:
    formatted = sqlparse.format(sql, reindent=True, keyword_case="upper")
    _console.print(Panel(Syntax(formatted, "sql", theme=_sql_theme()), title="Generated SQL", border_style="dim"))


def _print_table(columns: list, rows: list) -> None:
    if not rows:
        _console.print("[yellow]No results found.[/yellow]")
        return
    table = Table(show_header=True, header_style="bold")
    for col in columns:
        table.add_column(str(col))
    for row in rows:
        table.add_row(*[str(v) if v is not None else "" for v in row])
    _console.print(table)
    _console.print(f"[dim]{len(rows)} row(s) returned.[/dim]")


@contextmanager
def _spinner(description: str):
    with Progress(SpinnerColumn(), TextColumn("{task.description}"), TimeElapsedColumn(), console=_console) as progress:
        progress.add_task(description)
        yield


app = typer.Typer()

command_completer = FuzzyWordCompleter(
    [
        "collisions",
        "participants",
        "weather",
        "beats",
        "violations",
        "date",
        "station",
        "how many",
        "show me",
        "count",
        "list",
        "find",
        "breakdown",
        "total",
        "average",
        "last week",
        "last month",
        "last year",
        "this year",
        "by date",
        "by month",
        "collision",
        "beat",
        "violation",
        "injury",
        "fatality",
        "weather station",
    ]
)


def _resolve_nl_query(nl_query: str) -> str:
    if service.is_blank(nl_query):
        nl_query = prompt("Enter your natural language query: ", completer=command_completer)
    if service.is_blank(nl_query):
        typer.echo("No valid natural language query provided. Exiting.")
        raise typer.Exit(1)
    return nl_query


@app.command()
def generate_sql(nl_query: Annotated[Optional[str], typer.Argument(help="Natural language query to convert to SQL")]):
    nl_query = _resolve_nl_query(nl_query)
    with _spinner(f"Generating SQL for: {nl_query}"):
        sql = service.get_sql(nl_query)
    _print_sql(sql)


@app.command()
def query(
    nl_query: Annotated[
        Optional[str], typer.Argument(help="Natural language query to run against the database")
    ] = None,
    analyze: Annotated[bool, typer.Option("--analyze", help="Analyze results after the query.")] = False,
    question: Annotated[
        Optional[str], typer.Option("--question", help="Specific question to ask about the results.")
    ] = None,
):
    nl_query = _resolve_nl_query(nl_query)
    with _spinner(f"Generating SQL for: {nl_query}"):
        sql = service.get_sql(nl_query)
    with _spinner("Executing query..."):
        columns, rows = service.execute_query(sql)
    _print_sql(sql)
    _console.rule()
    _print_table(columns, rows)

    analysis.save_cache(nl_query, sql, columns, rows)

    if analyze:
        _console.rule()
        with _spinner("Analyzing results..."):
            result = analysis.analyze(nl_query, sql, columns, rows, question)
        _console.print(Markdown(result, code_theme=_sql_theme()))


@app.command()
def analyze(
    question: Annotated[Optional[str], typer.Argument(help="Analysis question (optional)")] = None,
):
    cached = analysis.load_cache()
    if cached is None:
        typer.echo("No cached query found. Run `query` first.")
        raise typer.Exit(1)
    nl_query, sql, columns, rows = cached
    _console.print(f"[dim]Analyzing last query:[/dim] {nl_query} [dim]({len(rows)} row(s) in cache)[/dim]")
    _console.rule()
    with _spinner("Analyzing..."):
        result = analysis.analyze(nl_query, sql, columns, rows, question)
    _console.print(Markdown(result, code_theme=_sql_theme()))


@app.command()
def test():
    typer.echo("Testing the CLI")


if __name__ == "__main__":
    app()
