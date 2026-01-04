"""
src/main_cli.py

CLI application using Click with auto-generated documentation support.

ENHANCEMENTS:
- Auto-generated Markdown documentation from CLI commands
- OpenAPI-style documentation structure
- Rich help text with examples
- Documentation export command
"""

import sys
import os
import logging
import click
import json
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
from rich.markdown import Markdown
from rich import print as rprint

from src.main import preprocess_file
from src.utils.config_manager import ConfigManager
from src.utils.logger import setup_logging
from src.core.processor import TextPreprocessor
from src.cli.batch_commands import batch  # Batch management CLI commands

# Add src to the Python path if it's not already there.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

src_dir = os.path.abspath(os.path.dirname(__file__))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Set up logging early in the entrypoint script
settings = ConfigManager.get_settings()
setup_logging()
logger = logging.getLogger("ingestion_service")

# Rich console for beautiful output
console = Console()

# Instantiate TextPreprocessor to ensure spaCy model is loaded
try:
    preprocessor = TextPreprocessor()
    logger.info("TextPreprocessor initialized for CLI, spaCy model loaded.")
except Exception as e:
    console.print(
        f"[bold red]Error:[/bold red] Failed to initialize TextPreprocessor: {e}")
    sys.exit(1)


# CLI Documentation metadata
CLI_METADATA = {
    "title": "Data Ingestion & Preprocessing CLI",
    "version": "1.0.0",
    "description": "A command-line interface for cleaning and preprocessing news articles with NLP enrichment.",
    "author": "Data Engineering Team",
    "contact": {
        "name": "Support",
        "email": "support@example.com",
        "url": "https://github.com/your-repo"
    }
}


def generate_cli_documentation(ctx, output_format='markdown'):
    """
    Generate comprehensive CLI documentation in OpenAPI-like format.
    
    Args:
        ctx: Click context
        output_format: 'markdown', 'json', or 'html'
    
    Returns:
        Formatted documentation string
    """
    docs = {
        "metadata": CLI_METADATA,
        "commands": {}
    }

    # Iterate through all commands
    for cmd_name, cmd in ctx.command.commands.items():
        cmd_docs = {
            "name": cmd_name,
            "description": cmd.help or "No description available",
            "usage": f"ingestion-cli {cmd_name} [OPTIONS]",
            "options": [],
            "examples": []
        }

        # Extract parameters/options
        for param in cmd.params:
            param_doc = {
                "name": param.name,
                "type": param.type.name if hasattr(param.type, 'name') else str(param.type),
                "required": param.required,
                "default": param.default if param.default is not None else "None",
                # Use getattr for safety
                "help": getattr(param, 'help', None) or "No description"
            }

            if isinstance(param, click.Option):
                param_doc["flags"] = param.opts
                param_doc["is_flag"] = param.is_flag
            elif isinstance(param, click.Argument):
                param_doc["flags"] = [param.name]
                param_doc["is_argument"] = True

            cmd_docs["options"].append(param_doc)

        # Add command-specific examples
        if cmd_name == "process":
            cmd_docs["examples"] = [
                {
                    "description": "Process file locally (synchronous)",
                    "command": "ingestion-cli process -i input.jsonl -o output.jsonl"
                },
                {
                    "description": "Process with Celery (asynchronous)",
                    "command": "ingestion-cli process -i input.jsonl -o output.jsonl --celery"
                },
                {
                    "description": "Disable typo correction",
                    "command": "ingestion-cli process -i input.jsonl -o output.jsonl --disable-typo-correction"
                }
            ]
        elif cmd_name == "validate":
            cmd_docs["examples"] = [
                {
                    "description": "Validate JSONL file",
                    "command": "ingestion-cli validate input.jsonl"
                }
            ]
        elif cmd_name == "test-model":
            cmd_docs["examples"] = [
                {
                    "description": "Test with default text",
                    "command": "ingestion-cli test-model"
                },
                {
                    "description": "Test with custom text",
                    "command": "ingestion-cli test-model --text \"Apple Inc. in San Francisco\""
                }
            ]

        docs["commands"][cmd_name] = cmd_docs

    # Format output
    if output_format == 'json':
        return json.dumps(docs, indent=2)
    elif output_format == 'markdown':
        return _format_markdown_docs(docs)
    elif output_format == 'html':
        return _format_html_docs(docs)
    else:
        return json.dumps(docs, indent=2)


def _format_markdown_docs(docs):
    """Format documentation as Markdown."""
    md = f"# {docs['metadata']['title']}\n\n"
    md += f"**Version:** {docs['metadata']['version']}\n\n"
    md += f"{docs['metadata']['description']}\n\n"
    md += f"**Contact:** {docs['metadata']['contact']['email']}\n\n"
    md += "---\n\n"
    md += "## Commands\n\n"

    for cmd_name, cmd_info in docs["commands"].items():
        md += f"### `{cmd_name}`\n\n"
        md += f"{cmd_info['description']}\n\n"
        md += f"**Usage:** `{cmd_info['usage']}`\n\n"

        if cmd_info["options"]:
            md += "**Options:**\n\n"
            md += "| Option | Type | Required | Default | Description |\n"
            md += "|--------|------|----------|---------|-------------|\n"
            for opt in cmd_info["options"]:
                flags = ', '.join(opt.get('flags', [opt['name']]))
                md += f"| `{flags}` | {opt['type']} | {opt['required']} | {opt['default']} | {opt['help']} |\n"
            md += "\n"

        if cmd_info["examples"]:
            md += "**Examples:**\n\n"
            for ex in cmd_info["examples"]:
                md += f"- {ex['description']}\n"
                md += f"  ```bash\n  {ex['command']}\n  ```\n\n"

        md += "---\n\n"

    return md


def _format_html_docs(docs):
    """Format documentation as HTML."""
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{docs['metadata']['title']}</title>
    <style>
        body {{ font-family: Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }}
        h1, h2, h3 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        code {{ background-color: #f4f4f4; padding: 2px 6px; border-radius: 3px; }}
        pre {{ background-color: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; }}
        .command {{ background-color: #e7f3fe; padding: 15px; margin: 10px 0; border-left: 4px solid #2196F3; }}
    </style>
</head>
<body>
    <h1>{docs['metadata']['title']}</h1>
    <p><strong>Version:</strong> {docs['metadata']['version']}</p>
    <p>{docs['metadata']['description']}</p>
    <p><strong>Contact:</strong> <a href="mailto:{docs['metadata']['contact']['email']}">{docs['metadata']['contact']['email']}</a></p>
    <hr>
    <h2>Commands</h2>
"""

    for cmd_name, cmd_info in docs["commands"].items():
        html += f"""
    <div class="command">
        <h3>{cmd_name}</h3>
        <p>{cmd_info['description']}</p>
        <p><strong>Usage:</strong> <code>{cmd_info['usage']}</code></p>
"""

        if cmd_info["options"]:
            html += """
        <h4>Options</h4>
        <table>
            <tr>
                <th>Option</th>
                <th>Type</th>
                <th>Required</th>
                <th>Default</th>
                <th>Description</th>
            </tr>
"""
            for opt in cmd_info["options"]:
                flags = ', '.join(opt.get('flags', [opt['name']]))
                html += f"""
            <tr>
                <td><code>{flags}</code></td>
                <td>{opt['type']}</td>
                <td>{opt['required']}</td>
                <td>{opt['default']}</td>
                <td>{opt['help']}</td>
            </tr>
"""
            html += "        </table>\n"

        if cmd_info["examples"]:
            html += "        <h4>Examples</h4>\n"
            for ex in cmd_info["examples"]:
                html += f"""
        <p>{ex['description']}</p>
        <pre><code>{ex['command']}</code></pre>
"""

        html += "    </div>\n"

    html += """
</body>
</html>
"""
    return html


@click.group()
@click.version_option(version="2.0.0", prog_name="ingestion-cli")
@click.pass_context
def cli(ctx):
    """
    🧹 Data Ingestion & Preprocessing CLI v2.0.0

    A command-line interface for cleaning and preprocessing news articles.
    Supports batch processing, Celery integration, and multiple storage backends.

    \b
    Quick Start:
        ingestion-cli info                    # Show system information
        ingestion-cli test-model              # Test spaCy model
        ingestion-cli validate input.jsonl    # Validate file
        ingestion-cli process -i in.jsonl -o out.jsonl  # Process articles

        # NEW: Batch lifecycle management
        ingestion-cli batch submit -f data/input.jsonl --watch
        ingestion-cli batch status -j <job_id>
        ingestion-cli batch list --status RUNNING
        ingestion-cli batch pause -j <job_id>
        ingestion-cli batch resume -j <job_id>

    \b
    Documentation:
        ingestion-cli docs export --format markdown  # Export CLI docs
        ingestion-cli docs show                      # View docs in terminal

    For detailed help on any command, use:
        ingestion-cli COMMAND --help
    """
    ctx.ensure_object(dict)


# Register batch command group for lifecycle management
cli.add_command(batch)


@cli.group(name="docs")
def docs_group():
    """📚 Documentation commands for CLI reference and export."""
    pass


@docs_group.command(name="show")
@click.pass_context
def show_docs(ctx):
    """
    Display CLI documentation in terminal.
    
    \b
    Example:
        ingestion-cli docs show
    """
    parent_ctx = ctx.parent.parent
    docs_md = generate_cli_documentation(parent_ctx, output_format='markdown')

    console.print("\n")
    console.print(Markdown(docs_md))
    console.print("\n")


@docs_group.command(name="export")
@click.option(
    '--format',
    type=click.Choice(['markdown', 'json', 'html'], case_sensitive=False),
    default='markdown',
    help='Output format for documentation'
)
@click.option(
    '-o', '--output',
    type=click.Path(dir_okay=False, writable=True),
    default=None,
    help='Output file path (prints to stdout if not specified)'
)
@click.pass_context
def export_docs(ctx, format, output):
    """
    Export CLI documentation to file.
    
    \b
    Examples:
        ingestion-cli docs export --format markdown -o CLI_REFERENCE.md
        ingestion-cli docs export --format json -o cli-schema.json
        ingestion-cli docs export --format html -o cli-docs.html
    """
    parent_ctx = ctx.parent.parent
    docs = generate_cli_documentation(parent_ctx, output_format=format)

    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(docs, encoding='utf-8')
        console.print(
            f"\n[bold green]✅ Documentation exported to:[/bold green] {output}\n")
    else:
        console.print(docs)


@docs_group.command(name="openapi")
@click.option(
    '-o', '--output',
    type=click.Path(dir_okay=False, writable=True),
    default='cli-openapi.json',
    help='Output file path for OpenAPI-style schema'
)
@click.pass_context
def export_openapi_schema(ctx, output):
    """
    Export CLI commands as OpenAPI-style JSON schema.
    
    This generates a schema that mirrors the API's OpenAPI spec but for CLI commands.
    Useful for generating client libraries or integration documentation.
    
    \b
    Example:
        ingestion-cli docs openapi -o cli-schema.json
    """
    parent_ctx = ctx.parent.parent

    # Generate OpenAPI-style schema
    schema = {
        "openapi": "3.1.0",
        "info": {
            "title": CLI_METADATA["title"],
            "version": CLI_METADATA["version"],
            "description": CLI_METADATA["description"],
            "contact": CLI_METADATA["contact"]
        },
        "commands": {}
    }

    for cmd_name, cmd in parent_ctx.command.commands.items():
        cmd_schema = {
            "summary": cmd.help or "No description",
            "operationId": f"cli_{cmd_name}",
            "parameters": []
        }

        for param in cmd.params:
            param_schema = {
                "name": param.name,
                "in": "cli",
                "required": param.required,
                "schema": {
                    "type": _map_click_type_to_json_type(param.type),
                    "default": param.default if param.default is not None else None
                },
                # Use getattr for safety
                "description": getattr(param, 'help', None) or ""
            }

            if isinstance(param, click.Option):
                param_schema["flags"] = param.opts
            elif isinstance(param, click.Argument):
                param_schema["flags"] = [param.name]
                param_schema["is_argument"] = True

            cmd_schema["parameters"].append(param_schema)

        schema["commands"][cmd_name] = cmd_schema

    output_path = Path(output)
    output_path.write_text(json.dumps(schema, indent=2), encoding='utf-8')
    console.print(
        f"\n[bold green]✅ OpenAPI schema exported to:[/bold green] {output}\n")


def _map_click_type_to_json_type(click_type):
    """Map Click parameter types to JSON Schema types."""
    type_mapping = {
        'STRING': 'string',
        'INT': 'integer',
        'FLOAT': 'number',
        'BOOL': 'boolean',
        'Path': 'string',
        'Choice': 'string'
    }
    type_name = click_type.name if hasattr(
        click_type, 'name') else str(click_type)
    return type_mapping.get(type_name, 'string')


@cli.command(name="process")
@click.option(
    '-i', '--input',
    'input_path',
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=True,
    help='Path to input JSONL file (one article per line)'
)
@click.option(
    '-o', '--output',
    'output_path',
    type=click.Path(dir_okay=False, writable=True),
    required=True,
    help='Path to output JSONL file'
)
@click.option(
    '--celery/--no-celery',
    default=False,
    help='Submit tasks to Celery workers (async) or process locally (sync)'
)
@click.option(
    '--backends',
    type=str,
    default=None,
    help='Comma-separated list of storage backends (e.g., "jsonl,postgresql,elasticsearch")'
)
@click.option(
    '--disable-typo-correction',
    is_flag=True,
    default=False,
    help='Disable typo correction for this batch'
)
@click.option(
    '--disable-html-removal',
    is_flag=True,
    default=False,
    help='Disable HTML tag removal'
)
@click.option(
    '--disable-currency-standardization',
    is_flag=True,
    default=False,
    help='Disable currency standardization ($100 → USD 100)'
)
def process_command(input_path: str, output_path: str, celery: bool, backends: str,
                    disable_typo_correction: bool, disable_html_removal: bool,
                    disable_currency_standardization: bool):
    """
    Process a JSONL file containing news articles.
    
    \b
    Examples:
        # Process locally (synchronous)
        ingestion-cli process -i data/input.jsonl -o data/output.jsonl
        
        # Process with Celery (asynchronous)
        ingestion-cli process -i data/input.jsonl -o data/output.jsonl --celery
        
        # Disable typo correction
        ingestion-cli process -i data/input.jsonl -o data/output.jsonl --disable-typo-correction
    """
    console.print(
        "\n[bold cyan]🚀 Starting Article Processing Pipeline[/bold cyan]\n")

    # Build custom config if any flags set
    custom_config = {}
    if disable_typo_correction:
        custom_config['enable_typo_correction'] = False
    if disable_html_removal:
        custom_config['remove_html_tags'] = False
    if disable_currency_standardization:
        custom_config['standardize_currency'] = False

    # Display configuration
    config_table = Table(title="Configuration",
                         show_header=True, header_style="bold magenta")
    config_table.add_column("Setting", style="cyan")
    config_table.add_column("Value", style="green")

    config_table.add_row("Input File", input_path)
    config_table.add_row("Output File", output_path)
    config_table.add_row(
        "Processing Mode", "Celery (Async)" if celery else "Local (Sync)")
    config_table.add_row("Storage Backends",
                         backends if backends else "Default (from config)")
    config_table.add_row("SpaCy Model", settings.ingestion_service.model_name)
    config_table.add_row(
        "GPU Enabled", "Yes" if settings.general.gpu_enabled else "No")

    if custom_config:
        config_table.add_row("Custom Config", str(custom_config))

    console.print(config_table)
    console.print()

    try:
        # Count total lines for progress tracking
        with open(input_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for line in f if line.strip())

        console.print(
            f"[bold]Found {total_lines} articles to process[/bold]\n")

        # Call the processing function - it now returns stats
        stats = preprocess_file(
            input_path=input_path,
            output_path=output_path,
            use_celery=celery,
            custom_cleaning_config=custom_config if custom_config else None
        )

        # Display results with Rich formatting
        console.print(f"\n[bold green]✅ Processing complete![/bold green]")

        # Create results table
        results_table = Table(title="Processing Results",
                              show_header=True, header_style="bold magenta")
        results_table.add_column("Metric", style="cyan")
        results_table.add_column("Count", style="green", justify="right")

        summary = stats.get_summary()
        results_table.add_row("Total Lines", str(summary['total_lines']))
        results_table.add_row("Processed Successfully", str(
            summary['processed_successfully']))
        results_table.add_row("JSON Decode Errors", str(
            summary['json_decode_errors']))
        results_table.add_row("Validation Errors", str(
            summary['validation_errors']))
        results_table.add_row("Processing Errors", str(
            summary['processing_errors']))
        results_table.add_row("Success Rate", summary['success_rate'])

        console.print()
        console.print(results_table)
        console.print(f"\n[cyan]Results saved to:[/cyan] {output_path}")

        if stats.errors:
            console.print(
                f"\n[yellow]⚠️  {len(stats.errors)} errors occurred during processing[/yellow]")
            console.print(
                f"[dim]See error details in: {output_path}.replace('.jsonl', '_errors.json')[/dim]")

        console.print()

    except FileNotFoundError as e:
        console.print(
            f"[bold red]❌ Error:[/bold red] Input file not found: {input_path}")
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]❌ Error:[/bold red] {str(e)}")
        logger.error(f"Processing failed: {e}", exc_info=True)
        sys.exit(1)


@cli.command(name="validate")
@click.argument('input_path', type=click.Path(exists=True, dir_okay=False, readable=True))
def validate_command(input_path: str):
    """
    Validate a JSONL file for correct format and schema.
    
    \b
    Example:
        ingestion-cli validate data/input.jsonl
    """
    console.print(
        f"\n[bold cyan]🔍 Validating file:[/bold cyan] {input_path}\n")

    from src.schemas.data_models import ArticleInput
    import json
    from pydantic import ValidationError

    valid_count = 0
    error_count = 0
    errors = []

    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task("[cyan]Validating...", total=len(lines))

            for i, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    progress.advance(task)
                    continue

                try:
                    article_data = json.loads(line)
                    ArticleInput.model_validate(article_data)
                    valid_count += 1
                except json.JSONDecodeError as e:
                    error_count += 1
                    errors.append(f"Line {i}: Invalid JSON - {str(e)}")
                except ValidationError as e:
                    error_count += 1
                    errors.append(
                        f"Line {i}: Schema validation failed - {e.error_count()} errors")

                progress.advance(task)

        # Display results
        console.print()
        results_table = Table(title="Validation Results",
                              show_header=True, header_style="bold magenta")
        results_table.add_column("Metric", style="cyan")
        results_table.add_column("Count", style="green")

        results_table.add_row("Total Lines", str(len(lines)))
        results_table.add_row("Valid Articles", str(valid_count))
        results_table.add_row("Errors", str(error_count))

        console.print(results_table)

        if error_count > 0:
            console.print(
                f"\n[bold yellow]⚠️  Found {error_count} errors[/bold yellow]")
            if len(errors) <= 10:
                console.print("\n[bold]Error Details:[/bold]")
                for error in errors:
                    console.print(f"  [red]•[/red] {error}")
            else:
                console.print(f"\n[bold]First 10 errors:[/bold]")
                for error in errors[:10]:
                    console.print(f"  [red]•[/red] {error}")
                console.print(
                    f"\n  [dim]... and {len(errors) - 10} more errors[/dim]")
        else:
            console.print(
                f"\n[bold green]✅ All articles are valid![/bold green]\n")

        sys.exit(0 if error_count == 0 else 1)

    except Exception as e:
        console.print(f"[bold red]❌ Error:[/bold red] {str(e)}")
        logger.error(f"Validation failed: {e}", exc_info=True)
        sys.exit(1)


@cli.command(name="info")
def info_command():
    """
    Display system and configuration information.
    """
    console.print("\n[bold cyan]ℹ️  System Information[/bold cyan]\n")

    info_table = Table(show_header=True, header_style="bold magenta")
    info_table.add_column("Component", style="cyan")
    info_table.add_column("Details", style="green")

    # System info
    info_table.add_row("CLI Version", "1.0.0")
    info_table.add_row("Python Version", f"{sys.version.split()[0]}")

    # Configuration
    info_table.add_row("Log Level", settings.general.log_level)
    info_table.add_row(
        "GPU Enabled", "Yes" if settings.general.gpu_enabled else "No")
    info_table.add_row("SpaCy Model", settings.ingestion_service.model_name)
    info_table.add_row("Model Cache Dir",
                       settings.ingestion_service.model_cache_dir)

    # Cleaning pipeline
    pipeline = settings.ingestion_service.cleaning_pipeline
    info_table.add_row(
        "Typo Correction", "Enabled" if pipeline.enable_typo_correction else "Disabled")
    info_table.add_row(
        "NER Protection", "Enabled" if pipeline.typo_correction.use_ner_entities else "Disabled")
    info_table.add_row(
        "HTML Removal", "Enabled" if pipeline.remove_html_tags else "Disabled")
    info_table.add_row(
        "Currency Std", "Enabled" if pipeline.standardize_currency else "Disabled")

    # Celery
    info_table.add_row("Celery Broker", settings.celery.broker_url)
    info_table.add_row("Worker Concurrency", str(
        settings.celery.worker_concurrency))

    # Storage
    enabled_backends = settings.storage.enabled_backends
    info_table.add_row("Storage Backends", ", ".join(
        enabled_backends) if enabled_backends else "None")

    console.print(info_table)
    console.print()


@cli.command(name="test-model")
@click.option(
    '--text',
    type=str,
    default="This is a test article about artificial intelligence and machine learning.",
    help='Test text to process'
)
@click.option(
    '--disable-typo-correction',
    is_flag=True,
    default=False,
    help='Disable typo correction for this test'
)
def test_model_command(text: str, disable_typo_correction: bool):
    """
    Test the spaCy model with sample text using NER-protected cleaning.
    
    \b
    Example:
        ingestion-cli test-model --text "Apple Inc. in San Francisco"
        ingestion-cli test-model --text "Your text" --disable-typo-correction
    """
    console.print("\n[bold cyan]🧪 Testing SpaCy Model[/bold cyan]\n")

    try:
        # Build custom config if flag set
        custom_config = None
        if disable_typo_correction:
            custom_config = {'enable_typo_correction': False}
            # Temporarily update preprocessor config
            from src.utils.text_cleaners import TextCleanerConfig
            preprocessor.cleaning_config = TextCleanerConfig(custom_config)

        with console.status("[bold green]Processing text..."):
            # Use NER-protected cleaning
            cleaned_text, entities = preprocessor.clean_text_with_ner_protection(
                text)

        console.print(f"[bold]Original Text:[/bold]\n{text}\n")
        console.print(f"[bold]Cleaned Text:[/bold]\n{cleaned_text}\n")

        if entities:
            console.print(
                f"[bold green]Found {len(entities)} entities:[/bold green]\n")

            entity_table = Table(show_header=True, header_style="bold magenta")
            entity_table.add_column("Entity", style="cyan")
            entity_table.add_column("Type", style="green")
            entity_table.add_column("Position", style="yellow")

            for entity in entities:
                entity_table.add_row(
                    entity.text,
                    entity.type,
                    f"{entity.start_char}-{entity.end_char}"
                )

            console.print(entity_table)
        else:
            console.print("[yellow]No entities found[/yellow]")

        # Show config used
        if disable_typo_correction:
            console.print(
                f"\n[dim]Note: Typo correction was disabled for this test[/dim]")

        console.print(f"\n[bold green]✅ Model test complete![/bold green]\n")

    except Exception as e:
        console.print(f"[bold red]❌ Error:[/bold red] {str(e)}")
        logger.error(f"Model test failed: {e}", exc_info=True)
        sys.exit(1)


def main():
    """
    Main function to run the CLI application.
    """
    try:
        cli()
    except Exception as e:
        console.print(f"[bold red]❌ Unexpected error:[/bold red] {str(e)}")
        logger.critical(f"CLI crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

# src/main_cli.py

