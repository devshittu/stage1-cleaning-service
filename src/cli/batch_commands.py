"""
src/cli/batch_commands.py

Batch management CLI commands for job lifecycle control.

Features:
- Submit batch jobs from JSONL files
- Query job status with real-time updates
- Pause/resume/cancel running jobs
- List jobs with filtering
- Watch mode for real-time progress monitoring

DESIGN PATTERN: Clean separation of concerns
- CLI layer (this file) - User interaction
- API client layer - HTTP requests to orchestrator
- Business logic layer - Job manager, checkpoint manager

Usage:
    python -m src.main_cli batch submit --file data/input.jsonl --batch-id batch_001
    python -m src.main_cli batch status --job-id <job_id>
    python -m src.main_cli batch pause --job-id <job_id>
    python -m src.main_cli batch resume --job-id <job_id>
    python -m src.main_cli batch cancel --job-id <job_id>
    python -m src.main_cli batch list --status running
    python -m src.main_cli batch watch --job-id <job_id>
"""

import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional, Dict, Any

import click
import httpx
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.live import Live
from rich import print as rprint

from src.utils.json_sanitizer import sanitize_and_parse_json

logger = logging.getLogger("ingestion_service")
console = Console()

# Orchestrator API base URL
API_BASE_URL = os.getenv("ORCHESTRATOR_API_URL", "http://localhost:8000")


@click.group(name="batch")
def batch():
    """
    Batch job management commands.

    Manage batch processing jobs with lifecycle control (submit, pause, resume, cancel).
    """
    pass


@batch.command(name="submit")
@click.option(
    "--file",
    "-f",
    required=True,
    type=click.Path(exists=True),
    help="Path to JSONL file containing documents to process"
)
@click.option(
    "--batch-id",
    "-b",
    default=None,
    help="Optional batch ID for tracking (auto-generated if not provided)"
)
@click.option(
    "--checkpoint-interval",
    "-c",
    default=10,
    type=int,
    help="Save checkpoint every N documents (default: 10)"
)
@click.option(
    "--backends",
    "-e",
    multiple=True,
    default=["jsonl"],
    help="Storage backends to use (default: jsonl). Can specify multiple."
)
@click.option(
    "--watch",
    "-w",
    is_flag=True,
    help="Watch job progress after submission"
)
def submit_batch(file: str, batch_id: Optional[str], checkpoint_interval: int, backends: tuple, watch: bool):
    """
    Submit a batch job for processing.

    Reads documents from a JSONL file and submits them as a batch job.
    Returns immediately with a job_id for tracking.

    Examples:
        # Submit with auto-generated batch ID
        python -m src.main_cli batch submit -f data/input.jsonl

        # Submit with custom batch ID and checkpoint every 5 documents
        python -m src.main_cli batch submit -f data/input.jsonl -b my_batch_001 -c 5

        # Submit and watch progress
        python -m src.main_cli batch submit -f data/input.jsonl --watch
    """
    try:
        # Load documents from JSONL file
        console.print(f"[cyan]Reading documents from: {file}[/cyan]")
        documents = []

        with open(file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                # Use json_sanitizer for robust parsing
                doc, error = sanitize_and_parse_json(line, line_num)
                if doc:
                    documents.append(doc)
                elif error:
                    console.print(f"[yellow]Warning: Skipping line {line_num}: {error}[/yellow]")
                    continue

        if not documents:
            console.print("[red]Error: No valid documents found in file[/red]")
            sys.exit(1)

        console.print(f"[green]Loaded {len(documents)} documents[/green]")

        # Submit batch job to API
        payload = {
            "documents": documents,
            "batch_id": batch_id,
            "checkpoint_interval": checkpoint_interval,
            "persist_to_backends": list(backends)
        }

        with console.status("[bold green]Submitting batch job..."):
            response = httpx.post(
                f"{API_BASE_URL}/v1/documents/batch",
                json=payload,
                timeout=30.0
            )
            response.raise_for_status()

        result = response.json()
        job_id = result.get("job_id")

        # Display result in a panel
        result_text = f"""
[bold green]Batch Job Submitted Successfully![/bold green]

[cyan]Job ID:[/cyan] {job_id}
[cyan]Batch ID:[/cyan] {result.get('batch_id', 'N/A')}
[cyan]Total Documents:[/cyan] {result.get('total_documents', len(documents))}
[cyan]Checkpoint Interval:[/cyan] Every {checkpoint_interval} documents
[cyan]Storage Backends:[/cyan] {', '.join(backends)}

[bold]Track progress with:[/bold]
python -m src.main_cli batch status --job-id {job_id}

[bold]Or watch in real-time:[/bold]
python -m src.main_cli batch watch --job-id {job_id}
"""

        console.print(Panel(result_text, title="Batch Submission", border_style="green"))

        # Watch mode
        if watch:
            console.print("\n[cyan]Entering watch mode...[/cyan]\n")
            ctx = click.get_current_context()
            ctx.invoke(watch_job, job_id=job_id)

    except httpx.HTTPError as e:
        console.print(f"[red]API Error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        logger.exception("Failed to submit batch job")
        sys.exit(1)


@batch.command(name="status")
@click.option(
    "--job-id",
    "-j",
    required=True,
    help="Job ID to query"
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show detailed information including resource usage"
)
def get_status(job_id: str, verbose: bool):
    """
    Get current status of a batch job.

    Displays job progress, statistics, and status information.

    Examples:
        # Basic status
        python -m src.main_cli batch status -j abc-123

        # Detailed status with resource usage
        python -m src.main_cli batch status -j abc-123 --verbose
    """
    try:
        response = httpx.get(
            f"{API_BASE_URL}/v1/jobs/{job_id}",
            timeout=10.0
        )
        response.raise_for_status()

        job = response.json()

        # Create status table
        table = Table(title=f"Job Status: {job_id}", show_header=True, header_style="bold magenta")
        table.add_column("Field", style="cyan", no_wrap=True)
        table.add_column("Value", style="white")

        # Basic info
        status = job.get("status", "UNKNOWN")
        status_color = {
            "QUEUED": "yellow",
            "RUNNING": "green",
            "PAUSED": "blue",
            "COMPLETED": "bold green",
            "FAILED": "red",
            "CANCELLED": "red"
        }.get(status, "white")

        table.add_row("Status", f"[{status_color}]{status}[/{status_color}]")
        table.add_row("Batch ID", job.get("batch_id", "N/A"))
        table.add_row("Progress", f"{job.get('progress_percent', 0):.1f}%")
        table.add_row("Processed", f"{job.get('processed_documents', 0)}/{job.get('total_documents', 0)}")

        if job.get("failed_documents", 0) > 0:
            table.add_row("Failed", str(job.get("failed_documents")), style="red")

        # Timestamps
        if job.get("created_at"):
            table.add_row("Created", job["created_at"])
        if job.get("started_at"):
            table.add_row("Started", job["started_at"])
        if job.get("completed_at"):
            table.add_row("Completed", job["completed_at"])

        # Verbose mode: resource usage
        if verbose and job.get("resource_usage"):
            resource = job["resource_usage"]
            table.add_row("CPU Usage", f"{resource.get('cpu_percent', 0):.1f}%")
            table.add_row("Memory Usage", f"{resource.get('memory_percent', 0):.1f}% ({resource.get('memory_used_gb', 0):.2f} GB)")
            if resource.get("gpu_available"):
                table.add_row("GPU Memory", f"{resource.get('gpu_memory_used_mb', 0):.0f} MB / {resource.get('gpu_memory_total_mb', 0):.0f} MB")

        # Error message if failed
        if job.get("error_message"):
            table.add_row("Error", job["error_message"], style="red")

        console.print(table)

        # Checkpoint info if paused
        if status == "PAUSED" and job.get("checkpoint"):
            checkpoint_text = f"""
[bold blue]Checkpoint Available[/bold blue]

Resume this job to continue from document {job['checkpoint'].get('processed_count', 0)}
Command: python -m src.main_cli batch resume --job-id {job_id}
"""
            console.print(Panel(checkpoint_text, border_style="blue"))

    except httpx.HTTPError as e:
        if e.response.status_code == 404:
            console.print(f"[red]Job not found: {job_id}[/red]")
        else:
            console.print(f"[red]API Error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@batch.command(name="pause")
@click.option(
    "--job-id",
    "-j",
    required=True,
    help="Job ID to pause"
)
def pause_job(job_id: str):
    """
    Pause a running batch job.

    The job will save a checkpoint and gracefully stop processing.
    Can be resumed later with 'batch resume'.

    Examples:
        python -m src.main_cli batch pause -j abc-123
    """
    try:
        with console.status(f"[bold yellow]Pausing job {job_id}..."):
            response = httpx.patch(
                f"{API_BASE_URL}/v1/jobs/{job_id}/pause",
                timeout=10.0
            )
            response.raise_for_status()

        result = response.json()

        success_text = f"""
[bold green]Job Paused Successfully![/bold green]

[cyan]Job ID:[/cyan] {job_id}
[cyan]Message:[/cyan] {result.get('message', 'Job pause requested')}

The job will save a checkpoint and stop after the current document batch.
Resume with: python -m src.main_cli batch resume --job-id {job_id}
"""

        console.print(Panel(success_text, title="Job Paused", border_style="yellow"))

    except httpx.HTTPError as e:
        if e.response.status_code == 404:
            console.print(f"[red]Job not found: {job_id}[/red]")
        elif e.response.status_code == 409:
            console.print(f"[yellow]Job cannot be paused (may already be paused or completed)[/yellow]")
        else:
            console.print(f"[red]API Error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@batch.command(name="resume")
@click.option(
    "--job-id",
    "-j",
    required=True,
    help="Job ID to resume"
)
def resume_job(job_id: str):
    """
    Resume a paused batch job.

    The job will continue from where it was paused using the saved checkpoint.

    Examples:
        python -m src.main_cli batch resume -j abc-123
    """
    try:
        with console.status(f"[bold green]Resuming job {job_id}..."):
            response = httpx.patch(
                f"{API_BASE_URL}/v1/jobs/{job_id}/resume",
                timeout=10.0
            )
            response.raise_for_status()

        result = response.json()

        success_text = f"""
[bold green]Job Resumed Successfully![/bold green]

[cyan]Job ID:[/cyan] {job_id}
[cyan]Message:[/cyan] {result.get('message', 'Job resumed from checkpoint')}

Monitor progress with: python -m src.main_cli batch watch --job-id {job_id}
"""

        console.print(Panel(success_text, title="Job Resumed", border_style="green"))

    except httpx.HTTPError as e:
        if e.response.status_code == 404:
            console.print(f"[red]Job not found: {job_id}[/red]")
        elif e.response.status_code == 501:
            console.print(f"[yellow]Resume functionality is currently a placeholder. Please resubmit the batch.[/yellow]")
        elif e.response.status_code == 409:
            console.print(f"[yellow]Job cannot be resumed (may already be running or completed)[/yellow]")
        else:
            console.print(f"[red]API Error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@batch.command(name="cancel")
@click.option(
    "--job-id",
    "-j",
    required=True,
    help="Job ID to cancel"
)
@click.confirmation_option(
    prompt="Are you sure you want to cancel this job?"
)
def cancel_job(job_id: str):
    """
    Cancel a batch job.

    The job will stop processing and cannot be resumed.
    Requires confirmation.

    Examples:
        python -m src.main_cli batch cancel -j abc-123
    """
    try:
        with console.status(f"[bold red]Cancelling job {job_id}..."):
            response = httpx.delete(
                f"{API_BASE_URL}/v1/jobs/{job_id}",
                timeout=10.0
            )
            response.raise_for_status()

        result = response.json()

        success_text = f"""
[bold red]Job Cancelled![/bold red]

[cyan]Job ID:[/cyan] {job_id}
[cyan]Message:[/cyan] {result.get('message', 'Job cancelled successfully')}

The job has been stopped and cannot be resumed.
"""

        console.print(Panel(success_text, title="Job Cancelled", border_style="red"))

    except httpx.HTTPError as e:
        if e.response.status_code == 404:
            console.print(f"[red]Job not found: {job_id}[/red]")
        elif e.response.status_code == 409:
            console.print(f"[yellow]Job cannot be cancelled (may already be completed or cancelled)[/yellow]")
        else:
            console.print(f"[red]API Error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@batch.command(name="list")
@click.option(
    "--status",
    "-s",
    default=None,
    type=click.Choice(["QUEUED", "RUNNING", "PAUSED", "COMPLETED", "FAILED", "CANCELLED"], case_sensitive=False),
    help="Filter by job status"
)
@click.option(
    "--batch-id",
    "-b",
    default=None,
    help="Filter by batch ID"
)
@click.option(
    "--limit",
    "-l",
    default=20,
    type=int,
    help="Maximum number of jobs to display (default: 20)"
)
def list_jobs(status: Optional[str], batch_id: Optional[str], limit: int):
    """
    List batch jobs with optional filtering.

    Examples:
        # List all recent jobs
        python -m src.main_cli batch list

        # List only running jobs
        python -m src.main_cli batch list --status RUNNING

        # List jobs for a specific batch
        python -m src.main_cli batch list --batch-id my_batch_001

        # List last 50 jobs
        python -m src.main_cli batch list --limit 50
    """
    try:
        params = {"limit": limit}
        if status:
            params["status"] = status.upper()
        if batch_id:
            params["batch_id"] = batch_id

        response = httpx.get(
            f"{API_BASE_URL}/v1/jobs",
            params=params,
            timeout=10.0
        )
        response.raise_for_status()

        result = response.json()
        jobs = result.get("jobs", [])

        if not jobs:
            console.print("[yellow]No jobs found matching criteria[/yellow]")
            return

        # Create table
        table = Table(title=f"Batch Jobs ({result.get('total', len(jobs))} total)", show_header=True)
        table.add_column("Job ID", style="cyan", no_wrap=True)
        table.add_column("Batch ID", style="white")
        table.add_column("Status", style="white")
        table.add_column("Progress", justify="right")
        table.add_column("Docs", justify="right")
        table.add_column("Created", style="dim")

        for job in jobs:
            status_val = job.get("status", "UNKNOWN")
            status_color = {
                "QUEUED": "yellow",
                "RUNNING": "green",
                "PAUSED": "blue",
                "COMPLETED": "bold green",
                "FAILED": "red",
                "CANCELLED": "red"
            }.get(status_val, "white")

            progress = job.get("progress_percent", 0)
            processed = job.get("processed_documents", 0)
            total = job.get("total_documents", 0)

            table.add_row(
                job.get("job_id", "")[:16] + "...",
                job.get("batch_id", "N/A")[:20],
                f"[{status_color}]{status_val}[/{status_color}]",
                f"{progress:.1f}%",
                f"{processed}/{total}",
                job.get("created_at", "")[:19] if job.get("created_at") else "N/A"
            )

        console.print(table)

        if result.get("total", 0) > len(jobs):
            console.print(f"\n[dim]Showing {len(jobs)} of {result['total']} jobs. Use --limit to see more.[/dim]")

    except httpx.HTTPError as e:
        console.print(f"[red]API Error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@batch.command(name="watch")
@click.option(
    "--job-id",
    "-j",
    required=True,
    help="Job ID to watch"
)
@click.option(
    "--interval",
    "-i",
    default=2,
    type=int,
    help="Refresh interval in seconds (default: 2)"
)
def watch_job(job_id: str, interval: int):
    """
    Watch batch job progress in real-time.

    Continuously monitors and displays job status until completion.
    Press Ctrl+C to exit.

    Examples:
        # Watch with default 2-second interval
        python -m src.main_cli batch watch -j abc-123

        # Watch with 5-second interval
        python -m src.main_cli batch watch -j abc-123 --interval 5
    """
    try:
        console.print(f"[cyan]Watching job {job_id}...[/cyan] (Press Ctrl+C to exit)\n")

        with Live(console=console, refresh_per_second=1) as live:
            while True:
                try:
                    response = httpx.get(
                        f"{API_BASE_URL}/v1/jobs/{job_id}",
                        timeout=10.0
                    )
                    response.raise_for_status()
                    job = response.json()

                    # Create status display
                    table = Table(show_header=False, box=None, padding=(0, 2))
                    table.add_column("Field", style="cyan bold")
                    table.add_column("Value", style="white")

                    status = job.get("status", "UNKNOWN")
                    status_color = {
                        "QUEUED": "yellow",
                        "RUNNING": "green",
                        "PAUSED": "blue",
                        "COMPLETED": "bold green",
                        "FAILED": "red",
                        "CANCELLED": "red"
                    }.get(status, "white")

                    table.add_row("Status", f"[{status_color}]{status}[/{status_color}]")
                    table.add_row("Progress", f"{job.get('progress_percent', 0):.1f}%")
                    table.add_row("Processed", f"{job.get('processed_documents', 0)}/{job.get('total_documents', 0)}")

                    if job.get("failed_documents", 0) > 0:
                        table.add_row("Failed", f"[red]{job.get('failed_documents')}[/red]")

                    # Resource usage
                    if job.get("resource_usage"):
                        resource = job["resource_usage"]
                        table.add_row("CPU", f"{resource.get('cpu_percent', 0):.1f}%")
                        table.add_row("Memory", f"{resource.get('memory_percent', 0):.1f}%")

                    # Progress bar
                    progress_val = job.get("progress_percent", 0)
                    bar_width = 50
                    filled = int(bar_width * progress_val / 100)
                    bar = "█" * filled + "░" * (bar_width - filled)

                    # Build display using Group for proper rendering
                    from rich.console import Group
                    display = Group(
                        f"\n[bold]{bar}[/bold] {progress_val:.1f}%\n",
                        table,
                        f"\n[dim]Last updated: {time.strftime('%H:%M:%S')}[/dim]"
                    )

                    live.update(Panel(display, title=f"Job: {job_id[:24]}...", border_style=status_color))

                    # Exit if terminal state
                    if status in ["COMPLETED", "FAILED", "CANCELLED"]:
                        console.print(f"\n[bold]Job {status.lower()}![/bold]")
                        break

                    time.sleep(interval)

                except httpx.HTTPError as e:
                    if e.response.status_code == 404:
                        console.print(f"\n[red]Job not found: {job_id}[/red]")
                        break
                    else:
                        console.print(f"\n[yellow]API Error: {e}. Retrying...[/yellow]")
                        time.sleep(interval)
                except KeyboardInterrupt:
                    console.print("\n\n[yellow]Watch stopped by user[/yellow]")
                    break

    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        sys.exit(1)
