from __future__ import annotations

import logging
import os
import time
from typing import List

from pipeline.base import PipelineResult
from pipeline.base import TaskConfig

logger = logging.getLogger(__name__)


class TaskRunner:
    def run(self, tasks: List[TaskConfig]) -> List[PipelineResult]:
        results = []
        for task in tasks:
            results.append(self._run_one(task))
        return results

    def _run_one(self, task: TaskConfig) -> PipelineResult:
        start = time.monotonic()
        input_rows = 0
        try:
            df = task.source.read()
            input_rows = len(df)
            for transform in task.transformations:
                df = transform(df)
            if task.sink is not None:
                task.sink.write(df)
            output_rows = len(df)
            duration = round(time.monotonic() - start, 2)
            logger.info(
                f"task_ok name={task.name!r} input_rows={input_rows} " f"output_rows={output_rows} duration={duration}s"
            )
            return PipelineResult(
                name=task.name,
                description=task.description,
                output_blob=task.output_blob,
                input_rows=input_rows,
                output_rows=output_rows,
                duration_seconds=duration,
                status="ok",
            )
        except Exception as exc:
            duration = round(time.monotonic() - start, 2)
            logger.error(f"task_failed name={task.name!r} error={exc!r} duration={duration}s")
            return PipelineResult(
                name=task.name,
                description=task.description,
                output_blob=task.output_blob,
                input_rows=input_rows,
                output_rows=0,
                duration_seconds=duration,
                status="failed",
                error=str(exc),
            )


def write_step_summary(results: List[PipelineResult], title: str = "Pipeline Results") -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with open(summary_path, "a", encoding="utf-8") as f:
        f.write(f"## {title}\n\n")
        f.write("| Task | Description | Input | Output | Duration | Status |\n")
        f.write("|---|---|---|---|---|---|\n")
        for r in results:
            icon = "✅" if r.status == "ok" else "❌"
            f.write(
                f"| `{r.name}` | {r.description} | {r.input_rows:,} | {r.output_rows:,} "
                f"| {r.duration_seconds:.1f}s | {icon} |\n"
            )
        f.write("\n")
        f.write("```mermaid\n")
        f.write("graph LR\n")
        for r in results:
            safe_name = r.name.replace("-", "_").replace(" ", "_").replace("/", "_").replace(".", "_")
            safe_blob = r.output_blob.replace("/", "_").replace(".", "_").replace("-", "_")
            icon = "✅" if r.status == "ok" else "❌"
            f.write(f'    {safe_name}["{r.name} {icon}"] --> {safe_blob}["{r.output_blob}"]\n')
        f.write("```\n\n")


# Backward-compatible alias
PipelineRunner = TaskRunner
