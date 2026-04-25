"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from .pipelines.batch_process.pipeline import create_pipeline as create_batch_process
from .pipelines.export_to_bigquery.pipeline import create_pipeline as export_data_to_bigquery
from .pipelines.send_notificaction import create_pipeline as send_notificaction

def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()
    pipelines["__default__"] = sum(pipelines.values())
    pipelines["batch_process"] = create_batch_process()
    pipelines["export_to_bigquery"] = export_data_to_bigquery()
    pipelines["send_notification"] = send_notificaction()
    return pipelines
