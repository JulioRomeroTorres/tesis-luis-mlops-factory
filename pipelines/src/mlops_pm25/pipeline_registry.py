"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from .pipelines.ingest_meteorical_features.pipeline import create_pipeline as ingest_meteorical_features
from .pipelines.training.pipeline import create_pipeline as training
from .pipelines.prediction.pipeline import create_pipeline as prediction

def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()
    pipelines["__default__"] = sum(pipelines.values())
    pipelines["ingest_meteorical_features"] = ingest_meteorical_features()
    pipelines["trainig"] = training()
    pipelines["prediction"] = prediction()
    return pipelines
