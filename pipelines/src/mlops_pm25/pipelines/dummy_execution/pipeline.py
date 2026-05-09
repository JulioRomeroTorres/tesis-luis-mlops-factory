"""
This is a boilerplate pipeline 'dummy_execution'
generated using Kedro 0.19.15
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import get_datetime

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func= get_datetime,
            inputs= None,
            outputs=None,
            name="Get_Current_Datetime_Node"
        ),
    ])
