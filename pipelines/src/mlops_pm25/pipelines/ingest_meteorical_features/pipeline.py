"""
This is a boilerplate pipeline 'ingest_meteorical_features'
generated using Kedro 0.19.15
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import get_meteorological_data_by_station, ingest_features

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func= get_meteorological_data_by_station,
            inputs= ["params:stations_ids", "params:features_names",
                     "params:senamhi_enpoints.features", "params:start_period",
                     "params:end_period"],
            outputs="meteorological_data",
            name="Get_Meteorolofical_data"
        ),
        node(
            func= ingest_features,
            inputs= ["params:db_name", "params:table_name", "meteorological_data"],
            outputs=None,
            name="Ingest_feature"
        ),
    ])
