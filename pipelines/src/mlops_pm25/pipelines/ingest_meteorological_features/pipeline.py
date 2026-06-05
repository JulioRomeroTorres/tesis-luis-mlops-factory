"""
This is a boilerplate pipeline 'ingest_meteorical_features'
generated using Kedro 0.19.15
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import get_meteorological_data_by_station, ingest_features, add_date_columns    

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func= get_meteorological_data_by_station,
            inputs= ["params:stations_ids", "params:features_names",
                     "params:senamhi_enpoint", "params:start_period",
                     "params:end_period"],
            outputs="meteorological_data",
            name="Get_Meteorolofical_data"
        ),
        node(
            func= add_date_columns,
            inputs= ["meteorological_data"],
            outputs="formatted_meteorological_data",
            name="Add_Formatted_Date_Columns"
        ),
        node(
            func= ingest_features,
            inputs= ["params:db_name", "params:table_name", "formatted_meteorological_data"],
            outputs=None,
            name="Ingest_feature"
        ),
    ])
