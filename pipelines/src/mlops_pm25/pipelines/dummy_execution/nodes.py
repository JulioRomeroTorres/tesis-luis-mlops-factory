"""
This is a boilerplate pipeline 'dummy_execution'
generated using Kedro 0.19.15
"""
from typing import Any
from mlops_pm25.pipelines.commons.utils import get_current_datetime

def get_datetime(features_names: Any, start_period: str):
    print(f"features_names: {features_names} type {type(features_names)}, start_period: {start_period} type {type(start_period)}")
    current_datetime = get_current_datetime()
    print(f"Current Datetime {current_datetime}")
    return 
