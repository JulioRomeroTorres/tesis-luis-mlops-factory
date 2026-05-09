"""
This is a boilerplate pipeline 'dummy_execution'
generated using Kedro 0.19.15
"""
from pipelines.commons.utils import get_current_datetime

def get_datetime():
    current_datetime = get_current_datetime()
    print(f"Current Datetime {current_datetime}")
    return 
