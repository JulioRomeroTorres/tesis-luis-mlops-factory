from typing import Optional
from pydantic import BaseModel

class Pm25Inference(BaseModel):
    value: float
    reading_datetime: str
    station_id: str

class ComparationInference(BaseModel):
    predicted: Optional[float]
    measured: Optional[float]
    reading_datetime: str