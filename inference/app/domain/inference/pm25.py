from pydantic import BaseModel

class Pm25Inference(BaseModel):
    value: float
    reading_datetime: str
    station_id: str