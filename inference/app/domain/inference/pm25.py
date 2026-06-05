from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class Pm25Inference(BaseModel):
    value: Optional[float]
    reading_datetime: datetime
    station_id: str

    def format_json(self):
        return {
            "value": self.value,
            "station_id": self.station_id,
            "reading_datetime": self.reading_datetime.strftime("%d/%m/%Y%H:%M:") 
        }

class ComparationInference(BaseModel):
    predicted: Optional[float]
    measured: Optional[float]
    reading_datetime: str