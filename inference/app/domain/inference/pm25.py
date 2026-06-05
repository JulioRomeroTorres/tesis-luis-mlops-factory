import math
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

    def _normalize_float_value(self, value: Optional[float]):
        if (value is None) or (math.isnan(value)):
            return None
        
        return value

    def format_json(self):
        return {
            "predicted": self._normalize_float_value(self.predicted),
            "measured": self._normalize_float_value(self.measured),
            "reading_datetime": self.reading_datetime   
        }