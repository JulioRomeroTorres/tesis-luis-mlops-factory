import { DataPoint } from "../types";

const BASE_PATH = import.meta.env.VITE_API_URL || 'https://model-backend-734156824859.us-east4.run.app';

export const getComparation = async (stationId: string, lowerDatetimeLimit: string, upperDatetimeLimit: string): Promise<DataPoint[]> => {
    
  const params = new URLSearchParams({
    lower_limit_datetime_inference: lowerDatetimeLimit,
    station_id: stationId,
    upper_limit_datetime_inference: upperDatetimeLimit
  });

  const completedUrl = `${BASE_PATH}?${params}`;
  
  try{
    const response = await fetch(completedUrl);
    if (!response.ok) {
      throw new Error(`Error en la petición: ${response.status}`);
    }

    const data: DataPoint[] = await response.json();
    return data;

  } catch (error) {
    console.error("Hubo un problema con la operación fetch:", error);
    return [];
  }

}