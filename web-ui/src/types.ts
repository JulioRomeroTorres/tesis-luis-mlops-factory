export interface DataPoint {
  reading_datetime: string;
  measured: number;
  predicted: number;
}

export interface ChartData {
  labels: string[];
  datasets: {
    label: string;
    data: number[];
    borderColor: string;
    backgroundColor: string;
  }[];
}
