export interface DataPoint {
  date_information: string;
  variable_1: number;
  variable_2: number;
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
