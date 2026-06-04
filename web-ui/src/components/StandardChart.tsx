import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  ChartOptions
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { DataPoint } from '../types';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

interface StandardChartProps {
  points: DataPoint[];
}

export const StandardChart = ({ points }: StandardChartProps) => {
  const chartData = {
    labels: points.map(p => p.reading_datetime),
    datasets: [
      {
        label: 'Predicción',
        data: points.map(p => p.predicted),
        borderColor: 'orange',
        backgroundColor: 'rgba(255, 165, 0, 0.2)',
        borderWidth: 3,
        pointRadius: 4,
        pointBackgroundColor: 'orange',
        tension: 0.2,
        fill: true,
      },
      {
        label: 'Lectura Senamhi',
        data: points.map(p => p.measured),
        borderColor: 'blue',
        backgroundColor: 'rgba(0, 0, 255, 0.1)',
        borderWidth: 3,
        pointRadius: 4,
        pointBackgroundColor: 'blue',
        tension: 0.2,
        fill: true,
      }
    ]
  };

  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top' as const,
        labels: {
          font: {
            size: 14,
            weight: 'bold'
          }
        }
      },
      title: {
        display: true,
        text: 'Telemetry Data Analysis',
        font: {
          size: 20,
          weight: 'bold'
        },
        padding: 20
      },
      tooltip: {
        mode: 'index',
        intersect: false,
      }
    },
    scales: {
      y: {
        beginAtZero: true,
        grid: {
          color: '#f0f0f0'
        },
        title: {
          display: true,
          text: 'PM 2.5 (µg/m³)',
          font: {
            size: 14,
            weight: 'bold'
          }
        },
      },
      x: {
        grid: {
          display: false
        },
        title: {
          display: true,
          text: 'Fecha de Lectura (HH:MM:SS DD/MM/YYYY)',
          font: {
            size: 14,
            weight: 'bold'
          }
        },
      }
    }
  };

  return (
    <div className="bg-white p-8 rounded-xl shadow-lg border border-gray-200 h-[600px] w-full">
      {points.length > 0 ? (
        <Line data={chartData} options={options} />
      ) : (
        <div className="flex flex-col items-center justify-center h-full text-gray-400 space-y-4">
          <svg className="w-16 h-16 opacity-20" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
          <p className="text-lg font-medium">No data to display. Please select a date range and fetch.</p>
        </div>
      )}
    </div>
  );
};
