import React from 'react';
import { Handle, Position, NodeProps, Node } from '@xyflow/react';
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

type ChartNodeData = {
  points: DataPoint[];
};

export const ChartNode = ({ data }: NodeProps<Node<ChartNodeData>>) => {
  const chartData = {
    labels: data.points.map(p => p.date_information),
    datasets: [
      {
        label: 'Predicción',
        data: data.points.map(p => p.variable_1),
        borderColor: 'orange',
        backgroundColor: 'rgba(255, 165, 0, 0.5)',
        tension: 0.1
      },
      {
        label: 'Variable 2',
        data: data.points.map(p => p.variable_2),
        borderColor: 'blue',
        backgroundColor: 'rgba(0, 0, 255, 0.5)',
        tension: 0.1
      }
    ]
  };

  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top' as const,
      },
      title: {
        display: true,
        text: 'Variable Comparison',
      },
    },
    scales: {
      y: {
        title: {
          display: true,
          text: 'PM2.5 (µg/m³)',
        },
      },
      x: {
        title: {
          display: true,
          text: 'Fecha de lectura (HH:MM:SS DD/MM/YYYY)',
        },
      }
    }
  };

  return (
    <div className="bg-white p-4 rounded-lg shadow-lg border-2 border-orange-500" style={{ width: 800, height: 500 }}>
      <Handle type="target" position={Position.Top} />
      <div className="h-[450px] w-full">
        {data.points.length > 0 ? (
          <Line data={chartData} options={options} />
        ) : (
          <div className="flex items-center justify-center h-full text-gray-400">
            No data available. Use the Control Node to fetch.
          </div>
        )}
      </div>
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
};
