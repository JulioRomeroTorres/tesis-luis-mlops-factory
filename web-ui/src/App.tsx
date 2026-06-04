import React, { useState, useCallback } from 'react';
import { ControlPanel } from './components/ControlPanel';
import { StandardChart } from './components/StandardChart';
import { DataPoint } from './types';

export default function App() {
  const [dataPoints, setDataPoints] = useState<DataPoint[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  const handleFetch = useCallback(async (start: string, end: string) => {
    setIsLoading(true);

    try {
      console.log(`Fetching telemetry data from ${start} to ${end}`);
      
      // Simulating API latency
      await new Promise(resolve => setTimeout(resolve, 1000)); 
      
      const mockData: DataPoint[] = [
        { "date_information": "10:00:00 12/10/2023", "variable_1": 11, "variable_2": 12 },
        { "date_information": "11:00:00 12/10/2023", "variable_1": 13, "variable_2": 14 },
        { "date_information": "12:00:00 12/10/2023", "variable_1": 25, "variable_2": 8 },
        { "date_information": "13:00:00 12/10/2023", "variable_1": 18, "variable_2": 22 },
        { "date_information": "14:00:00 12/10/2023", "variable_1": 30, "variable_2": 15 },
        { "date_information": "15:00:00 12/10/2023", "variable_1": 22, "variable_2": 19 },
        { "date_information": "16:00:00 12/10/2023", "variable_1": 28, "variable_2": 25 },
        { "date_information": "17:00:00 12/10/2023", "variable_1": 15, "variable_2": 30 },
      ];

      setDataPoints(mockData);
    } catch (error) {
      console.error('Error fetching data:', error);
    } finally {
      setIsLoading(false);
    }
  }, []);

  return (
    <div className="min-h-screen bg-slate-50 p-8">
      <div className="max-w-7xl mx-auto">
        <header className="mb-10 flex items-center justify-between">
          <div>
            <h1 className="text-4xl font-black text-slate-900 tracking-tight">
              AI AGENT <span className="text-blue-600">HUB</span>
            </h1>
            <p className="text-slate-500 font-medium">Environmental Telemetry Dashboard</p>
          </div>
          <div className="bg-white px-4 py-2 rounded-lg border border-slate-200 shadow-sm">
            <span className="text-xs font-bold text-slate-400 uppercase">System Status: </span>
            <span className="text-xs font-black text-green-500 uppercase">Operational</span>
          </div>
        </header>

        <main className="space-y-6">
          <ControlPanel onFetch={handleFetch} isLoading={isLoading} />
          <StandardChart points={dataPoints} />
        </main>

        <footer className="mt-12 pt-8 border-t border-slate-200 text-center text-slate-400 text-xs font-bold uppercase tracking-widest">
          &copy; 2026 MLOPS FACTORY - PROTOTYPE v2.0
        </footer>
      </div>
    </div>
  );
}
