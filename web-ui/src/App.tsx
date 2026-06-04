import React, { useState, useCallback } from 'react';
import { ControlPanel } from './components/ControlPanel';
import { StandardChart } from './components/StandardChart';
import { DataPoint } from './types';
import { getComparation } from './api/inference';

export default function App() {
  const [dataPoints, setDataPoints] = useState<DataPoint[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  const handleFetch = useCallback(async (stationId:string, start: string, end: string) => {
    setIsLoading(true);

    try {
      console.log(`Fetching telemetry data from ${start} to ${end}`);
      const environmentalData: DataPoint[] = await getComparation(stationId, start, end)
      setDataPoints(environmentalData);
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
              Tesis  <span className="text-blue-600">Luis Romero</span> 
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
