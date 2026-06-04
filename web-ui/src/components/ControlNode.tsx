import React, { useState } from 'react';
import { Handle, Position, NodeProps, Node } from '@xyflow/react';

type ControlNodeData = {
  onFetch: (startDate: string, endDate: string) => void;
  isLoading: boolean;
};

export const ControlNode = ({ data }: NodeProps<Node<ControlNodeData>>) => {
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');

  const handleFetch = () => {
    let finalStart = startDate;
    let finalEnd = endDate;

    // Logic to handle missing hours if we were using text inputs, 
    // but with datetime-local we get YYYY-MM-DDTHH:mm.
    // If empty, we can notify or use current date with defaults.
    
    if (!finalStart) {
      const d = new Date();
      d.setHours(0, 0, 0, 0);
      finalStart = d.toISOString().slice(0, 16);
    }
    
    if (!finalEnd) {
      const d = new Date();
      d.setHours(23, 59, 0, 0);
      finalEnd = d.toISOString().slice(0, 16);
    }

    data.onFetch(finalStart, finalEnd);
  };

  return (
    <div className="bg-white p-6 rounded-xl shadow-2xl border-2 border-blue-500 w-96">
      <div className="flex items-center gap-2 mb-4">
        <div className="w-3 h-3 bg-blue-500 rounded-full animate-pulse"></div>
        <h3 className="text-xl font-black text-gray-800 uppercase tracking-wider">Data Fetcher</h3>
      </div>
      
      <div className="space-y-4">
        <div className="space-y-1">
          <label className="text-xs font-bold text-gray-500 uppercase">Start Timestamp</label>
          <input
            type="datetime-local"
            className="w-full bg-gray-50 border-2 border-gray-200 rounded-lg p-3 focus:border-blue-500 focus:outline-none transition-all"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            placeholder="YYYY-MM-DD 00:00"
          />
        </div>

        <div className="space-y-1">
          <label className="text-xs font-bold text-gray-500 uppercase">End Timestamp</label>
          <input
            type="datetime-local"
            className="w-full bg-gray-50 border-2 border-gray-200 rounded-lg p-3 focus:border-blue-500 focus:outline-none transition-all"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            placeholder="YYYY-MM-DD 23:59"
          />
        </div>

        <button
          onClick={handleFetch}
          disabled={data.isLoading}
          className={`w-full py-4 rounded-lg font-black text-white uppercase tracking-widest shadow-lg transition-all transform active:scale-95 ${
            data.isLoading 
              ? 'bg-gray-400 cursor-not-allowed' 
              : 'bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 hover:shadow-blue-200 hover:-translate-y-1'
          }`}
        >
          {data.isLoading ? (
            <span className="flex items-center justify-center gap-2">
              <svg className="animate-spin h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
              Processing
            </span>
          ) : 'Execute Fetch'}
        </button>
      </div>
      
      <div className="mt-4 pt-4 border-t border-gray-100 flex justify-between items-center text-[10px] text-gray-400 font-bold uppercase">
        <span>Status: {data.isLoading ? 'Syncing' : 'Ready'}</span>
        <span>v1.0.0</span>
      </div>

      <Handle type="source" position={Position.Bottom} className="w-3 h-3 bg-blue-500 border-2 border-white" />
    </div>
  );
};
