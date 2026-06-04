import React, { useState } from 'react';

interface ControlPanelProps {
  onFetch: (startDate: string, endDate: string) => void;
  isLoading: boolean;
}

export const ControlPanel = ({ onFetch, isLoading }: ControlPanelProps) => {
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');

  const handleFetch = () => {
    let finalStart = startDate;
    let finalEnd = endDate;

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

    onFetch(finalStart, finalEnd);
  };

  return (
    <div className="bg-white p-6 rounded-xl shadow-md border border-gray-200 mb-8">
      <div className="flex flex-wrap items-end gap-6">
        <div className="flex-1 min-w-[200px] space-y-1">
          <label className="text-xs font-bold text-gray-500 uppercase">Start Date & Hour</label>
          <input
            type="datetime-local"
            className="w-full bg-gray-50 border-2 border-gray-200 rounded-lg p-3 focus:border-blue-500 focus:outline-none transition-all"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
          />
        </div>

        <div className="flex-1 min-w-[200px] space-y-1">
          <label className="text-xs font-bold text-gray-500 uppercase">End Date & Hour</label>
          <input
            type="datetime-local"
            className="w-full bg-gray-50 border-2 border-gray-200 rounded-lg p-3 focus:border-blue-500 focus:outline-none transition-all"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
          />
        </div>

        <button
          onClick={handleFetch}
          disabled={isLoading}
          className={`px-8 py-3.5 rounded-lg font-black text-white uppercase tracking-widest shadow-lg transition-all transform active:scale-95 min-w-[160px] ${
            isLoading 
              ? 'bg-gray-400 cursor-not-allowed' 
              : 'bg-blue-600 hover:bg-blue-700 hover:shadow-blue-200'
          }`}
        >
          {isLoading ? 'Fetching...' : 'Fetch Data'}
        </button>
      </div>
    </div>
  );
};
