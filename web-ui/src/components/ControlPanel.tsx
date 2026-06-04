import React, { useState } from 'react';

interface ControlPanelProps {
  onFetch: (stationId: string, startDate: string, endDate: string) => void;
  isLoading: boolean;
}

export const ControlPanel = ({ onFetch, isLoading }: ControlPanelProps) => {
  const [startDate, setStartDate] = useState('');
  const [stationId, setStationId] = useState('');
  const [endDate, setEndDate] = useState('');
  const [error, setError] = useState<string | null>(null);

  const handleFetch = () => {
    setError(null);

    let finalStart = startDate;
    let finalEnd = endDate;

    // Default handling if empty
    if (!finalStart) {
      const d = new Date();
      d.setHours(0, 0, 0, 0);
      // Format to YYYY-MM-DDTHH:00
      finalStart = d.toISOString().slice(0, 11) + "00:00";
    } else {
      // Ensure it ends in :00 if the picker allowed minutes
      finalStart = finalStart.slice(0, 14) + "00";
    }
    
    if (!finalEnd) {
      const d = new Date();
      d.setHours(23, 0, 0, 0);
      finalEnd = d.toISOString().slice(0, 11) + "23:00";
    } else {
      finalEnd = finalEnd.slice(0, 14) + "00";
    }

    // Validation: End must be after Start
    const startObj = new Date(finalStart);
    const endObj = new Date(finalEnd);

    if (endObj <= startObj) {
      setError("Validation Error: End date/hour must be strictly after the start date/hour.");
      return;
    }

    onFetch(stationId, finalStart, finalEnd);
  };

  return (
    <div className="bg-white p-6 rounded-xl shadow-md border border-gray-200 mb-8">
      <div className="flex flex-wrap items-end gap-6">
        <div className="flex-1 min-w-[200px] space-y-1">
          <label className="text-xs font-bold text-gray-500 uppercase">Start Date & Hour</label>
          <input
            type="datetime-local"
            step="3600" // Hint for hour-only increments in some browsers
            className="w-full bg-gray-50 border-2 border-gray-200 rounded-lg p-3 focus:border-blue-500 focus:outline-none transition-all"
            value={startDate}
            onChange={(e) => {
              // Extracting only YYYY-MM-DDTHH:00
              const value = e.target.value;
              if (value) {
                setStartDate(value.slice(0, 14) + "00");
              } else {
                setStartDate("");
              }
            }}
          />
          <p className="text-[10px] text-gray-400 font-medium">Format: DD/MM/YYYY HH:00</p>
        </div>

        <div className="flex-1 min-w-[200px] space-y-1">
          <label className="text-xs font-bold text-gray-500 uppercase">End Date & Hour</label>
          <input
            type="datetime-local"
            step="3600"
            className="w-full bg-gray-50 border-2 border-gray-200 rounded-lg p-3 focus:border-blue-500 focus:outline-none transition-all"
            value={endDate}
            onChange={(e) => {
              const value = e.target.value;
              if (value) {
                setEndDate(value.slice(0, 14) + "00");
              } else {
                setEndDate("");
              }
            }}
          />
          <p className="text-[10px] text-gray-400 font-medium">Format: DD/MM/YYYY HH:00</p>
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

      {error && (
        <div className="mt-4 p-3 bg-red-50 border-l-4 border-red-500 text-red-700 text-sm font-bold rounded flex items-center gap-2">
          <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
          </svg>
          {error}
        </div>
      )}
    </div>
  );
};
