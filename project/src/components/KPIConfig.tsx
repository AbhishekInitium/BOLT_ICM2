import React, { useState, useEffect } from 'react';
import { Plus, Save, Download, Upload, Trash2 } from 'lucide-react';
import { useAuthStore } from '../store/authStore';

interface KPI {
  id: string;
  name: string;
  description: string;
  dataSource: string;
  unit: string;
  target: number;
  weight: number;
}

export function KPIConfig() {
  const user = useAuthStore((state) => state.user);
  const [kpis, setKpis] = useState<KPI[]>([]);
  const [message, setMessage] = useState('');

  useEffect(() => {
    if (user?.clientId) {
      const savedKpis = localStorage.getItem(`KPIconfig_${user.clientId}`);
      if (savedKpis) {
        setKpis(JSON.parse(savedKpis));
      }
    }
  }, [user?.clientId]);

  const handleSave = () => {
    if (user?.clientId) {
      localStorage.setItem(`KPIconfig_${user.clientId}`, JSON.stringify(kpis));
      setMessage('KPI Configuration saved successfully');
      setTimeout(() => setMessage(''), 3000);
    }
  };

  const handleExport = () => {
    const dataStr = JSON.stringify(kpis, null, 2);
    const dataBlob = new Blob([dataStr], { type: 'application/json' });
    const url = URL.createObjectURL(dataBlob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${user?.clientId}_kpi_config.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const handleImport = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const importedKpis = JSON.parse(e.target?.result as string);
          setKpis(importedKpis);
          setMessage('KPI Configuration imported successfully');
          setTimeout(() => setMessage(''), 3000);
        } catch (error) {
          setMessage('Error importing KPI configuration');
          setTimeout(() => setMessage(''), 3000);
        }
      };
      reader.readAsText(file);
    }
  };

  const addKPI = () => {
    const newKPI: KPI = {
      id: crypto.randomUUID(),
      name: '',
      description: '',
      dataSource: 'manual',
      unit: '',
      target: 0,
      weight: 1
    };
    setKpis([...kpis, newKPI]);
  };

  const updateKPI = (id: string, updates: Partial<KPI>) => {
    setKpis(kpis.map(kpi => kpi.id === id ? { ...kpi, ...updates } : kpi));
  };

  const deleteKPI = (id: string) => {
    setKpis(kpis.filter(kpi => kpi.id !== id));
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold text-gray-900">KPI Configuration</h1>
        <div className="flex space-x-3">
          <button
            onClick={handleSave}
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-indigo-600 hover:bg-indigo-700"
          >
            <Save className="h-4 w-4 mr-2" />
            Save
          </button>
          <button
            onClick={handleExport}
            className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-green-600 hover:bg-green-700"
          >
            <Download className="h-4 w-4 mr-2" />
            Export
          </button>
          <label className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 cursor-pointer">
            <Upload className="h-4 w-4 mr-2" />
            Import
            <input
              type="file"
              accept=".json"
              onChange={handleImport}
              className="hidden"
            />
          </label>
        </div>
      </div>

      {message && (
        <div className="bg-green-50 border border-green-200 rounded-md p-4">
          <p className="text-green-700">{message}</p>
        </div>
      )}

      <div className="bg-white shadow rounded-lg">
        <div className="px-4 py-5 sm:p-6">
          <button
            onClick={addKPI}
            className="mb-6 inline-flex items-center px-4 py-2 border border-gray-300 shadow-sm text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50"
          >
            <Plus className="h-4 w-4 mr-2" />
            Add KPI
          </button>

          <div className="space-y-6">
            {kpis.map((kpi) => (
              <div key={kpi.id} className="border border-gray-200 rounded-lg p-4">
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                  <div>
                    <label className="block text-sm font-medium text-gray-700">Name</label>
                    <input
                      type="text"
                      value={kpi.name}
                      onChange={(e) => updateKPI(kpi.id, { name: e.target.value })}
                      className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700">Data Source</label>
                    <select
                      value={kpi.dataSource}
                      onChange={(e) => updateKPI(kpi.id, { dataSource: e.target.value })}
                      className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                    >
                      <option value="manual">Manual Input</option>
                      <option value="excel">Excel Import</option>
                      <option value="api">API Integration</option>
                    </select>
                  </div>

                  <div className="sm:col-span-2">
                    <label className="block text-sm font-medium text-gray-700">Description</label>
                    <textarea
                      rows={2}
                      value={kpi.description}
                      onChange={(e) => updateKPI(kpi.id, { description: e.target.value })}
                      className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700">Unit</label>
                    <input
                      type="text"
                      value={kpi.unit}
                      onChange={(e) => updateKPI(kpi.id, { unit: e.target.value })}
                      className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700">Target</label>
                    <input
                      type="number"
                      value={kpi.target}
                      onChange={(e) => updateKPI(kpi.id, { target: parseFloat(e.target.value) })}
                      className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700">Weight</label>
                    <input
                      type="number"
                      value={kpi.weight}
                      onChange={(e) => updateKPI(kpi.id, { weight: parseFloat(e.target.value) })}
                      className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                    />
                  </div>

                  <div className="flex items-end">
                    <button
                      onClick={() => deleteKPI(kpi.id)}
                      className="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
                    >
                      <Trash2 className="h-4 w-4 mr-2" />
                      Delete
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}