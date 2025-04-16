import React from 'react';
import { Plus, X, AlertCircle } from 'lucide-react';
import { Button } from '../ui/button';
import { Card } from '../ui/card';

interface CreditSplit {
  id: string;
  role: string;
  percentage: number;
}

interface CreditSplitTableProps {
  splits: CreditSplit[];
  hierarchyFile: string;
  onSplitsChange: (splits: CreditSplit[]) => void;
  onHierarchyFileChange: (file: string) => void;
  disabled?: boolean;
  uploadedFiles?: Record<string, { columns: string[] }>;
}

export function CreditSplitTable({ 
  splits, 
  hierarchyFile,
  onSplitsChange, 
  onHierarchyFileChange,
  disabled = false,
  uploadedFiles = {}
}: CreditSplitTableProps) {
  const [error, setError] = React.useState<string>('');

  const validateSplits = () => {
    const totalPercentage = splits.reduce((sum, split) => sum + split.percentage, 0);
    
    if (totalPercentage !== 100) {
      setError('Total percentage must equal 100%');
      return false;
    }

    if (splits.length === 0) {
      setError('At least one credit split is required');
      return false;
    }

    setError('');
    return true;
  };

  React.useEffect(() => {
    if (!disabled) {
      validateSplits();
    }
  }, [splits, disabled]);

  return (
    <div className="space-y-8">
      <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
        <h3 className="text-lg font-medium text-gray-900 mb-6">Credit Split Configuration</h3>
        
        <div className="space-y-6">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Hierarchy Source File
            </label>
            <input
              type="text"
              value={hierarchyFile}
              onChange={(e) => onHierarchyFileChange(e.target.value)}
              disabled={disabled}
              className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
              placeholder="e.g., MH_DEC.csv"
            />
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Role
                  </th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Percentage
                  </th>
                  {!disabled && (
                    <th scope="col" className="relative px-6 py-3">
                      <span className="sr-only">Actions</span>
                    </th>
                  )}
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {splits.map((split) => (
                  <tr key={split.id}>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <input
                        type="text"
                        value={split.role}
                        onChange={(e) => {
                          const newSplits = splits.map(s => 
                            s.id === split.id ? { ...s, role: e.target.value } : s
                          );
                          onSplitsChange(newSplits);
                        }}
                        disabled={disabled}
                        className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
                        placeholder="Enter role"
                      />
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <input
                        type="number"
                        min="0"
                        max="100"
                        value={split.percentage}
                        onChange={(e) => {
                          const newSplits = splits.map(s => 
                            s.id === split.id ? { ...s, percentage: parseFloat(e.target.value) || 0 } : s
                          );
                          onSplitsChange(newSplits);
                        }}
                        disabled={disabled}
                        className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
                      />
                    </td>
                    {!disabled && (
                      <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => {
                            const newSplits = splits.filter(s => s.id !== split.id);
                            onSplitsChange(newSplits);
                          }}
                          className="rounded-full hover:bg-gray-100 transition"
                        >
                          <X className="h-4 w-4" />
                        </Button>
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    Total
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {splits.reduce((sum, split) => sum + split.percentage, 0)}%
                  </td>
                  <td />
                </tr>
              </tfoot>
            </table>
          </div>

          {!disabled && (
            <Button
              variant="outline"
              onClick={() => {
                const newSplit: CreditSplit = {
                  id: crypto.randomUUID(),
                  role: '',
                  percentage: 0
                };
                onSplitsChange([...splits, newSplit]);
              }}
              className="w-full rounded-full hover:bg-gray-100 transition"
            >
              <Plus className="h-4 w-4 mr-2" />
              Add Split
            </Button>
          )}

          {error && (
            <div className="p-4 bg-red-50 rounded-md">
              <div className="flex">
                <AlertCircle className="h-5 w-5 text-red-400" />
                <div className="ml-3">
                  <p className="text-sm text-red-700">{error}</p>
                </div>
              </div>
            </div>
          )}
        </div>
      </Card>
    </div>
  );
}