import React from 'react';
import { Plus, X } from 'lucide-react';
import { Button } from '../ui/button';

interface PayoutTier {
  id: string;
  from: number;
  to: number;
  rate: number;
  isPercentage: boolean;
}

interface PayoutTierBuilderProps {
  tiers: PayoutTier[];
  onChange: (tiers: PayoutTier[]) => void;
  disabled?: boolean;
}

export function PayoutTierBuilder({ tiers, onChange, disabled = false }: PayoutTierBuilderProps) {
  const addTier = () => {
    const lastTier = tiers[tiers.length - 1];
    const newTier: PayoutTier = {
      id: crypto.randomUUID(),
      from: lastTier ? lastTier.to : 0,
      to: lastTier ? lastTier.to + 100000 : 100000,
      rate: 0,
      isPercentage: true
    };
    onChange([...tiers, newTier]);
  };

  const updateTier = (id: string, updates: Partial<PayoutTier>) => {
    onChange(tiers.map(tier => 
      tier.id === id ? { ...tier, ...updates } : tier
    ));
  };

  const removeTier = (id: string) => {
    onChange(tiers.filter(tier => tier.id !== id));
  };

  return (
    <div className="space-y-4">
      {tiers.map((tier, index) => (
        <div key={tier.id} className="flex items-center space-x-4">
          <div className="w-1/4">
            <label className="block text-sm font-medium text-gray-700">From</label>
            <input
              type="number"
              value={tier.from}
              onChange={(e) => updateTier(tier.id, { from: parseFloat(e.target.value) })}
              disabled={disabled || index === 0}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
            />
          </div>

          <div className="w-1/4">
            <label className="block text-sm font-medium text-gray-700">To</label>
            <input
              type="number"
              value={tier.to}
              onChange={(e) => updateTier(tier.id, { to: parseFloat(e.target.value) })}
              disabled={disabled}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
            />
          </div>

          <div className="w-1/4">
            <label className="block text-sm font-medium text-gray-700">Rate</label>
            <input
              type="number"
              value={tier.rate}
              onChange={(e) => updateTier(tier.id, { rate: parseFloat(e.target.value) })}
              disabled={disabled}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
            />
          </div>

          <div className="w-1/4">
            <label className="block text-sm font-medium text-gray-700">Type</label>
            <select
              value={tier.isPercentage ? 'percentage' : 'fixed'}
              onChange={(e) => updateTier(tier.id, { isPercentage: e.target.value === 'percentage' })}
              disabled={disabled}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
            >
              <option value="percentage">Percentage</option>
              <option value="fixed">Fixed Amount</option>
            </select>
          </div>

          {!disabled && (
            <div className="flex items-end">
              <Button
                variant="outline"
                size="sm"
                onClick={() => removeTier(tier.id)}
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          )}
        </div>
      ))}

      {!disabled && (
        <Button
          variant="outline"
          onClick={addTier}
        >
          <Plus className="h-4 w-4 mr-2" />
          Add Tier
        </Button>
      )}
    </div>
  );
}