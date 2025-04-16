import React from 'react';
import { Plus, X, AlertCircle } from 'lucide-react';
import { Button } from '../ui/button';
import { Card } from '../ui/card';
import type { AdjustmentRule, KpiField } from '../../types';

interface AdjustmentRuleBuilderProps {
  rules: AdjustmentRule[];
  onChange: (rules: AdjustmentRule[]) => void;
  disabled?: boolean;
  kpiFields?: KpiField[];
  sectionName?: string;
}

const OPERATORS = {
  Number: ['=', '!=', '>', '<', '>=', '<='],
  String: ['=', '!=', 'CONTAINS', 'NOT CONTAINS', 'IN', 'NOT IN'],
  Date: ['=', '!=', '>', '<', '>=', '<=']
};

const TARGETS = ['Payout', 'Quota', 'Rate'] as const;
const ADJUSTMENT_TYPES = ['percentage', 'fixed'] as const;

export function AdjustmentRuleBuilder({ 
  rules, 
  onChange, 
  disabled = false, 
  kpiFields = [],
  sectionName = ''
}: AdjustmentRuleBuilderProps) {
  const addRule = () => {
    const newRule: AdjustmentRule = {
      id: crypto.randomUUID(),
      condition: {
        field: '',
        operator: '=',
        value: ''
      },
      adjustment: {
        target: 'Payout',
        type: 'percentage',
        value: 0
      }
    };
    onChange([...rules, newRule]);
  };

  const updateRule = (id: string, updates: Partial<AdjustmentRule>) => {
    onChange(rules.map(rule => 
      rule.id === id ? { ...rule, ...updates } : rule
    ));
  };

  const removeRule = (id: string) => {
    onChange(rules.filter(rule => rule.id !== id));
  };

  const getFieldDataType = (fieldName: string): string => {
    const field = kpiFields.find(f => f.name === fieldName);
    return field?.dataType || 'String';
  };

  if (kpiFields.length === 0) {
    return (
      <div className="p-4 bg-gray-50 rounded-lg text-gray-500 text-center">
        No KPI fields available for adjustment rules
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {rules.map((rule) => {
        const dataType = getFieldDataType(rule.condition.field);
        const availableOperators = OPERATORS[dataType as keyof typeof OPERATORS] || OPERATORS.String;

        return (
          <Card key={rule.id} className="p-6 border border-gray-200">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-4">
                <h4 className="font-medium text-gray-700">Condition</h4>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Field</label>
                    <select
                      value={rule.condition.field}
                      onChange={(e) => updateRule(rule.id, { 
                        condition: { ...rule.condition, field: e.target.value }
                      })}
                      disabled={disabled}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                    >
                      <option value="">Select Field</option>
                      {kpiFields.map(field => (
                        <option key={field.name} value={field.name}>
                          {field.name}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Operator</label>
                    <select
                      value={rule.condition.operator}
                      onChange={(e) => updateRule(rule.id, { 
                        condition: { ...rule.condition, operator: e.target.value }
                      })}
                      disabled={disabled || !rule.condition.field}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                    >
                      {availableOperators.map(op => (
                        <option key={op} value={op}>{op}</option>
                      ))}
                    </select>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Value</label>
                  {dataType === 'Number' ? (
                    <input
                      type="number"
                      value={rule.condition.value}
                      onChange={(e) => updateRule(rule.id, { 
                        condition: { ...rule.condition, value: e.target.value }
                      })}
                      disabled={disabled || !rule.condition.field}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                    />
                  ) : dataType === 'Date' ? (
                    <input
                      type="date"
                      value={rule.condition.value}
                      onChange={(e) => updateRule(rule.id, { 
                        condition: { ...rule.condition, value: e.target.value }
                      })}
                      disabled={disabled || !rule.condition.field}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                    />
                  ) : (
                    <input
                      type="text"
                      value={rule.condition.value}
                      onChange={(e) => updateRule(rule.id, { 
                        condition: { ...rule.condition, value: e.target.value }
                      })}
                      disabled={disabled || !rule.condition.field}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                      placeholder="Enter value"
                    />
                  )}
                </div>
              </div>

              <div className="space-y-4">
                <h4 className="font-medium text-gray-700">Adjustment</h4>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Target</label>
                    <select
                      value={rule.adjustment.target}
                      onChange={(e) => updateRule(rule.id, { 
                        adjustment: { 
                          ...rule.adjustment, 
                          target: e.target.value as AdjustmentRule['adjustment']['target']
                        }
                      })}
                      disabled={disabled}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                    >
                      {TARGETS.map(target => (
                        <option key={target} value={target}>{target}</option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Type</label>
                    <select
                      value={rule.adjustment.type}
                      onChange={(e) => updateRule(rule.id, { 
                        adjustment: { 
                          ...rule.adjustment, 
                          type: e.target.value as AdjustmentRule['adjustment']['type']
                        }
                      })}
                      disabled={disabled}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                    >
                      {ADJUSTMENT_TYPES.map(type => (
                        <option key={type} value={type}>
                          {type.charAt(0).toUpperCase() + type.slice(1)}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Value</label>
                  <div className="relative">
                    <input
                      type="number"
                      value={rule.adjustment.value}
                      onChange={(e) => updateRule(rule.id, { 
                        adjustment: { ...rule.adjustment, value: parseFloat(e.target.value) || 0 }
                      })}
                      disabled={disabled}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                    />
                    <span className="absolute right-3 top-2 text-gray-500">
                      {rule.adjustment.type === 'percentage' ? '%' : ''}
                    </span>
                  </div>
                </div>

                {!disabled && (
                  <div className="pt-4">
                    <Button
                      variant="outline"
                      onClick={() => removeRule(rule.id)}
                      className="w-full rounded-full hover:bg-gray-100 transition"
                    >
                      <X className="h-4 w-4 mr-2" />
                      Remove Rule
                    </Button>
                  </div>
                )}
              </div>
            </div>
          </Card>
        );
      })}

      {!disabled && (
        <Button
          variant="outline"
          onClick={addRule}
          className="w-full rounded-full hover:bg-gray-100 transition"
        >
          <Plus className="h-4 w-4 mr-2" />
          Add Adjustment Rule
        </Button>
      )}
    </div>
  );
}