import React, { useState, useEffect } from 'react';
import { Plus, X, AlertCircle } from 'lucide-react';
import { Button } from '../ui/button';

interface KpiField {
  name: string;
  sourceField: string;
  dataType: string;
}

interface Rule {
  id: string;
  field: string;
  operator: string;
  value: string;
}

interface RuleBuilderProps {
  rules: Rule[];
  onChange: (rules: Rule[]) => void;
  disabled?: boolean;
  kpiFields?: KpiField[];
  sectionName?: string;
}

const OPERATORS = {
  Number: ['=', '!=', '>', '<', '>=', '<='],
  String: ['=', '!=', 'CONTAINS', 'NOT CONTAINS', 'IN', 'NOT IN'],
  Date: ['=', '!=', '>', '<', '>=', '<=']
};

export function RuleBuilder({ rules, onChange, disabled = false, kpiFields = [], sectionName = '' }: RuleBuilderProps) {
  const [availableFields, setAvailableFields] = useState<KpiField[]>([]);
  const [error, setError] = useState<string>('');

  useEffect(() => {
    console.log(`RuleBuilder ${sectionName} - Received KPI fields:`, kpiFields);
    setAvailableFields(kpiFields);
    setError(kpiFields.length === 0 ? `No KPI fields available for ${sectionName}` : '');
  }, [kpiFields, sectionName]);

  const addRule = () => {
    const newRule: Rule = {
      id: crypto.randomUUID(),
      field: '',
      operator: '=',
      value: ''
    };
    onChange([...rules, newRule]);
  };

  const updateRule = (id: string, updates: Partial<Rule>) => {
    onChange(rules.map(rule => 
      rule.id === id ? { ...rule, ...updates } : rule
    ));
  };

  const removeRule = (id: string) => {
    onChange(rules.filter(rule => rule.id !== id));
  };

  const getFieldDataType = (fieldName: string): string => {
    const field = availableFields.find(f => f.name === fieldName);
    return field?.dataType || 'String';
  };

  if (error) {
    return (
      <div className="p-4 bg-gray-50 rounded-lg text-gray-500 text-center flex items-center justify-center space-x-2">
        <AlertCircle className="h-5 w-5" />
        <span>{error}</span>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {rules.map((rule) => {
        const dataType = getFieldDataType(rule.field);
        const availableOperators = OPERATORS[dataType as keyof typeof OPERATORS] || OPERATORS.String;

        return (
          <div key={rule.id} className="flex items-center space-x-4 p-4 bg-white rounded-lg border">
            <div className="flex-1">
              <select
                value={rule.field}
                onChange={(e) => {
                  const newField = e.target.value;
                  updateRule(rule.id, { 
                    field: newField,
                    operator: OPERATORS[getFieldDataType(newField) as keyof typeof OPERATORS][0],
                    value: ''
                  });
                }}
                disabled={disabled}
                className="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
              >
                <option value="">Select KPI Field</option>
                {availableFields.map(field => (
                  <option key={field.name} value={field.name}>
                    {field.name}
                  </option>
                ))}
              </select>
            </div>

            <div className="w-40">
              <select
                value={rule.operator}
                onChange={(e) => updateRule(rule.id, { operator: e.target.value })}
                disabled={disabled || !rule.field}
                className="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
              >
                {availableOperators.map(op => (
                  <option key={op} value={op}>{op}</option>
                ))}
              </select>
            </div>

            <div className="flex-1">
              {dataType === 'Number' && (
                <input
                  type="number"
                  value={rule.value}
                  onChange={(e) => updateRule(rule.id, { value: e.target.value })}
                  disabled={disabled || !rule.field}
                  className="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
                  placeholder="Enter value"
                />
              )}
              {dataType === 'Date' && (
                <input
                  type="date"
                  value={rule.value}
                  onChange={(e) => updateRule(rule.id, { value: e.target.value })}
                  disabled={disabled || !rule.field}
                  className="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
                />
              )}
              {dataType === 'String' && (
                <input
                  type="text"
                  value={rule.value}
                  onChange={(e) => updateRule(rule.id, { value: e.target.value })}
                  disabled={disabled || !rule.field}
                  className="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
                  placeholder="Enter value"
                />
              )}
            </div>

            {!disabled && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => removeRule(rule.id)}
              >
                <X className="h-4 w-4" />
              </Button>
            )}
          </div>
        );
      })}

      {!disabled && (
        <Button
          variant="outline"
          onClick={addRule}
        >
          <Plus className="h-4 w-4 mr-2" />
          Add Rule
        </Button>
      )}
    </div>
  );
}