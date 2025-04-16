import React, { useState, useRef, useEffect } from 'react';
import { format } from 'date-fns';
import { Plus, Save, Edit2, X, FileUp, Calculator, Upload, AlertCircle, Check } from 'lucide-react';
import { Button } from './ui/button';
import { Card } from './ui/card';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './ui/tabs';
import { useAuthStore } from '../store/authStore';
import { RuleBuilder } from './scheme/RuleBuilder';
import { AdjustmentRuleBuilder } from './scheme/AdjustmentRuleBuilder';
import { PayoutTierBuilder } from './scheme/PayoutTierBuilder';
import { CreditSplitTable } from './scheme/CreditSplitTable';
import type { SchemeConfig, KpiConfig, CustomRule } from '../types';

type Mode = 'initial' | 'new' | 'view' | 'edit';

interface UploadedFile {
  name: string;
  data: any[];
  columns: string[];
}

const DEFAULT_CONFIG: SchemeConfig = {
  name: '',
  description: '',
  effectiveFrom: '',
  effectiveTo: '',
  quotaAmount: 0,
  revenueBase: '',
  baseMapping: {
    sourceFile: '',
    agentField: '',
    amountField: ''
  },
  qualificationRules: [],
  adjustmentRules: [],
  exclusionRules: [],
  creditRules: [],
  creditSplits: [],
  creditHierarchyFile: '',
  payoutTiers: [],
  customRules: [],
  kpiConfig: undefined
};

const EVALUATION_LEVELS = ['Agent', 'Team', 'Region', 'Per Record'];
const METRIC_TYPES = ['Count', 'Sum', 'Average', 'Minimum', 'Maximum'];
const PERIOD_TYPES = ['Monthly', 'Quarterly', 'Yearly'];

export function SchemeDesigner() {
  const user = useAuthStore((state) => state.user);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const kpiConfigInputRef = useRef<HTMLInputElement>(null);
  const additionalKpiConfigInputRef = useRef<HTMLInputElement>(null);
  const originalConfigRef = useRef<SchemeConfig | null>(null);
  const [mode, setMode] = useState<Mode>('initial');
  const [loadedKpiConfig, setLoadedKpiConfig] = useState<KpiConfig | null>(null);
  const [activeSection, setActiveSection] = useState<string>('base');
  const [hasChanges, setHasChanges] = useState(false);
  const [globalError, setGlobalError] = useState<string>('');
  const [uploadedFiles, setUploadedFiles] = useState<Record<string, UploadedFile>>({});
  const [config, setConfig] = useState<SchemeConfig>(DEFAULT_CONFIG);
  const [validationErrors, setValidationErrors] = useState<Record<string, string>>({});
  const [loadingErrors, setLoadingErrors] = useState<Record<string, string>>({});
  const [successMessage, setSuccessMessage] = useState<string>('');
  const [activeTab, setActiveTab] = useState('rules');

  const handleFileLoad = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const loadedConfig = JSON.parse(e.target?.result as string);
        console.log('Loaded scheme:', loadedConfig);
        
        const requiredFields = [
          'name',
          'description',
          'effectiveFrom',
          'effectiveTo',
          'quotaAmount',
          'revenueBase',
          'baseMapping',
          'qualificationRules',
          'adjustmentRules',
          'exclusionRules',
          'creditRules',
          'creditSplits',
          'payoutTiers',
          'customRules'
        ];

        for (const field of requiredFields) {
          if (!(field in loadedConfig)) {
            throw new Error(`Missing required field: ${field}`);
          }
        }

        if (!loadedConfig.status) {
          loadedConfig.status = 'Draft';
        }
        
        if (!loadedConfig.id) {
          loadedConfig.id = crypto.randomUUID();
        }

        originalConfigRef.current = JSON.parse(JSON.stringify(loadedConfig));
        
        setConfig(loadedConfig);
        if (loadedConfig.kpiConfig) {
          setLoadedKpiConfig(loadedConfig.kpiConfig);
        }
        setMode('view');
        setGlobalError('');
        setValidationErrors({});
        setLoadingErrors({});
        setSuccessMessage('Scheme loaded successfully');
        setTimeout(() => setSuccessMessage(''), 3000);
      } catch (err) {
        console.error('Error parsing scheme:', err);
        setLoadingErrors({
          parse: `Failed to load scheme: ${err instanceof Error ? err.message : 'Invalid format'}`
        });
      }
    };

    reader.onerror = () => {
      setLoadingErrors({
        file: 'Failed to read the file. Please try again.'
      });
    };

    reader.readAsText(file);
  };

  const handleKpiConfigUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const kpiConfig = JSON.parse(e.target?.result as string);
          
          if (!kpiConfig.qualificationFields?.length) {
            setValidationErrors(prev => ({
              ...prev,
              kpiConfig: 'Invalid KPI configuration: Must contain at least one qualification field'
            }));
            return;
          }

          if (!kpiConfig.calculationBase) {
            setValidationErrors(prev => ({
              ...prev,
              kpiConfig: 'Invalid KPI configuration: Missing calculation base'
            }));
            return;
          }

          setLoadedKpiConfig(kpiConfig);
          setConfig(prev => ({
            ...prev,
            revenueBase: kpiConfig.calculationBase,
            kpiConfig
          }));
          setValidationErrors({});
          setSuccessMessage('KPI Configuration loaded successfully');
          setTimeout(() => setSuccessMessage(''), 3000);
        } catch (error) {
          setValidationErrors(prev => ({
            ...prev,
            kpiConfig: 'Failed to parse KPI configuration file'
          }));
        }
      };
      reader.readAsText(file);
    }
  };

  const handleAdditionalKpiConfigUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file && config.kpiConfig) {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const newKpiConfig = JSON.parse(e.target?.result as string);
          
          const mergedConfig: KpiConfig = {
            ...config.kpiConfig,
            baseData: [...(config.kpiConfig.baseData || []), ...(newKpiConfig.baseData || [])],
            qualificationFields: [...(config.kpiConfig.qualificationFields || []), ...(newKpiConfig.qualificationFields || [])],
            adjustmentFields: [...(config.kpiConfig.adjustmentFields || []), ...(newKpiConfig.adjustmentFields || [])],
            exclusionFields: [...(config.kpiConfig.exclusionFields || []), ...(newKpiConfig.exclusionFields || [])],
            creditFields: [...(config.kpiConfig.creditFields || []), ...(newKpiConfig.creditFields || [])]
          };

          setLoadedKpiConfig(mergedConfig);
          setConfig(prev => ({
            ...prev,
            kpiConfig: mergedConfig
          }));

          setSuccessMessage('Additional KPI fields merged successfully');
          setTimeout(() => setSuccessMessage(''), 3000);
        } catch (error) {
          setValidationErrors(prev => ({
            ...prev,
            kpiConfig: 'Failed to parse additional KPI configuration file'
          }));
        }
      };
      reader.readAsText(file);
    }
  };

  const validateConfig = (): boolean => {
    const errors: Record<string, string> = {};

    if (!config.name.trim()) {
      errors.name = 'Scheme Name is required';
    }
    if (!config.effectiveFrom) {
      errors.effectiveFrom = 'Effective From date is required';
    }
    if (!config.effectiveTo) {
      errors.effectiveTo = 'Effective To date is required';
    }
    if (!config.quotaAmount || config.quotaAmount <= 0) {
      errors.quotaAmount = 'Valid Quota Amount is required';
    }

    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleSave = () => {
    if (!validateConfig()) {
      return;
    }

    const timestamp = format(new Date(), 'ddMMyy_HHmm');
    const filename = `Scheme_${user?.clientId}_${timestamp}.json`;
    
    const dataStr = JSON.stringify(config, null, 2);
    const dataBlob = new Blob([dataStr], { type: 'application/json' });
    const url = URL.createObjectURL(dataBlob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    
    setSuccessMessage('Scheme saved successfully');
    setTimeout(() => {
      setSuccessMessage('');
      setMode('initial');
      setConfig(DEFAULT_CONFIG);
      setValidationErrors({});
    }, 3000);
  };

  const handleCancel = () => {
    if (originalConfigRef.current) {
      setConfig(JSON.parse(JSON.stringify(originalConfigRef.current)));
      setMode('view');
    } else {
      setMode('initial');
    }
    setValidationErrors({});
  };

  const addCustomRule = () => {
    const newRule: CustomRule = {
      id: crypto.randomUUID(),
      evaluationLevel: EVALUATION_LEVELS[0],
      metric: METRIC_TYPES[0],
      period: PERIOD_TYPES[0],
      threshold: 0,
      groupBy: ''
    };
    setConfig(prev => ({
      ...prev,
      customRules: [...prev.customRules, newRule]
    }));
    setHasChanges(true);
  };

  const updateCustomRule = (id: string, updates: Partial<CustomRule>) => {
    setConfig(prev => ({
      ...prev,
      customRules: prev.customRules.map(rule => 
        rule.id === id ? { ...rule, ...updates } : rule
      )
    }));
    setHasChanges(true);
  };

  const removeCustomRule = (id: string) => {
    setConfig(prev => ({
      ...prev,
      customRules: prev.customRules.filter(rule => rule.id !== id)
    }));
    setHasChanges(true);
  };

  if (Object.keys(loadingErrors).length > 0) {
    return (
      <div className="max-w-5xl mx-auto py-12 px-4">
        <Card className="bg-red-50 border-red-200 p-8 rounded-xl">
          <div className="flex items-center space-x-3 text-red-800">
            <AlertCircle className="h-6 w-6" />
            <h3 className="text-lg font-medium">Error Loading Scheme</h3>
          </div>
          <ul className="mt-4 text-sm text-red-700 space-y-2">
            {Object.entries(loadingErrors).map(([key, error]) => (
              <li key={key} className="flex items-center space-x-2">
                <span>•</span>
                <span>{error}</span>
              </li>
            ))}
          </ul>
          <Button
            onClick={() => {
              setLoadingErrors({});
              setMode('initial');
            }}
            variant="outline"
            className="mt-6 rounded-full hover:bg-red-100 transition"
          >
            Return to Start
          </Button>
        </Card>
      </div>
    );
  }

  if (mode === 'initial') {
    return (
      <div className="max-w-5xl mx-auto py-12 px-4">
        <Card className="bg-gray-50 rounded-xl p-12 shadow-sm border border-gray-200">
          <div className="text-center space-y-6">
            <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-black">
              <Calculator className="h-8 w-8 text-white" />
            </div>
            <h1 className="text-3xl font-semibold text-slate-800">Scheme Designer</h1>
            <p className="text-slate-600">Create or manage incentive schemes</p>
            
            <div className="max-w-sm mx-auto space-y-4 pt-6">
              <Button
                onClick={() => setMode('new')}
                className="w-full rounded-full bg-black text-white hover:opacity-90 transition py-6"
              >
                <Plus className="h-5 w-5 mr-2" />
                Create New Scheme
              </Button>
              
              <Button 
                variant="outline" 
                className="w-full rounded-full border-2 hover:bg-gray-100 transition py-6"
                onClick={() => fileInputRef.current?.click()}
              >
                <FileUp className="h-5 w-5 mr-2" />
                View Existing Scheme
              </Button>
              <input
                ref={fileInputRef}
                type="file"
                accept=".json"
                onChange={handleFileLoad}
                className="hidden"
              />
            </div>
          </div>
        </Card>
      </div>
    );
  }

  return (
    <div className="max-w-5xl mx-auto py-12 px-4 space-y-8">
      <div className="flex justify-between items-center">
        <h1 className="text-3xl font-semibold text-slate-800">
          {mode === 'new' ? 'Create New Scheme' : 'View Scheme'}
        </h1>
        <div className="flex space-x-3">
          {mode === 'view' && (
            <Button 
              onClick={() => setMode('edit')} 
              variant="outline"
              className="rounded-full hover:bg-gray-100 transition"
            >
              <Edit2 className="h-4 w-4 mr-2" />
              Edit Scheme
            </Button>
          )}
          {mode === 'edit' && (
            <>
              <Button 
                onClick={handleSave}
                className="rounded-full bg-black text-white hover:opacity-90 transition"
              >
                <Save className="h-4 w-4 mr-2" />
                Save Scheme
              </Button>
              <Button 
                onClick={handleCancel} 
                variant="outline"
                className="rounded-full hover:bg-gray-100 transition"
              >
                <X className="h-4 w-4 mr-2" />
                Cancel
              </Button>
            </>
          )}
          {mode === 'new' && hasChanges && (
            <Button 
              onClick={handleSave}
              className="rounded-full bg-black text-white hover:opacity-90 transition"
            >
              <Save className="h-4 w-4 mr-2" />
              Save Scheme
            </Button>
          )}
        </div>
      </div>

      {successMessage && (
        <Card className="bg-green-50 border-green-200 p-4">
          <div className="flex items-center text-green-700">
            <Check className="h-5 w-5 mr-2" />
            {successMessage}
          </div>
        </Card>
      )}

      <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">Scheme Name</label>
            <input
              type="text"
              value={config.name}
              onChange={(e) => {
                setConfig({ ...config, name: e.target.value });
                setHasChanges(true);
              }}
              disabled={mode === 'view'}
              className={`w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50 ${
                validationErrors.name ? 'border-red-300' : ''
              }`}
            />
            {validationErrors.name && (
              <p className="mt-1 text-sm text-red-600">{validationErrors.name}</p>
            )}
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">KPI Configuration</label>
            <div className="mt-1 space-y-2">
              {config.kpiConfig ? (
                <div className="p-4 bg-gray-100 rounded-lg">
                  <p className="text-sm text-slate-600">
                    Using KPI Configuration: {config.kpiConfig.name || 'Unnamed Configuration'}
                  </p>
                  <p className="text-xs text-slate-500 mt-1">
                    {config.kpiConfig.qualificationFields.length} qualification fields,{' '}
                    {config.kpiConfig.adjustmentFields.length} adjustment fields,{' '}
                    {config.kpiConfig.exclusionFields.length} exclusion fields,{' '}
                    {config.kpiConfig.creditFields?.length || 0} credit fields
                  </p>
                  {mode !== 'view' && (
                    <Button
                      onClick={() => additionalKpiConfigInputRef.current?.click()}
                      variant="outline"
                      size="sm"
                      className="mt-2 rounded-full hover:bg-gray-100 transition"
                    >
                      <Upload className="h-4 w-4 mr-2" />
                      Upload Additional KPI Fields
                    </Button>
                  )}
                </div>
              ) : (
                <Button
                  onClick={() => kpiConfigInputRef.current?.click()}
                  disabled={mode === 'view'}
                  variant="outline"
                  className={`w-full rounded-full hover:bg-gray-100 transition ${
                    validationErrors.kpiConfig ? 'border-red-300' : ''
                  }`}
                >
                  <Upload className="h-4 w-4 mr-2" />
                  Upload KPI Configuration
                </Button>
              )}
              <input
                ref={kpiConfigInputRef}
                type="file"
                accept=".json"
                onChange={handleKpiConfigUpload}
                className="hidden"
              />
              <input
                ref={additionalKpiConfigInputRef}
                type="file"
                accept=".json"
                onChange={handleAdditionalKpiConfigUpload}
                className="hidden"
              />
              {validationErrors.kpiConfig && (
                <p className="mt-1 text-sm text-red-600">{validationErrors.kpiConfig}</p>
              )}
            </div>
          </div>

          <div className="sm:col-span-2">
            <label className="block text-sm font-medium text-slate-700 mb-1">Description</label>
            <textarea
              rows={3}
              value={config.description}
              onChange={(e) => {
                setConfig({ ...config, description: e.target.value });
                setHasChanges(true);
              }}
              disabled={mode === 'view'}
              className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">Effective From</label>
            <input
              type="date"
              value={config.effectiveFrom}
              onChange={(e) => {
                setConfig({ ...config, effectiveFrom: e.target.value });
                setHasChanges(true);
              }}
              disabled={mode === 'view'}
              className={`w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50 ${
                validationErrors.effectiveFrom ? 'border-red-300' : ''
              }`}
            />
            {validationErrors.effectiveFrom && (
              <p className="mt-1 text-sm text-red-600">{validationErrors.effectiveFrom}</p>
            )}
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">Effective To</label>
            <input
              type="date"
              value={config.effectiveTo}
              onChange={(e) => {
                setConfig({ ...config, effectiveTo: e.target.value });
                setHasChanges(true);
              }}
              disabled={mode === 'view'}
              className={`w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50 ${
                validationErrors.effectiveTo ? 'border-red-300' : ''
              }`}
            />
            {validationErrors.effectiveTo && (
              <p className="mt-1 text-sm text-red-600">{validationErrors.effectiveTo}</p>
            )}
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">Quota Amount</label>
            <input
              type="number"
              value={config.quotaAmount}
              onChange={(e) => {
                setConfig({ ...config, quotaAmount: parseFloat(e.target.value) });
                setHasChanges(true);
              }}
              disabled={mode === 'view'}
              className={`w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50 ${
                validationErrors.quotaAmount ? 'border-red-300' : ''
              }`}
            />
            {validationErrors.quotaAmount && (
              <p className="mt-1 text-sm text-red-600">{validationErrors.quotaAmount}</p>
            )}
          </div>
        </div>
      </Card>

      {config && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-1">
          <Tabs 
            value={activeSection} 
            onValueChange={setActiveSection}
            className="w-full"
          >
            <TabsList className="w-full justify-start bg-gray-50 p-1 rounded-lg">
              <TabsTrigger value="base" className="rounded-md">Base Data</TabsTrigger>
              <TabsTrigger value="qualification" className="rounded-md">Qualification Criteria</TabsTrigger>
              <TabsTrigger value="adjustment" className="rounded-md">Adjustment Fields</TabsTrigger>
              <TabsTrigger value="exclusion" className="rounded-md">Exclusion Fields</TabsTrigger>
              <TabsTrigger value="payout" className="rounded-md">Payout Tiers</TabsTrigger>
              <TabsTrigger value="credit" className="rounded-md">Credit Rules</TabsTrigger>
              <TabsTrigger value="custom" className="rounded-md">Custom Rules</TabsTrigger>
            </TabsList>

            <div className="p-6">
              <TabsContent value="base">
                <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
                  <h3 className="text-xl font-semibold text-slate-800 mb-6">Base Data Configuration</h3>
                  <div className="grid grid-cols-1 gap-6">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Source File Name</label>
                      <input
                        type="text"
                        value={config.baseMapping.sourceFile}
                        onChange={(e) => {
                          setConfig({
                            ...config,
                            baseMapping: { ...config.baseMapping, sourceFile: e.target.value }
                          });
                          setHasChanges(true);
                        }}
                        disabled={mode === 'view'}
                        placeholder="e.g., Input_Sales.csv"
                        className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Agent Field</label>
                      <input
                        type="text"
                        value={config.baseMapping.agentField}
                        onChange={(e) => {
                          setConfig({
                            ...config,
                            baseMapping: { ...config.baseMapping, agentField: e.target.value }
                          });
                          setHasChanges(true);
                        }}
                        disabled={mode === 'view'}
                        placeholder="e.g., Sales Employee"
                        className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Base Amount Field</label>
                      <input
                        type="text"
                        value={config.baseMapping.amountField}
                        onChange={(e) => {
                          setConfig({
                            ...config,
                            baseMapping: { ...config.baseMapping, amountField: e.target.value }
                          });
                          setHasChanges(true);
                        }}
                        disabled={mode === 'view'}
                        placeholder="e.g., Net Value"
                        className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                      />
                    </div>
                  </div>
                </Card>
              </TabsContent>

              <TabsContent value="qualification">
                <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
                  <h3 className="text-xl font-semibold text-slate-800 mb-6">Qualification Rules</h3>
                  <RuleBuilder
                    rules={config.qualificationRules}
                    onChange={(rules) => {
                      setConfig({ ...config, qualificationRules: rules });
                      setHasChanges(true);
                    }}
                    disabled={mode === 'view'}
                    kpiFields={config.kpiConfig?.qualificationFields || []}
                    sectionName="Qualification Rules"
                  />
                </Card>
              </TabsContent>

              <TabsContent value="adjustment">
                <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
                  <h3 className="text-xl font-semibold text-slate-800 mb-6">Adjustment Rules</h3>
                  <AdjustmentRuleBuilder
                    rules={config.adjustmentRules}
                    onChange={(rules) => {
                      setConfig({ ...config, adjustmentRules: rules });
                      setHasChanges(true);
                    }}
                    disabled={mode === 'view'}
                    kpiFields={config.kpiConfig?.adjustmentFields || []}
                    sectionName="Adjustment Rules"
                  />
                </Card>
              </TabsContent>

              <TabsContent value="exclusion">
                <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
                  <h3 className="text-xl font-semibold text-slate-800 mb-6">Exclusion Rules</h3>
                  <RuleBuilder
                    rules={config.exclusionRules}
                    onChange={(rules) => {
                      setConfig({ ...config, exclusionRules: rules });
                      setHasChanges(true);
                    }}
                    disabled={mode === 'view'}
                    kpiFields={config.kpiConfig?.exclusionFields || []}
                    sectionName="Exclusion Rules"
                  />
                </Card>
              </TabsContent>

              <TabsContent value="payout">
                <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
                  <h3 className="text-xl font-semibold text-slate-800 mb-6">Payout Tiers</h3>
                  <PayoutTierBuilder
                    tiers={config.payoutTiers}
                    onChange={(tiers) => {
                      setConfig({ ...config, payoutTiers: tiers });
                      setHasChanges(true);
                    }}
                    disabled={mode === 'view'}
                  />
                </Card>
              </TabsContent>

              <TabsContent value="credit">
                <CreditSplitTable
                  splits={config.creditSplits}
                  hierarchyFile={config.creditHierarchyFile}
                  onSplitsChange={(splits) => {
                    setConfig({ ...config, creditSplits: splits });
                    setHasChanges(true);
                  }}
                  onHierarchyFileChange={(file) => {
                    setConfig({ ...config, creditHierarchyFile: file });
                    setHasChanges(true);
                  }}
                  disabled={mode === 'view'}
                  uploadedFiles={uploadedFiles}
                />
              </TabsContent>

              <TabsContent value="custom">
                <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
                  <div className="flex justify-between items-center mb-6">
                    <h3 className="text-xl font-semibold text-slate-800">Custom Rules</h3>
                    {mode !== 'view' && (
                      <Button
                        onClick={addCustomRule}
                        variant="outline"
                        className="rounded-full hover:bg-gray-100 transition"
                      >
                        <Plus className="h-4 w-4 mr-2" />
                        Add Custom Rule
                      </Button>
                    )}
                  </div>

                  <div className="space-y-6">
                    {config.customRules.map((rule) => (
                      <Card key={rule.id} className="p-6 border border-gray-200">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                              Evaluation Level
                            </label>
                            <select
                              value={rule.evaluationLevel}
                              onChange={(e) => updateCustomRule(rule.id, { evaluationLevel: e.target.value })}
                              disabled={mode === 'view'}
                              className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                            >
                              {EVALUATION_LEVELS.map(level => (
                                <option key={level} value={level}>{level}</option>
                              ))}
                            </select>
                          </div>

                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                              Metric
                            </label>
                            <select
                              value={rule.metric}
                              onChange={(e) => updateCustomRule(rule.id, { metric: e.target.value })}
                              disabled={mode === 'view'}
                              className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                            >
                              {METRIC_TYPES.map(type => (
                                <option key={type} value={type}>{type}</option>
                              ))}
                            </select>
                          </div>

                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                              Period
                            </label>
                            <select
                              value={rule.period}
                              onChange={(e) => updateCustomRule(rule.id, { period: e.target.value })}
                              disabled={mode === 'view'}
                              className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                            >
                              {PERIOD_TYPES.map(type => (
                                <option key={type} value={type}>{type}</option>
                              ))}
                            </select>
                          </div>

                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                              Threshold
                            </label>
                            <input
                              type="number"
                              value={rule.threshold}
                              onChange={(e) => updateCustomRule(rule.id, { threshold: parseFloat(e.target.value) })}
                              disabled={mode === 'view'}
                              className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                            />
                          </div>

                          <div className="md:col-span-2">
                            <label className="block text-sm font-medium text-gray-700 mb-1">
                              Group By (Optional)
                            </label>
                            <input
                              type="text"
                              value={rule.groupBy || ''}
                              onChange={(e) => updateCustomRule(rule.id, { groupBy: e.target.value })}
                              disabled={mode === 'view'}
                              placeholder="e.g., Region, Product Category"
                              className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                            />
                          </div>

                          {mode !== 'view' && (
                            <div className="md:col-span-2">
                              <Button
                                onClick={() => removeCustomRule(rule.id)}
                                variant="outline"
                                className="w-full rounded-full hover:bg-gray-100 transition"
                              >
                                <X className="h-4 w-4 mr-2" />
                                Remove Rule
                              </Button>
                            </div>
                          )}
                        </div>
                      </Card>
                    ))}

                    {config.customRules.length === 0 && (
                      <div className="text-center text-gray-500 py-8">
                        No custom rules defined
                      </div>
                    )}
                  </div>
                </Card>
              </TabsContent>
            </div>
          </Tabs>
        </div>
      )}
    </div>
  );
}