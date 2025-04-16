import React, { useState, useRef, useEffect } from 'react';
import { Plus, Save, Edit2, X, FileUp, Calculator, Upload, AlertCircle, Check } from 'lucide-react';
import { Button } from './ui/button';
import { Card } from './ui/card';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './ui/tabs';
import { useAuthStore } from '../store/authStore';
import { format } from 'date-fns';

interface KpiEntry {
  id: string;
  name: string;
  description: string;
  sourceType: 'System' | 'External';
  sourceField: string;
  sourceFile?: string;
  dataType: 'Number' | 'String' | 'Date';
  evaluationLevel: 'Agent' | 'Team' | 'Region' | 'Per Record';
  aggregation: 'Sum' | 'Avg' | 'Min' | 'Max' | 'NotApplicable';
  isNew?: boolean;
  isEditing?: boolean;
  originalValues?: Partial<KpiEntry>;
  validationError?: string;
}

interface KpiConfig {
  calculationBase: string;
  baseField: string;
  baseData: KpiEntry[];
  qualificationFields: KpiEntry[];
  adjustmentFields: KpiEntry[];
  exclusionFields: KpiEntry[];
  creditFields: KpiEntry[];
}

type Mode = 'initial' | 'new' | 'view' | 'edit';

const CALCULATION_BASES = [
  'Sales Orders',
  'Invoices',
  'Collections',
  'Policies',
  'Loans'
];

const EVALUATION_LEVELS = ['Agent', 'Team', 'Region', 'Per Record'];

export function KpiConfigurator() {
  const user = useAuthStore((state) => state.user);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const kpiConfigInputRef = useRef<HTMLInputElement>(null);
  const additionalKpiConfigInputRef = useRef<HTMLInputElement>(null);
  const originalConfigRef = useRef<KpiConfig | null>(null);
  const [mode, setMode] = useState<Mode>('initial');
  const [loadedKpiConfig, setLoadedKpiConfig] = useState<KpiConfig | null>(null);
  const [activeSection, setActiveSection] = useState<string>('base');
  const [hasChanges, setHasChanges] = useState(false);
  const [globalError, setGlobalError] = useState<string>('');
  const [config, setConfig] = useState<KpiConfig>({
    calculationBase: CALCULATION_BASES[0],
    baseField: '',
    baseData: [],
    qualificationFields: [],
    adjustmentFields: [],
    exclusionFields: [],
    creditFields: []
  });
  const [validationErrors, setValidationErrors] = useState<Record<string, string>>({});
  const [loadingErrors, setLoadingErrors] = useState<Record<string, string>>({});
  const [successMessage, setSuccessMessage] = useState<string>('');
  const [activeTab, setActiveTab] = useState('rules');

  useEffect(() => {
    if (loadedKpiConfig) {
      console.log('KPI Config Updated:', loadedKpiConfig);
    }
  }, [loadedKpiConfig]);

  useEffect(() => {
    console.log('Scheme Config Updated:', config);
  }, [config]);

  const createKpiEntry = (): KpiEntry => ({
    id: crypto.randomUUID(),
    name: '',
    description: '',
    sourceType: 'System',
    sourceField: '',
    dataType: 'Number',
    evaluationLevel: 'Agent',
    aggregation: 'Sum',
    isNew: true,
    isEditing: true
  });

  const handleFileLoad = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const loadedConfig = JSON.parse(e.target?.result as string);
          originalConfigRef.current = JSON.parse(JSON.stringify(loadedConfig));
          setConfig({
            ...loadedConfig,
            creditFields: loadedConfig.creditFields || []
          });
          setMode('view');
          setGlobalError('');
        } catch (error) {
          setGlobalError('Failed to parse KPI configuration file');
        }
      };
      reader.readAsText(file);
    }
  };

  const handleCancel = () => {
    if (originalConfigRef.current) {
      setConfig(JSON.parse(JSON.stringify(originalConfigRef.current)));
      setMode('view');
      setHasChanges(false);
      setValidationErrors({});
    }
  };

  const validateConfig = (): boolean => {
    const errors: Record<string, string> = {};

    if (!config.calculationBase) {
      errors.calculationBase = 'Calculation Base is required';
    }
    if (config.qualificationFields.length === 0) {
      errors.qualificationFields = 'At least one Qualification Criteria KPI must be added';
    }

    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const validateKpi = (kpi: KpiEntry): boolean => {
    let error = '';
    if (!kpi.name.trim()) {
      error = 'KPI Name is required';
    } else if (!kpi.sourceField.trim()) {
      error = 'Source Field is required';
    } else if (kpi.sourceType === 'External') {
      if (!kpi.sourceFile?.trim()) {
        error = 'Source File is required for external KPIs';
      }
    }

    if (error) {
      setConfig(prev => ({
        ...prev,
        [activeSection]: prev[activeSection as keyof KpiConfig].map(k => 
          k.id === kpi.id ? { ...k, validationError: error } : k
        )
      }));
      return false;
    }
    return true;
  };

  const handleSave = () => {
    if (!validateConfig()) {
      return;
    }

    const timestamp = format(new Date(), 'ddMMyy_HHmm');
    const filename = `KPIconfig_${user?.clientId}_${timestamp}.json`;
    
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
    
    setSuccessMessage('Configuration saved successfully');
    setTimeout(() => {
      setSuccessMessage('');
      setMode('initial');
      setConfig({
        calculationBase: CALCULATION_BASES[0],
        baseField: '',
        baseData: [],
        qualificationFields: [],
        adjustmentFields: [],
        exclusionFields: [],
        creditFields: []
      });
      originalConfigRef.current = null;
      setHasChanges(false);
      setGlobalError('');
    }, 3000);
  };

  const addKpiToSection = (section: keyof Omit<KpiConfig, 'calculationBase' | 'baseField'>) => {
    if (mode === 'view') return;
    const newKpi = createKpiEntry();
    setConfig(prev => ({
      ...prev,
      [section]: [...prev[section], newKpi]
    }));
    setHasChanges(true);
  };

  const updateKpi = (
    section: keyof Omit<KpiConfig, 'calculationBase' | 'baseField'>,
    id: string,
    updates: Partial<KpiEntry>
  ) => {
    if (mode === 'view') return;
    setConfig(prev => ({
      ...prev,
      [section]: prev[section].map(kpi => 
        kpi.id === id ? { ...kpi, ...updates, validationError: undefined } : kpi
      )
    }));
    setHasChanges(true);
  };

  const startEditingKpi = (
    section: keyof Omit<KpiConfig, 'calculationBase' | 'baseField'>,
    id: string
  ) => {
    setConfig(prev => ({
      ...prev,
      [section]: prev[section].map(kpi => 
        kpi.id === id 
          ? { 
              ...kpi, 
              isEditing: true,
              validationError: undefined,
              originalValues: {
                name: kpi.name,
                description: kpi.description,
                sourceType: kpi.sourceType,
                sourceField: kpi.sourceField,
                sourceFile: kpi.sourceFile,
                dataType: kpi.dataType,
                evaluationLevel: kpi.evaluationLevel,
                aggregation: kpi.aggregation
              }
            }
          : kpi
      )
    }));
  };

  const cancelEditingKpi = (
    section: keyof Omit<KpiConfig, 'calculationBase' | 'baseField'>,
    id: string
  ) => {
    setConfig(prev => ({
      ...prev,
      [section]: prev[section].map(kpi => {
        if (kpi.id !== id) return kpi;
        
        if (kpi.isNew) {
          return null;
        }
        
        return {
          ...kpi,
          ...kpi.originalValues,
          isEditing: false,
          validationError: undefined,
          originalValues: undefined
        };
      }).filter(Boolean) as KpiEntry[]
    }));
  };

  const saveKpi = (
    section: keyof Omit<KpiConfig, 'calculationBase' | 'baseField'>,
    id: string
  ) => {
    const kpi = config[section].find(k => k.id === id);
    if (!kpi || !validateKpi(kpi)) return;

    setConfig(prev => ({
      ...prev,
      [section]: prev[section].map(k => 
        k.id === id 
          ? { 
              ...k, 
              isNew: false, 
              isEditing: false,
              validationError: undefined,
              originalValues: undefined
            }
          : k
      )
    }));
    setHasChanges(true);
  };

  const deleteKpi = (
    section: keyof Omit<KpiConfig, 'calculationBase' | 'baseField'>,
    id: string
  ) => {
    if (mode === 'view') return;
    setConfig(prev => ({
      ...prev,
      [section]: prev[section].filter(kpi => kpi.id !== id)
    }));
    setHasChanges(true);
  };

  const renderKpiSection = (
    section: keyof Omit<KpiConfig, 'calculationBase' | 'baseField'>,
    title: string
  ) => {
    if (config[section].length === 0 && mode === 'view') {
      return (
        <div className="bg-gray-50 rounded-xl p-8 text-center text-slate-600">
          <p>No KPI fields available in this section for the selected configuration.</p>
        </div>
      );
    }

    return (
      <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
        <div className="flex justify-between items-center mb-6">
          <h3 className="text-xl font-semibold text-slate-800">{title}</h3>
          <Button 
            onClick={() => addKpiToSection(section)} 
            variant="outline"
            disabled={mode === 'view'}
            className="rounded-full hover:bg-gray-100 transition"
          >
            <Plus className="h-4 w-4 mr-2" />
            Add KPI
          </Button>
        </div>
        
        <div className="space-y-6">
          {config[section].map((kpi) => (
            <Card key={kpi.id} className="p-6 border border-gray-200 rounded-xl">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">KPI Name</label>
                    <input
                      type="text"
                      value={kpi.name}
                      onChange={(e) => updateKpi(section, kpi.id, { name: e.target.value })}
                      disabled={!kpi.isEditing}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Description</label>
                    <textarea
                      value={kpi.description}
                      onChange={(e) => updateKpi(section, kpi.id, { description: e.target.value })}
                      disabled={!kpi.isEditing}
                      className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                      rows={2}
                    />
                  </div>
                </div>

                <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Source Type</label>
                      <select
                        value={kpi.sourceType}
                        onChange={(e) => updateKpi(section, kpi.id, { sourceType: e.target.value as 'System' | 'External' })}
                        disabled={!kpi.isEditing}
                        className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                      >
                        <option value="System">System</option>
                        <option value="External">External</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Data Type</label>
                      <select
                        value={kpi.dataType}
                        onChange={(e) => updateKpi(section, kpi.id, { dataType: e.target.value as 'Number' | 'String' | 'Date' })}
                        disabled={!kpi.isEditing}
                        className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                      >
                        <option value="Number">Number</option>
                        <option value="String">String</option>
                        <option value="Date">Date</option>
                      </select>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    {kpi.sourceType === 'External' && (
                      <>
                        <div>
                          <label className="block text-sm font-medium text-slate-700 mb-1">Source File</label>
                          <input
                            type="text"
                            value={kpi.sourceFile || ''}
                            onChange={(e) => updateKpi(section, kpi.id, { sourceFile: e.target.value })}
                            disabled={!kpi.isEditing}
                            className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                            placeholder="e.g., Input_Sales.csv"
                          />
                        </div>
                        <div>
                          <label className="block text-sm font-medium text-slate-700 mb-1">Source Field</label>
                          <input
                            type="text"
                            value={kpi.sourceField}
                            onChange={(e) => updateKpi(section, kpi.id, { sourceField: e.target.value })}
                            disabled={!kpi.isEditing}
                            className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                            placeholder="e.g., Net Value"
                          />
                        </div>
                      </>
                    )}
                    {kpi.sourceType === 'System' && (
                      <div className="col-span-2">
                        <label className="block text-sm font-medium text-slate-700 mb-1">Source Field</label>
                        <input
                          type="text"
                          value={kpi.sourceField}
                          onChange={(e) => updateKpi(section, kpi.id, { sourceField: e.target.value })}
                          disabled={!kpi.isEditing}
                          className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                        />
                      </div>
                    )}
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Evaluation Level</label>
                      <select
                        value={kpi.evaluationLevel}
                        onChange={(e) => updateKpi(section, kpi.id, { evaluationLevel: e.target.value as 'Agent' | 'Team' | 'Region' | 'Per Record' })}
                        disabled={!kpi.isEditing}
                        className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                      >
                        {EVALUATION_LEVELS.map(level => (
                          <option key={level} value={level}>{level}</option>
                        ))}
                      </select>
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1">Aggregation</label>
                      <select
                        value={kpi.aggregation}
                        onChange={(e) => updateKpi(section, kpi.id, { aggregation: e.target.value as 'Sum' | 'Avg' | 'Min' | 'Max' | 'NotApplicable' })}
                        disabled={!kpi.isEditing}
                        className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
                      >
                        <option value="Sum">Sum</option>
                        <option value="Avg">Average</option>
                        <option value="Min">Minimum</option>
                        <option value="Max">Maximum</option>
                        <option value="NotApplicable">Not Applicable</option>
                      </select>
                    </div>
                  </div>

                  {kpi.validationError && (
                    <div className="text-red-500 text-sm flex items-center">
                      <AlertCircle className="h-4 w-4 mr-1" />
                      {kpi.validationError}
                    </div>
                  )}

                  <div className="flex space-x-2 pt-4">
                    {kpi.isEditing ? (
                      <>
                        <Button
                          onClick={() => saveKpi(section, kpi.id)}
                          variant="outline"
                          size="sm"
                          className="rounded-full hover:bg-gray-100 transition"
                        >
                          <Check className="h-4 w-4 mr-2" />
                          Save KPI
                        </Button>
                        <Button
                          onClick={() => cancelEditingKpi(section, kpi.id)}
                          variant="outline"
                          size="sm"
                          className="rounded-full hover:bg-gray-100 transition"
                        >
                          <X className="h-4 w-4 mr-2" />
                          Cancel
                        </Button>
                      </>
                    ) : (
                      <>
                        <Button
                          onClick={() => startEditingKpi(section, kpi.id)}
                          variant="outline"
                          size="sm"
                          disabled={mode === 'view'}
                          className="rounded-full hover:bg-gray-100 transition"
                        >
                          <Edit2 className="h-4 w-4 mr-2" />
                          Edit
                        </Button>
                        <Button
                          onClick={() => deleteKpi(section, kpi.id)}
                          variant="outline"
                          size="sm"
                          disabled={mode === 'view'}
                          className="rounded-full hover:bg-gray-100 transition"
                        >
                          <X className="h-4 w-4 mr-2" />
                          Delete
                        </Button>
                      </>
                    )}
                  </div>
                </div>
              </div>
            </Card>
          ))}
        </div>
      </Card>
    );
  };

  if (mode === 'initial') {
    return (
      <div className="max-w-5xl mx-auto py-12 px-4">
        <Card className="bg-gray-50 rounded-xl p-12 shadow-sm border border-gray-200">
          <div className="text-center space-y-6">
            <h1 className="text-3xl font-semibold text-slate-800">KPI Configuration</h1>
            <p className="text-slate-600">Create or manage your KPI configurations</p>
            
            <div className="max-w-sm mx-auto space-y-4 pt-6">
              <Button
                onClick={() => {
                  setMode('new');
                  setConfig({
                    calculationBase: CALCULATION_BASES[0],
                    baseField: '',
                    baseData: [],
                    qualificationFields: [],
                    adjustmentFields: [],
                    exclusionFields: [],
                    creditFields: []
                  });
                  setGlobalError('');
                }}
                className="w-full rounded-full bg-black text-white hover:opacity-90 transition py-6"
              >
                <Plus className="h-5 w-5 mr-2" />
                Create New Configuration
              </Button>
              
              <Button 
                variant="outline" 
                className="w-full rounded-full border-2 hover:bg-gray-100 transition py-6"
                onClick={() => fileInputRef.current?.click()}
              >
                <FileUp className="h-5 w-5 mr-2" />
                View Existing Configuration
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
        <h1 className="text-3xl font-semibold text-slate-800">KPI Configuration</h1>
        <div className="flex space-x-3">
          {mode === 'view' && (
            <Button 
              onClick={() => setMode('edit')} 
              variant="outline"
              className="rounded-full hover:bg-gray-100 transition"
            >
              <Edit2 className="h-4 w-4 mr-2" />
              Edit Configuration
            </Button>
          )}
          {mode === 'edit' && (
            <>
              <Button 
                onClick={handleSave}
                className="rounded-full bg-black text-white hover:opacity-90 transition"
              >
                <Save className="h-4 w-4 mr-2" />
                Save Configuration
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
              Save Configuration
            </Button>
          )}
        </div>
      </div>

      {globalError && (
        <Card className="bg-red-50 border-red-200 p-4">
          <div className="flex items-center text-red-700">
            <AlertCircle className="h-5 w-5 mr-2" />
            {globalError}
          </div>
        </Card>
      )}

      {successMessage && (
        <Card className="bg-green-50 border-green-200 p-4">
          <div className="flex items-center text-green-700">
            <Check className="h-5 w-5 mr-2" />
            {successMessage}
          </div>
        </Card>
      )}

      <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">Calculation Base</label>
            <select
              value={config.calculationBase}
              onChange={(e) => {
                setConfig({ ...config, calculationBase: e.target.value });
                setHasChanges(true);
              }}
              disabled={mode === 'view'}
              className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
            >
              {CALCULATION_BASES.map(base => (
                <option key={base} value={base}>{base}</option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">Base Field</label>
            <input
              type="text"
              value={config.baseField}
              onChange={(e) => {
                setConfig({ ...config, baseField: e.target.value });
                setHasChanges(true);
              }}
              disabled={mode === 'view'}
              className="w-full rounded-md border border-gray-300 shadow-inner px-3 py-2 focus:ring focus:outline-none disabled:bg-gray-50"
            />
          </div>
        </div>
      </Card>

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
            <TabsTrigger value="credit" className="rounded-md">Credit Rules</TabsTrigger>
          </TabsList>

          <div className="p-6">
            <TabsContent value="base">
              {renderKpiSection('baseData', 'Base Data KPIs')}
            </TabsContent>

            <TabsContent value="qualification">
              {renderKpiSection('qualificationFields', 'Qualification Criteria KPIs')}
            </TabsContent>

            <TabsContent value="adjustment">
              {renderKpiSection('adjustmentFields', 'Adjustment Fields KPIs')}
            </TabsContent>

            <TabsContent value="exclusion">
              {renderKpiSection('exclusionFields', 'Exclusion Fields KPIs')}
            </TabsContent>

            <TabsContent value="credit">
              {renderKpiSection('creditFields', 'Credit Rules KPIs')}
            </TabsContent>
          </div>
        </Tabs>
      </div>
    </div>
  );
}