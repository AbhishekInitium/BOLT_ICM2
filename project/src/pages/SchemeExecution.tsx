import React, { useState, useRef, useCallback } from 'react';
import { Upload, AlertCircle, Check, X, FileUp, Calendar, Play, Beaker } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Card } from '../components/ui/card';
import { Label } from '../components/ui/label';
import { format, isWithinInterval, parseISO, isValid } from 'date-fns';
import { runScheme } from '../../runtime/schemes/runScheme.js';

interface ValidationError {
  type: 'error' | 'warning';
  message: string;
  details: string[];
}

interface SchemeData {
  id?: string;
  name: string;
  description: string;
  effectiveFrom: string;
  effectiveTo: string;
  quotaAmount: number;
  revenueBase: string;
  baseMapping: {
    sourceFile: string;
    agentField: string;
    amountField: string;
  };
  kpiConfig: {
    calculationBase: string;
    baseField: string;
    baseData: Array<{
      id: string;
      name: string;
      description: string;
      sourceType: string;
      sourceField: string;
      dataType: string;
      evaluationLevel: string;
      aggregation: string;
      sourceFile?: string;
    }>;
    qualificationFields: Array<{
      id: string;
      name: string;
      description: string;
      sourceType: string;
      sourceField: string;
      dataType: string;
      evaluationLevel: string;
      aggregation: string;
      sourceFile?: string;
    }>;
    adjustmentFields: Array<any>;
    exclusionFields: Array<any>;
    creditFields: Array<any>;
  };
  creditHierarchyFile?: string;
}

interface UploadedFile {
  name: string;
  data: any[];
  columns: string[];
}

interface RequiredField {
  fileName: string;
  fieldName: string;
  source: string;
  description: string;
}

interface ExecutionResult {
  success: boolean;
  message: string;
  data?: any;
  errors?: string[];
}

export function SchemeExecution() {
  const schemeFileInputRef = useRef<HTMLInputElement>(null);
  const dataFileInputRef = useRef<HTMLInputElement>(null);
  const [selectedScheme, setSelectedScheme] = useState<SchemeData | null>(null);
  const [validationErrors, setValidationErrors] = useState<ValidationError[]>([]);
  const [uploadedFiles, setUploadedFiles] = useState<Record<string, UploadedFile>>({});
  const [requiredFiles, setRequiredFiles] = useState<Set<string>>(new Set());
  const [requiredFields, setRequiredFields] = useState<RequiredField[]>([]);
  const [missingColumns, setMissingColumns] = useState<Record<string, string[]>>({});
  const [runAsOfDate, setRunAsOfDate] = useState<string>('');
  const [isExecuting, setIsExecuting] = useState(false);
  const [executionResult, setExecutionResult] = useState<ExecutionResult | null>(null);

  const resetState = useCallback(() => {
    setSelectedScheme(null);
    setValidationErrors([]);
    setUploadedFiles({});
    setMissingColumns({});
    setRequiredFiles(new Set());
    setRequiredFields([]);
    setRunAsOfDate('');
    setIsExecuting(false);
    setExecutionResult(null);
  }, []);

  const getRequiredFiles = useCallback((scheme: SchemeData): Set<string> => {
    const files = new Set<string>();
    const fields: RequiredField[] = [];
    
    if (scheme.baseMapping?.sourceFile) {
      files.add(scheme.baseMapping.sourceFile);
      fields.push({
        fileName: scheme.baseMapping.sourceFile,
        fieldName: scheme.baseMapping.agentField,
        source: 'Base Mapping',
        description: 'Agent identifier field'
      });
      fields.push({
        fileName: scheme.baseMapping.sourceFile,
        fieldName: scheme.baseMapping.amountField,
        source: 'Base Mapping',
        description: 'Base amount field'
      });
    }
    
    if (scheme.kpiConfig) {
      const addFields = (configFields: any[], source: string) => {
        configFields.forEach(field => {
          if (field.sourceType === 'External' && field.sourceFile) {
            files.add(field.sourceFile);
            fields.push({
              fileName: field.sourceFile,
              fieldName: field.sourceField,
              source,
              description: field.description || field.name
            });
          }
        });
      };

      addFields(scheme.kpiConfig.baseData, 'Base Data');
      addFields(scheme.kpiConfig.qualificationFields, 'Qualification');
      addFields(scheme.kpiConfig.adjustmentFields, 'Adjustment');
      addFields(scheme.kpiConfig.exclusionFields, 'Exclusion');
      if (scheme.kpiConfig.creditFields) {
        addFields(scheme.kpiConfig.creditFields, 'Credit');
      }
    }

    if (scheme.creditHierarchyFile) {
      files.add(scheme.creditHierarchyFile);
      fields.push({
        fileName: scheme.creditHierarchyFile,
        fieldName: 'Reports To',
        source: 'Credit Hierarchy',
        description: 'Reporting hierarchy information'
      });
    }

    setRequiredFields(fields);
    return files;
  }, []);

  const validateSchemeJson = (scheme: any): { valid: boolean; errors: ValidationError[] } => {
    const errors: ValidationError[] = [];

    const requiredFields = [
      'name',
      'description',
      'effectiveFrom',
      'effectiveTo',
      'quotaAmount',
      'revenueBase',
      'baseMapping',
      'kpiConfig'
    ];

    requiredFields.forEach(field => {
      if (!(field in scheme)) {
        errors.push({
          type: 'error',
          message: `Missing required field: ${field}`,
          details: [`The field "${field}" must be present in the scheme configuration`]
        });
      }
    });

    if (scheme.effectiveFrom && scheme.effectiveTo) {
      const fromDate = parseISO(scheme.effectiveFrom);
      const toDate = parseISO(scheme.effectiveTo);
      
      if (!isValid(fromDate) || !isValid(toDate)) {
        errors.push({
          type: 'error',
          message: 'Invalid date format',
          details: ['effectiveFrom and effectiveTo must be valid ISO dates']
        });
      } else if (fromDate >= toDate) {
        errors.push({
          type: 'error',
          message: 'Invalid date range',
          details: ['effectiveFrom must be before effectiveTo']
        });
      }
    }

    if (scheme.baseMapping) {
      const requiredMappingFields = ['sourceFile', 'agentField', 'amountField'];
      requiredMappingFields.forEach(field => {
        if (!(field in scheme.baseMapping)) {
          errors.push({
            type: 'error',
            message: `Missing required field in baseMapping: ${field}`,
            details: [`baseMapping must include "${field}"`]
          });
        }
      });
    }

    if (scheme.kpiConfig) {
      if (!scheme.kpiConfig.calculationBase) {
        errors.push({
          type: 'error',
          message: 'Missing calculationBase in kpiConfig',
          details: ['kpiConfig must include calculationBase']
        });
      }

      if (!Array.isArray(scheme.kpiConfig.baseData)) {
        errors.push({
          type: 'error',
          message: 'Invalid baseData in kpiConfig',
          details: ['baseData must be an array']
        });
      }

      if (!Array.isArray(scheme.kpiConfig.qualificationFields)) {
        errors.push({
          type: 'error',
          message: 'Invalid qualificationFields in kpiConfig',
          details: ['qualificationFields must be an array']
        });
      }
    }

    return {
      valid: errors.filter(e => e.type === 'error').length === 0,
      errors
    };
  };

  const handleSchemeUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const scheme = JSON.parse(e.target?.result as string);
          console.log('Loaded scheme:', scheme);
          
          resetState();
          
          const validation = validateSchemeJson(scheme);
          setValidationErrors(validation.errors);
          
          if (validation.valid) {
            setSelectedScheme(scheme);
            const required = getRequiredFiles(scheme);
            setRequiredFiles(required);
          }
        } catch (err) {
          console.error('Error parsing scheme:', err);
          setValidationErrors([{
            type: 'error',
            message: 'Failed to parse scheme file',
            details: [err instanceof Error ? err.message : 'Invalid format']
          }]);
        }
      };

      reader.onerror = () => {
        setValidationErrors([{
          type: 'error',
          message: 'Failed to read file',
          details: ['Please check the file and try again']
        }]);
      };

      reader.readAsText(file);
    }
  };

  const parseCSV = (content: string): any[] => {
    const lines = content.split('\n');
    if (lines.length < 2) return [];
    
    const headers = lines[0].split(',').map(h => h.trim());
    return lines.slice(1)
      .filter(line => line.trim())
      .map(line => {
        const values = line.split(',').map(v => v.trim());
        return headers.reduce((obj, header, i) => {
          obj[header] = values[i];
          return obj;
        }, {} as Record<string, string>);
      });
  };

  const handleDataUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files || !selectedScheme) return;

    const newFiles = { ...uploadedFiles };
    let hasError = false;

    Array.from(files).forEach(file => {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const content = e.target?.result as string;
          const data = file.name.endsWith('.json') 
            ? JSON.parse(content)
            : parseCSV(content);

          if (!Array.isArray(data) || data.length === 0) {
            throw new Error('Invalid data format');
          }

          const columns = Object.keys(data[0] || {});
          newFiles[file.name] = {
            name: file.name,
            data,
            columns
          };

          setUploadedFiles(newFiles);
          
          if (selectedScheme) {
            const newMissingColumns = validateColumns(selectedScheme, newFiles);
            setMissingColumns(newMissingColumns);
          }
        } catch (err) {
          console.error(`Error parsing file ${file.name}:`, err);
          hasError = true;
          setValidationErrors(prev => [...prev, {
            type: 'error',
            message: `Failed to parse file: ${file.name}`,
            details: ['Please ensure it is valid CSV/JSON']
          }]);
        }
      };
      reader.readAsText(file);
    });
  };

  const validateColumns = useCallback((scheme: SchemeData, files: Record<string, UploadedFile>): Record<string, string[]> => {
    const missingColumns: Record<string, string[]> = {};

    requiredFields.forEach(field => {
      const file = files[field.fileName];
      if (file && !file.columns.includes(field.fieldName)) {
        if (!missingColumns[field.fileName]) {
          missingColumns[field.fileName] = [];
        }
        missingColumns[field.fileName].push(field.fieldName);
      }
    });

    return missingColumns;
  }, [requiredFields]);

  const isReadyToExecute = useCallback(() => {
    if (!selectedScheme || !runAsOfDate) return false;

    const allFilesUploaded = Array.from(requiredFiles).every(file => file in uploadedFiles);
    if (!allFilesUploaded) return false;

    const noMissingColumns = Object.keys(missingColumns).length === 0;
    if (!noMissingColumns) return false;

    try {
      const runDate = parseISO(runAsOfDate);
      if (!isValid(runDate)) return false;

      const isValidDate = isWithinInterval(runDate, {
        start: parseISO(selectedScheme.effectiveFrom),
        end: parseISO(selectedScheme.effectiveTo)
      });
      return isValidDate;
    } catch {
      return false;
    }
  }, [selectedScheme, runAsOfDate, requiredFiles, uploadedFiles, missingColumns]);

  const handleExecute = async (mode: 'simulation' | 'production') => {
    if (!isReadyToExecute() || isExecuting || !selectedScheme) return;
    
    setIsExecuting(true);
    setExecutionResult(null);
    
    try {
      console.log('[Execution] Running scheme with runScheme function');
      const result = await runScheme(selectedScheme, uploadedFiles, runAsOfDate);

      setExecutionResult({
        success: true,
        message: `Scheme executed successfully in ${mode} mode`,
        data: result
      });
    } catch (error) {
      console.error('[Execution] Failed:', error);
      setExecutionResult({
        success: false,
        message: error instanceof Error ? error.message : 'Unknown execution error',
        errors: [error instanceof Error ? error.message : 'Unknown error occurred']
      });
    } finally {
      setIsExecuting(false);
    }
  };

  return (
    <div className="max-w-5xl mx-auto py-12 px-4 space-y-8">
      <div className="flex justify-between items-center">
        <h1 className="text-3xl font-semibold text-slate-800">Scheme Execution</h1>
      </div>

      <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
        <h2 className="text-xl font-semibold text-slate-800 mb-6">Upload Scheme JSON</h2>
        
        {selectedScheme ? (
          <div className="space-y-6">
            <div className="bg-white p-6 rounded-lg border border-gray-200">
              <dl className="grid grid-cols-2 gap-6">
                <div>
                  <dt className="text-sm font-medium text-gray-500">Scheme Name</dt>
                  <dd className="mt-1 text-lg text-gray-900">{selectedScheme.name}</dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-gray-500">Calculation Base</dt>
                  <dd className="mt-1 text-lg text-gray-900">{selectedScheme.kpiConfig.calculationBase}</dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-gray-500">Effective From</dt>
                  <dd className="mt-1 text-lg text-gray-900">
                    {format(new Date(selectedScheme.effectiveFrom), 'MMMM d, yyyy')}
                  </dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-gray-500">Effective To</dt>
                  <dd className="mt-1 text-lg text-gray-900">
                    {format(new Date(selectedScheme.effectiveTo), 'MMMM d, yyyy')}
                  </dd>
                </div>
                <div className="col-span-2">
                  <dt className="text-sm font-medium text-gray-500">Description</dt>
                  <dd className="mt-1 text-lg text-gray-900">{selectedScheme.description}</dd>
                </div>
              </dl>
            </div>

            <Button
              variant="outline"
              onClick={resetState}
              className="w-full rounded-full hover:bg-gray-100 transition"
            >
              <X className="h-4 w-4 mr-2" />
              Clear Selection
            </Button>
          </div>
        ) : (
          <Button
            onClick={() => schemeFileInputRef.current?.click()}
            className="w-full rounded-full hover:bg-gray-100 transition"
          >
            <Upload className="h-4 w-4 mr-2" />
            Upload Scheme JSON
          </Button>
        )}
        <input
          ref={schemeFileInputRef}
          type="file"
          accept=".json"
          onChange={handleSchemeUpload}
          className="hidden"
        />
      </Card>

      {selectedScheme && (
        <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
          <h2 className="text-xl font-semibold text-slate-800 mb-6">Required Input Files</h2>
          
          <div className="space-y-6">
            <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      File Name
                    </th>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Status
                    </th>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Required Fields
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {Array.from(requiredFiles).map((fileName) => {
                    const isUploaded = fileName in uploadedFiles;
                    const missing = missingColumns[fileName] || [];
                    const fields = requiredFields
                      .filter(f => f.fileName === fileName)
                      .map(f => f.fieldName);
                    
                    return (
                      <tr key={fileName}>
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                          {fileName}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          {isUploaded ? (
                            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                              <Check className="h-4 w-4 mr-1" />
                              Uploaded
                            </span>
                          ) : (
                            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                              <X className="h-4 w-4 mr-1" />
                              Missing
                            </span>
                          )}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {missing.length > 0 ? (
                            <span className="text-red-600">
                              Missing: {missing.join(', ')}
                            </span>
                          ) : (
                            <span>{fields.join(', ')}</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <Button
              onClick={() => dataFileInputRef.current?.click()}
              className="w-full rounded-full hover:bg-gray-100 transition"
            >
              <FileUp className="h-4 w-4 mr-2" />
              Upload Data Files
            </Button>
            <input
              ref={dataFileInputRef}
              type="file"
              accept=".csv,.json"
              multiple
              onChange={handleDataUpload}
              className="hidden"
            />
          </div>
        </Card>
      )}

      {selectedScheme && (
        <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
          <h2 className="text-xl font-semibold text-slate-800 mb-6">Run Controls</h2>
          
          <div className="space-y-6">
            <div>
              <Label htmlFor="runDate" className="text-sm font-medium text-gray-700">Run As Of Date</Label>
              <div className="mt-1 relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <Calendar className="h-5 w-5 text-gray-400" />
                </div>
                <input
                  type="date"
                  id="runDate"
                  value={runAsOfDate}
                  onChange={(e) => setRunAsOfDate(e.target.value)}
                  min={selectedScheme.effectiveFrom}
                  max={selectedScheme.effectiveTo}
                  className="block w-full pl-10 pr-3 py-2 text-base border border-gray-300 rounded-md focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <Button
                onClick={() => handleExecute('simulation')}
                disabled={!isReadyToExecute() || isExecuting}
                variant="outline"
                className="rounded-full hover:bg-gray-100 transition"
              >
                <Beaker className="h-4 w-4 mr-2" />
                {isExecuting ? 'Simulating...' : 'Simulate Scheme'}
              </Button>

              <Button
                onClick={() => handleExecute('production')}
                disabled={!isReadyToExecute() || isExecuting}
                className="rounded-full bg-black text-white hover:opacity-90 transition"
              >
                <Play className="h-4 w-4 mr-2" />
                {isExecuting ? 'Running...' : 'Run Production'}
              </Button>
            </div>
          </div>
        </Card>
      )}

      {executionResult && (
        <Card className="bg-gray-50 rounded-xl p-6 shadow-sm border border-gray-200">
          <h2 className="text-xl font-semibold text-slate-800 mb-6">Execution Results</h2>
          
          <div className="bg-white p-6 rounded-lg border border-gray-200">
            <dl className="grid grid-cols-2 gap-6">
              <div>
                <dt className="text-sm font-medium text-gray-500">Status</dt>
                <dd className="mt-1 text-lg text-gray-900">
                  {executionResult.success ? (
                    <span className="text-green-600">Success</span>
                  ) : (
                    <span className="text-red-600">Failed</span>
                  )}
                </dd>
              </div>
              <div>
                <dt className="text-sm font-medium text-gray-500">Message</dt>
                <dd className="mt-1 text-lg text-gray-900">{executionResult.message}</dd>
              </div>
              {executionResult.data && (
                <>
                  <div>
                    <dt className="text-sm font-medium text-gray-500">Total Records</dt>
                    <dd className="mt-1 text-lg text-gray-900">{executionResult.data.totalRecords}</dd>
                  </div>
                  <div>
                    <dt className="text-sm font-medium text-gray-500">Processed At</dt>
                    <dd className="mt-1 text-lg text-gray-900">
                      {format(new Date(executionResult.data.processedAt), 'PPpp')}
                    </dd>
                  </div>
                </>
              )}
            </dl>
          </div>
        </Card>
      )}

      {validationErrors.length > 0 && (
        <Card className={`p-4 ${
          validationErrors.some(e => e.type === 'error')
            ? 'bg-red-50 border-red-200'
            : 'bg-yellow-50 border-yellow-200'
        }`}>
          <div className="space-y-4">
            {validationErrors.map((error, index) => (
              <div key={index} className="flex items-start space-x-2">
                {error.type === 'error' ? (
                  <AlertCircle className="h-5 w-5 text-red-500 mt-0.5" />
                
                ) : (
                  <AlertCircle className="h-5 w-5 text-yellow-500 mt-0.5" />
                )}
                <div className={error.type === 'error' ? 'text-red-700' : 'text-yellow-700'}>
                  <p className="font-medium">{error.message}</p>
                  <ul className="mt-1 text-sm space-y-1">
                    {error.details.map((detail, i) => (
                      <li key={i}>• {detail}</li>
                    ))}
                  </ul>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {selectedScheme && validationErrors.every(e => e.type === 'warning') && (
        <Card className="bg-green-50 border-green-200 p-4">
          <div className="flex items-center text-green-700">
            <Check className="h-5 w-5 mr-2" />
            Scheme validation successful
          </div>
        </Card>
      )}
    </div>
  );
}