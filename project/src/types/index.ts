// Core type definitions for the ICM platform
export interface Compensation {
  id: string;
  period: string;
  amount: number;
  currency: string;
  status: 'draft' | 'pending' | 'approved' | 'paid';
}

export interface KPI {
  id: string;
  name: string;
  source: 'sap' | 'excel';
  calculation: string;
  target: number;
  actual: number;
}

export interface CompensationScheme {
  id: string;
  name: string;
  description: string;
  rules: Rule[];
  effectiveFrom: string;
  effectiveTo: string;
}

export interface Rule {
  id: string;
  field: string;
  operator: string;
  value: string;
}

export interface AdjustmentCondition {
  field: string;
  operator: string;
  value: string;
}

export interface AdjustmentAction {
  target: 'Payout' | 'Quota' | 'Rate';
  type: 'percentage' | 'fixed';
  value: number;
}

export interface AdjustmentRule {
  id: string;
  condition: AdjustmentCondition;
  adjustment: AdjustmentAction;
}

export interface SchemeConfig {
  name: string;
  description: string;
  effectiveFrom: string;
  effectiveTo: string;
  quotaAmount: number;
  revenueBase: string;
  baseMapping: BaseMapping;
  qualificationRules: Rule[];
  adjustmentRules: AdjustmentRule[];
  exclusionRules: Rule[];
  creditRules: Rule[];
  creditSplits: CreditSplit[];
  creditHierarchyFile: string;
  payoutTiers: PayoutTier[];
  customRules: CustomRule[];
  kpiConfig?: KpiConfig;
}

export interface BaseMapping {
  sourceFile: string;
  agentField: string;
  amountField: string;
}

export interface CreditSplit {
  id: string;
  role: string;
  percentage: number;
}

export interface PayoutTier {
  id: string;
  from: number;
  to: number;
  rate: number;
  isPercentage: boolean;
}

export interface CustomRule {
  id: string;
  evaluationLevel: string;
  metric: string;
  period: string;
  threshold: number;
  groupBy?: string;
}

export interface KpiConfig {
  name?: string;
  calculationBase: string;
  baseData: KpiField[];
  qualificationFields: KpiField[];
  adjustmentFields: KpiField[];
  exclusionFields: KpiField[];
  creditFields: KpiField[];
}

export interface KpiField {
  name: string;
  sourceField: string;
  dataType: string;
}