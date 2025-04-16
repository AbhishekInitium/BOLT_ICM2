import { format } from 'date-fns';
import type { SchemeConfig, Rule, AdjustmentRule, CustomRule } from '../types';

interface ExecutionContext {
  scheme: SchemeConfig;
  files: Record<string, { data: any[]; columns: string[] }>;
  runAsOfDate: string;
  mode: 'simulation' | 'production';
}

interface ExecutionResult {
  success: boolean;
  message: string;
  data: {
    totalRecords: number;
    processedAt: string;
    summary: {
      totalAgents: number;
      qualified: number;
      totalPayout: number;
    };
    agents: AgentResult[];
  };
}

interface AgentResult {
  agentId: string;
  qualified: boolean;
  commission: number;
  baseData: Record<string, any>;
  qualifyingCriteria: Array<{
    rule: string;
    result: boolean;
    details?: string;
  }>;
  adjustments: Array<{
    rule: string;
    applied: boolean;
    value: number;
    details?: string;
  }>;
  exclusions: Array<{
    rule: string;
    applied: boolean;
    details?: string;
  }>;
  creditSplits: Array<{
    repId: string;
    role: string;
    amount: number;
    effectiveFrom: string;
    effectiveTo: string;
  }>;
}

export class SchemeExecutionEngine {
  private context: ExecutionContext;
  private baseData: Record<string, any[]> = {};
  private agentResults: Record<string, AgentResult> = {};

  constructor(context: ExecutionContext) {
    this.context = context;
  }

  public async execute(): Promise<ExecutionResult> {
    try {
      // 1. Preprocess data
      await this.preprocessData();

      // 2. Process each agent
      const agents = this.getUniqueAgents();
      for (const agentId of agents) {
        await this.processAgent(agentId);
      }

      // 3. Apply credit splits
      if (this.context.scheme.creditSplits.length > 0) {
        await this.applyCreditSplits();
      }

      // 4. Generate final result
      return this.generateResult();
    } catch (error) {
      console.error('Execution error:', error);
      throw error;
    }
  }

  private async preprocessData(): Promise<void> {
    const baseFile = this.context.files[this.context.scheme.baseMapping.sourceFile];
    if (!baseFile) throw new Error('Base file not found');

    // Create field mappings from KPI config
    const fieldMappings = new Map<string, string>();
    
    if (this.context.scheme.kpiConfig) {
      const addMappings = (fields: Array<{ name: string; sourceField: string }>) => {
        fields.forEach(field => {
          fieldMappings.set(field.name, field.sourceField);
        });
      };

      addMappings(this.context.scheme.kpiConfig.baseData);
      addMappings(this.context.scheme.kpiConfig.qualificationFields);
      addMappings(this.context.scheme.kpiConfig.adjustmentFields);
      addMappings(this.context.scheme.kpiConfig.exclusionFields);
      if (this.context.scheme.kpiConfig.creditFields) {
        addMappings(this.context.scheme.kpiConfig.creditFields);
      }
    }

    // Group and transform data
    this.baseData = baseFile.data.reduce((acc, row) => {
      const agentId = row[this.context.scheme.baseMapping.agentField];
      if (!acc[agentId]) acc[agentId] = [];
      
      // Transform row to use KPI field names
      const transformedRow = { ...row };
      fieldMappings.forEach((sourceField, kpiField) => {
        transformedRow[kpiField] = row[sourceField];
      });
      
      acc[agentId].push(transformedRow);
      return acc;
    }, {} as Record<string, any[]>);
  }

  private getUniqueAgents(): string[] {
    return Object.keys(this.baseData);
  }

  private async processAgent(agentId: string): Promise<void> {
    const agentData = this.baseData[agentId];
    if (!agentData) return;

    const result: AgentResult = {
      agentId,
      qualified: true,
      commission: 0,
      baseData: {},
      qualifyingCriteria: [],
      adjustments: [],
      exclusions: [],
      creditSplits: []
    };

    // 1. Check qualification rules
    result.qualified = await this.evaluateQualificationRules(agentId, agentData, result);
    if (!result.qualified) {
      this.agentResults[agentId] = result;
      return;
    }

    // 2. Apply exclusion rules
    const validRecords = await this.applyExclusionRules(agentData, result);

    // 3. Calculate base commission
    let commission = await this.calculateBaseCommission(validRecords);

    // 4. Apply adjustment rules
    commission = await this.applyAdjustmentRules(commission, validRecords, result);

    // 5. Apply custom rules
    commission = await this.applyCustomRules(commission, validRecords, result);

    result.commission = commission;
    this.agentResults[agentId] = result;
  }

  private async evaluateQualificationRules(
    agentId: string,
    data: any[],
    result: AgentResult
  ): Promise<boolean> {
    for (const rule of this.context.scheme.qualificationRules) {
      const ruleResult = await this.evaluateRule(rule, data);
      
      result.qualifyingCriteria.push({
        rule: rule.field,
        result: ruleResult,
        details: `Rule ${rule.field} ${rule.operator} ${rule.value}`
      });
      
      if (!ruleResult) return false;
    }
    return true;
  }

  private async applyExclusionRules(
    data: any[],
    result: AgentResult
  ): Promise<any[]> {
    return data.filter(record => {
      for (const rule of this.context.scheme.exclusionRules) {
        if (this.evaluateRuleOnRecord(rule, record)) {
          result.exclusions.push({
            rule: rule.field,
            applied: true,
            details: `Excluded: ${rule.field} ${rule.operator} ${rule.value}`
          });
          return false;
        }
      }
      return true;
    });
  }

  private async calculateBaseCommission(records: any[]): Promise<number> {
    return records.reduce((sum, record) => {
      const amount = parseFloat(record[this.context.scheme.baseMapping.amountField]) || 0;
      return sum + amount;
    }, 0);
  }

  private async applyAdjustmentRules(
    baseCommission: number,
    records: any[],
    result: AgentResult
  ): Promise<number> {
    let finalCommission = baseCommission;

    for (const rule of this.context.scheme.adjustmentRules) {
      const matchingRecords = records.filter(record => 
        this.evaluateRuleOnRecord(rule.condition, record)
      );

      if (matchingRecords.length > 0) {
        const adjustment = this.calculateAdjustment(rule, finalCommission);
        finalCommission += adjustment;

        result.adjustments.push({
          rule: rule.condition.field,
          applied: true,
          value: adjustment,
          details: `Applied ${rule.adjustment.type}: ${rule.adjustment.value}`
        });
      }
    }

    return finalCommission;
  }

  private async applyCustomRules(
    commission: number,
    records: any[],
    result: AgentResult
  ): Promise<number> {
    let finalCommission = commission;

    for (const rule of this.context.scheme.customRules) {
      const adjustment = await this.evaluateCustomRule(rule, records);
      finalCommission += adjustment;
    }

    return finalCommission;
  }

  private async applyCreditSplits(): Promise<void> {
    const hierarchyFile = this.context.files[this.context.scheme.creditHierarchyFile];
    if (!hierarchyFile) return;

    for (const agentId in this.agentResults) {
      const result = this.agentResults[agentId];
      if (!result.qualified) continue;

      const splits = await this.calculateCreditSplits(
        agentId,
        result.commission,
        hierarchyFile.data
      );

      result.creditSplits = splits;
    }
  }

  private async calculateCreditSplits(
    agentId: string,
    amount: number,
    hierarchyData: any[]
  ): Promise<Array<{ repId: string; role: string; amount: number; effectiveFrom: string; effectiveTo: string }>> {
    const splits = [];
    let remainingAmount = amount;

    for (const split of this.context.scheme.creditSplits) {
      const splitAmount = (split.percentage / 100) * amount;
      remainingAmount -= splitAmount;

      // Find the reporting hierarchy
      const hierarchy = hierarchyData.find(h => 
        h['Sales Employee'] === agentId &&
        new Date(h['Valid From']) <= new Date(this.context.runAsOfDate) &&
        new Date(h['Valid To']) >= new Date(this.context.runAsOfDate)
      );

      splits.push({
        repId: hierarchy ? hierarchy['Reports To'] : agentId,
        role: split.role,
        amount: splitAmount,
        effectiveFrom: hierarchy ? hierarchy['Valid From'] : this.context.scheme.effectiveFrom,
        effectiveTo: hierarchy ? hierarchy['Valid To'] : this.context.scheme.effectiveTo
      });
    }

    return splits;
  }

  private evaluateRule(rule: Rule, data: any[]): boolean {
    return data.some(record => this.evaluateRuleOnRecord(rule, record));
  }

  private evaluateRuleOnRecord(rule: Rule | AdjustmentRule['condition'], record: any): boolean {
    const value = record[rule.field];
    return this.evaluateValue(value, rule.operator, rule.value);
  }

  private evaluateValue(value: any, operator: string, ruleValue: any): boolean {
    switch (operator) {
      case '=':
        return value === ruleValue;
      case '!=':
        return value !== ruleValue;
      case '>':
        return value > ruleValue;
      case '<':
        return value < ruleValue;
      case '>=':
        return value >= ruleValue;
      case '<=':
        return value <= ruleValue;
      case 'CONTAINS':
        return String(value).includes(String(ruleValue));
      case 'NOT CONTAINS':
        return !String(value).includes(String(ruleValue));
      default:
        return false;
    }
  }

  private calculateAdjustment(rule: AdjustmentRule, baseAmount: number): number {
    const { type, value } = rule.adjustment;
    return type === 'percentage' ? (baseAmount * value) / 100 : value;
  }

  private async evaluateCustomRule(rule: CustomRule, records: any[]): Promise<number> {
    // Implement custom rule logic based on evaluation level, metric, and period
    return 0;
  }

  private generateResult(): ExecutionResult {
    const agents = Object.values(this.agentResults);
    const qualified = agents.filter(a => a.qualified).length;
    const totalPayout = agents.reduce((sum, a) => sum + a.commission, 0);

    return {
      success: true,
      message: `Scheme executed successfully in ${this.context.mode} mode`,
      data: {
        totalRecords: Object.values(this.baseData).reduce((sum, records) => sum + records.length, 0),
        processedAt: format(new Date(), "yyyy-MM-dd'T'HH:mm:ss"),
        summary: {
          totalAgents: agents.length,
          qualified,
          totalPayout
        },
        agents
      }
    };
  }
}