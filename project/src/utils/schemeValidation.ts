// Update the validateSchemeJson function to handle the adjustment rules format

function validateRules(scheme: any, errors: ValidationResult['errors']): void {
  // Validate qualification rules
  if (!Array.isArray(scheme.qualificationRules)) {
    errors.push({
      type: 'error',
      message: 'Invalid qualification rules',
      details: ['qualificationRules must be an array']
    });
  } else if (scheme.qualificationRules.length === 0) {
    errors.push({
      type: 'warning',
      message: 'No qualification rules defined',
      details: ['Scheme will qualify all records']
    });
  }

  // Validate adjustment rules
  if (Array.isArray(scheme.adjustmentRules)) {
    scheme.adjustmentRules.forEach((rule: any, index: number) => {
      // Handle both old and new format
      const condition = rule.condition || {
        field: rule.field,
        operator: rule.operator,
        value: rule.value
      };

      const adjustment = rule.adjustment || {
        target: rule.adjustmentTarget,
        type: rule.adjustmentType,
        value: rule.adjustmentValue
      };

      if (!condition.field || !condition.operator || !('value' in condition)) {
        errors.push({
          type: 'error',
          message: `Invalid adjustment rule condition at index ${index}`,
          details: ['condition must have field, operator, and value']
        });
      }

      if (!adjustment.target || !adjustment.type || !('value' in adjustment)) {
        errors.push({
          type: 'error',
          message: `Invalid adjustment rule adjustment at index ${index}`,
          details: ['adjustment must have target, type, and value']
        });
      }
    });
  }

  // Validate exclusion rules
  if (!Array.isArray(scheme.exclusionRules)) {
    errors.push({
      type: 'error',
      message: 'Invalid exclusion rules',
      details: ['exclusionRules must be an array']
    });
  }
}