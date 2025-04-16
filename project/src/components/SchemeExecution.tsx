// Update the handleSchemeUpload function in SchemeExecution.tsx

const handleSchemeUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
  const file = event.target.files?.[0];
  if (file) {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const scheme = JSON.parse(e.target?.result as string);
        console.log('Loaded scheme:', scheme);
        
        // Clear previous state first
        setValidationErrors([]);
        setExecutionResult(null);
        setUploadedFiles({});
        setMissingColumns({});
        
        const validation = validateSchemeJson(scheme);
        
        // Set validation errors if any
        if (validation.errors.length > 0) {
          setValidationErrors(validation.errors);
          // Only clear scheme if there are actual errors (not just warnings)
          if (!validation.valid) {
            setSelectedScheme(null);
            return;
          }
        }

        // Set default status if not provided
        if (!scheme.status) {
          scheme.status = 'Draft';
        }
        
        // Generate an ID if not present
        if (!scheme.id) {
          scheme.id = crypto.randomUUID();
        }
        
        // Update scheme and required files
        setSelectedScheme(scheme);
        const required = getRequiredFiles(scheme);
        setRequiredFiles(required);

      } catch (err) {
        console.error('Error parsing scheme:', err);
        setValidationErrors([{
          type: 'error',
          message: 'Failed to parse scheme file',
          details: [err instanceof Error ? err.message : 'Invalid format']
        }]);
        setSelectedScheme(null);
      }
    };

    reader.onerror = () => {
      setValidationErrors([{
        type: 'error',
        message: 'Failed to read file',
        details: ['Please check the file and try again']
      }]);
      setSelectedScheme(null);
    };

    reader.readAsText(file);
  }
};