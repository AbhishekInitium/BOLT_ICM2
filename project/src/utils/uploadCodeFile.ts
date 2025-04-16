import { promises as fs } from 'fs';
import path from 'path';

export async function uploadSchemeCodeFile(schemeName: string, file: File): Promise<string> {
  if (!file) {
    throw new Error('File is required');
  }

  console.log('[Upload] Starting file upload');
  const formData = new FormData();
  formData.append('schemeFile', file);

  try {
    console.log('[Upload] Sending request to server');
    const response = await fetch('/api/schemes/code', {
      method: 'POST',
      body: formData
    });

    if (!response.ok) {
      const error = await response.json();
      console.error('[Upload] Server returned error:', error);
      throw new Error(error.details || error.message || 'Failed to upload file');
    }

    const result = await response.json();
    console.log('[Upload] Upload successful:', result);
    return result.path;
  } catch (error) {
    console.error('[Upload] Upload failed:', error);
    throw error;
  }
}