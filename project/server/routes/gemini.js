import express from 'express';
import { GoogleGenAI } from '@google/genai';
import dotenv from 'dotenv';
import mime from 'mime';

dotenv.config();

const router = express.Router();

const SCHEME_EXECUTION_PROMPT = `...`; // keep as-is

router.post('/generateCode', async (req, res) => {
  console.log('[INFO] /generateCode endpoint hit');

  try {
    const { scheme } = req.body;
    console.log('[DEBUG] Received scheme:', JSON.stringify(scheme, null, 2));

    if (!scheme) {
      console.warn('[WARN] No scheme provided in request body');
      return res
        .status(400)
        .json({ error: 'Scheme configuration is required' });
    }

    console.log('[INFO] Initializing Gemini...');
    const ai = new GoogleGenAI({
      apiKey: process.env.GEMINI_API_KEY,
    });
    const model = 'gemini-2.5-pro-preview-03-25';

    const contents = [
      {
        parts: [
          { text: SCHEME_EXECUTION_PROMPT },
          { text: JSON.stringify(scheme, null, 2) },
        ],
      },
    ];

    console.log('[INFO] Sending content to Gemini...');
    const result = await ai.models.generateContent({
      contents,
      generationConfig: {
        temperature: 0.1,
        topK: 1,
        topP: 1,
        maxOutputTokens: 8192,
      },
    });

    console.log('[INFO] Gemini API call successful');

    const response = await result.response;
    const codeBlock = response.text();

    console.log('[INFO] Received code from Gemini');
    res.json({ code: codeBlock });
  } catch (error) {
    console.error('[ERROR] Error calling Gemini:', error?.message || error);

    if (
      error instanceof Error &&
      error.message.includes('GenerateContentRequest.safety_settings')
    ) {
      console.error('[ERROR] Likely invalid or missing safety_settings.');
    }

    res.status(500).json({
      error: 'Failed to generate scheme execution code from Gemini',
      details: error?.message || 'Unknown error',
    });
  }
});

export default router;
