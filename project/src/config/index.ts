import { config } from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables from the root directory
config({ path: path.join(__dirname, '../../.env') });

// Ensure MONGODB_URI is available
const MONGODB_URI = process.env.MONGODB_URI;
if (!MONGODB_URI) {
  throw new Error('MONGODB_URI is not defined in environment variables');
}

export const CONFIG = {
  env: process.env.NODE_ENV || 'development',
  mongodb: {
    uri: MONGODB_URI,
  },
  api: {
    port: parseInt(process.env.PORT || '3000', 10),
    cors: {
      origin: process.env.CORS_ORIGIN || '*',
    },
  },
} as const;