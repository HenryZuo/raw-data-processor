import fs from 'fs/promises';
import path from 'path';
import { loadSharedSchema } from './schema-loader.js';
import { processEvents } from './processor.js';
import type { SourceEvent } from './types.js';

const SOURCE_FILE = 'datathistle json sample .json';
const ENV_FILE = '.env.local';

process.on('unhandledRejection', (reason) => {
  console.error('UNHANDLED REJECTION:', reason);
  if (reason && typeof reason === 'object' && 'stack' in reason) {
    console.error('stacktrace:', (reason as Error).stack);
  }
});
process.on('uncaughtException', (error) => {
  console.error('UNCAUGHT EXCEPTION:', error);
  if (error && typeof error === 'object' && 'stack' in error) {
    console.error('stacktrace:', (error as Error).stack);
  }
});

async function loadEnvFile(filename: string): Promise<void> {
  const filePath = path.resolve(process.cwd(), filename);
  try {
    const raw = await fs.readFile(filePath, 'utf-8');
    for (const line of raw.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const equals = trimmed.indexOf('=');
      if (equals < 0) continue;
      const key = trimmed.slice(0, equals).trim();
      let value = trimmed.slice(equals + 1).trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      if (key && !process.env[key]) {
        process.env[key] = value;
      }
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      console.warn(`Failed to load ${filename}:`, (error as Error).message);
    }
  }
}

const DEFAULT_CHROMIUM_PATH = '/Applications/Chromium.app/Contents/MacOS/Chromium';

async function main(): Promise<void> {
  if (!process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    try {
      await fs.access(DEFAULT_CHROMIUM_PATH);
      process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH = DEFAULT_CHROMIUM_PATH;
    } catch {
      console.warn('Chromium executable not found at default path; please set PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH.');
    }
  }
  await loadSharedSchema();
  const sourcePath = path.resolve(process.cwd(), SOURCE_FILE);
  const raw = await fs.readFile(sourcePath, 'utf-8');
  const events: SourceEvent[] = JSON.parse(raw);
  const groqApiKey = process.env.GROQ_API_KEY;
  if (!groqApiKey) {
    throw new Error('Set GROQ_API_KEY in the environment before running this processor.');
  }

  const { activities, skipped } = await processEvents(events, groqApiKey);

  const exportName = `processed-datathistle-${new Date().toISOString().slice(0, 10)}.json`;
  const exportPath = path.resolve(process.cwd(), exportName);
  await fs.writeFile(exportPath, `${JSON.stringify(activities, null, 2)}\n`, 'utf-8');
  console.log(`Wrote ${activities.length} valid activities to ${exportName}`);
  if (skipped.length) {
    console.log(`Skipped ${skipped.length} records (${skipped.map((item) => item.reason).join('; ')})`);
  } else {
    console.log('No records were skipped.');
  }
}

async function bootstrap(): Promise<void> {
  await loadEnvFile(ENV_FILE);
  await main();
}

void bootstrap().catch((error) => {
  const detail =
    error && typeof error === 'object' && 'message' in error
      ? (error as Error).message
      : String(error);
  console.error('Processing failed:', detail);
  console.error('Full error dump:', error);
  if (error && typeof error === 'object' && 'stack' in error) {
    console.error('Stack trace:', (error as Error).stack);
  }
  process.exitCode = 1;
});
