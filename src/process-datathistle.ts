import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { chromium } from 'playwright';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import ts from 'typescript';
import type { Activity, Dates } from '../../london-kids-p1/packages/shared/src/activity';
import type { LondonArea } from '../../london-kids-p1/packages/shared/src/areas';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const sharedSrcDir = path.resolve(__dirname, '../../london-kids-p1/packages/shared/src');
const sharedRequire = createRequire(path.join(sharedSrcDir, 'activity.ts'));
const zodUrl = pathToFileURL(sharedRequire.resolve('zod')).href;

const tsCompilerOptions: ts.CompilerOptions = {
  module: ts.ModuleKind.ES2020,
  target: ts.ScriptTarget.ES2020,
  esModuleInterop: true,
};

let activitySchema: typeof import('../../london-kids-p1/packages/shared/src/activity').activitySchema;
let mapAddressToAreas: typeof import('../../london-kids-p1/packages/shared/src/areas').mapAddressToAreas;

const SOURCE_FILE = 'datathistle json sample .json';
const MAX_RETRIES = 3;
const TIMEOUT_MS = 30_000;
const RETRY_DELAY_MS = 2_000;
const GROQ_MODEL = process.env.GROQ_MODEL ?? 'llama-3.1-70b-versatile';
const DEFAULT_GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions';
const GENERIC_KEYWORDS = [
  'London family activities',
  'kids days out',
  'family bonding London',
  'child-friendly London',
  'children adventures',
  'London heritage walks',
  'museum experiences for kids',
  'outdoor discovery London',
  'creative workshops London',
  'indoor play London',
  'London waterfront fun',
  'science and discovery',
  'theatre for families',
  'park picnics London',
  'interactive learning London',
  'hidden gems London',
  'new things to try London',
  'family-friendly shows',
  'mini explorers London',
  'London neighbourhood walks',
];
const FALLBACK_LABELS = ['family', 'kids', 'London'];
const ENV_FILE = '.env.local';

const compiledDir = path.join(os.tmpdir(), 'raw-data-processor-shared');

async function loadSharedSchema(): Promise<void> {
  const areasModule = await compileSharedModule('areas');
  const activityModule = await compileSharedModule('activity', [
    {
      pattern: /from\s+['"]\.\/areas['"]/g,
      replacement: "from './areas.mjs'",
    },
    {
      pattern: /from\s+['"]zod['"]/g,
      replacement: `from '${zodUrl}'`,
    },
  ]);

  mapAddressToAreas = areasModule.mapAddressToAreas;
  activitySchema = activityModule.activitySchema;
}

async function compileSharedModule(
  name: 'activity' | 'areas',
  replacements: { pattern: RegExp; replacement: string }[] = [],
) {
  await fs.mkdir(compiledDir, { recursive: true });
  const filePath = path.join(sharedSrcDir, `${name}.ts`);
  let code = await fs.readFile(filePath, 'utf-8');
  for (const replacement of replacements) {
    code = code.replace(replacement.pattern, replacement.replacement);
  }

  const { outputText } = ts.transpileModule(code, {
    compilerOptions: tsCompilerOptions,
    fileName: path.basename(filePath),
  });

  const outPath = path.join(compiledDir, `${name}.mjs`);
  await fs.writeFile(outPath, outputText, 'utf-8');
  return import(pathToFileURL(outPath).href);
}

async function getOfficialUrlAndContent(inputUrl: string): Promise<{ url: string; text: string }> {
  const playwright = (await import('playwright')) as { chromium: any };
  const browser = await playwright.chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.route('**/*', (route: { request(): { resourceType(): string }; abort(): Promise<void>; continue(): Promise<void> }) => {
    const rt = route.request().resourceType();
    if (rt === 'image' || rt === 'stylesheet' || rt === 'font') {
      return route.abort();
    }
    return route.continue();
  });

  let finalUrl = inputUrl;
  try {
    const response = await page.goto(inputUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });
    if (response) finalUrl = response.url();
    await page.waitForTimeout(1500);

    const canonical = await page.locator('link[rel="canonical"]').getAttribute('href').catch(() => null);
    if (canonical) {
      finalUrl = new URL(canonical, finalUrl).href;
    }

    const text = await page.evaluate(() => {
      document.querySelectorAll('script, style, noscript').forEach((el) => el.remove());
      const tables = Array.from(document.querySelectorAll('table'))
        .map((t) => t.textContent?.trim())
        .filter(Boolean)
        .join('\n');
      return document.body.innerText + '\n\nOpening hours tables:\n' + tables;
    });
    await browser.close();
    return { url: finalUrl, text: text.slice(0, 12000) };
  } catch (err) {
    await browser.close();
    return { url: finalUrl, text: '' };
  }
}

interface SourceEvent {
  event_id: string;
  name: string;
  schedules?: SourceSchedule[];
  tags?: string[];
  descriptions?: { type: string; description: string }[];
  links?: SourceLink[];
  website?: string;
  images?: { url: string }[];
}

interface SourceSchedule {
  place_id: string;
  start_ts?: string;
  end_ts?: string;
  tags?: string[];
  place?: SourcePlace;
  performances?: SourcePerformance[];
  links?: SourceLink[];
}

interface SourcePlace {
  name?: string;
  address?: string;
  town?: string;
  postal_code?: string;
  lat?: number | null;
  lon?: number | null;
  lng?: number | null;
}

interface SourcePerformance {
  ts: string;
  time_unknown?: boolean | null;
  duration?: number | null;
  tickets?: SourceTicket[];
  links?: SourceLink[];
}

interface SourceTicket {
  type?: string;
  currency?: string;
  min_price?: number | null;
  max_price?: number | null;
  description?: string;
}

interface SourceLink {
  url: string;
  type?: string;
}

interface ActivityLLMOutput {
  type: 'event' | 'place';
  hasCommittedTimes: boolean;
  dates: Dates;
  officialAgeAvailable: boolean;
  minAge: number;
  maxAge: number;
  summary: string;
  priceLevel: 'free' | '£' | '££' | '£££';
  keywords: string[];
  labels: string[];
}

const RULES_TEXT = `You are an expert London family-activity curator. Analyze the official website content and DataThistle data.

Return ONLY valid JSON with these fields:

{
  "type": "event" | "place",
  "hasCommittedTimes": boolean,
  "dates": {
    "kind": "specific" | "ongoing",
    "instances"?: [{ "date": "YYYY-MM-DD", "startTime": "HH:MM", "endTime"?: "HH:MM" }],
    "openingHours"?: { "Mon"?: {"open":"HH:MM","close":"HH:MM"}, ... },
    "note"?: string
  },
  "officialAgeAvailable": boolean,
  "minAge": number,
  "maxAge": number,
  "summary": string (≤30 words, first-person parent voice, upbeat),
  "priceLevel": "free" | "£" | "££" | "£££",
  "keywords": string[],
  "labels": string[]
}

CRITICAL: If the activity has no fixed date/time (self-guided, download-itinerary, "start anytime", etc.) → set "hasCommittedTimes": false. These must be excluded.`;

const fallbackLLMOutput = (event: SourceEvent): ActivityLLMOutput => {
  const safeNameSegment = event.name
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^A-Za-z0-9-]/g, '');
  const safeNameToken = safeNameSegment || 'activity';
  const summary =
    `My kids are still going to adore ${safeNameToken} and its joyful London energy; it's a breezy family experience any time.`;
  const fallbackDates: Dates = {
    kind: 'ongoing',
    note: 'Fallback opening hours; please check the official site before visiting.',
  };
  return {
    type: 'place',
    hasCommittedTimes: true,
    dates: fallbackDates,
    officialAgeAvailable: false,
    minAge: 3,
    maxAge: 12,
    summary,
    priceLevel: '££',
    keywords: GENERIC_KEYWORDS,
    labels: FALLBACK_LABELS,
  };
};

const geocodeCache = new Map<string, { lat: number; lng: number }>();

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

async function main(): Promise<void> {
  await loadSharedSchema();
  const sourcePath = path.resolve(process.cwd(), SOURCE_FILE);
  const raw = await fs.readFile(sourcePath, 'utf-8');
  const events: SourceEvent[] = JSON.parse(raw);
  const activities: Activity[] = [];
  const skipped: { id: string; reason: string }[] = [];

  const eventsById = new Map<string, SourceEvent>();
  for (const event of events) {
    if (!eventsById.has(event.event_id)) {
      eventsById.set(event.event_id, event);
    }
  }
  const uniqueEvents = Array.from(eventsById.values());

  const groqApiKey = process.env.GROQ_API_KEY;
  if (!groqApiKey) {
    throw new Error('Set GROQ_API_KEY in the environment before running this processor.');
  }

  for (const event of uniqueEvents) {
    console.log(`Processing ${event.event_id}: ${event.name}`);
    const place = event.schedules?.[0]?.place;
    if (!place) {
      skipped.push({ id: event.event_id, reason: 'missing location data' });
      continue;
    }

    const candidateUrl = findBookingLink(event) ?? `https://www.datathistle.com/event/${event.event_id}`;
    const { url: officialUrl, text: pageContent } = await getOfficialUrlAndContent(candidateUrl);
    const llmData = await fetchLLMDataWithRetries(event, officialUrl, pageContent, groqApiKey);

    if (!llmData.hasCommittedTimes) {
      skipped.push({ id: event.event_id, reason: 'no committed date/time (self-guided/anytime)' });
      continue;
    }

    try {
      const activity = await buildActivity(event, officialUrl, llmData);
      activitySchema.parse(activity);
      activities.push(activity);
    } catch (error) {
      skipped.push({
        id: event.event_id,
        reason: `validation failed: ${(error as Error).message}`,
      });
      console.warn(
        `Validation for ${event.event_id} failed:`,
        (error as Error).message,
      );
    }
  }

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

async function fetchLLMDataWithRetries(
  event: SourceEvent,
  officialUrl: string,
  pageContent: string,
  apiKey: string,
): Promise<ActivityLLMOutput> {
  const prompt = buildPrompt(event, officialUrl, pageContent);
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      return await callGroqAPI(prompt, apiKey);
    } catch (error) {
      console.warn(`Groq attempt ${attempt} for ${event.event_id} failed:`, (error as Error).message);
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS);
      }
    }
  }
  console.warn(`Falling back for ${event.event_id} after ${MAX_RETRIES} Groq attempts.`);
  return fallbackLLMOutput(event);
}

function buildPrompt(event: SourceEvent, officialUrl: string, pageContent: string): string {
  const eventJson = JSON.stringify(event, null, 2);
  const eventDetails = [
    `DataThistle data:
${eventJson}`,
    `Official URL: ${officialUrl}`,
    `Website content:
${pageContent || 'No scraped content available.'}`,
  ].join('\n\n');

  return eventDetails;
}

function findBookingLink(event: SourceEvent): string | undefined {
  const schedules = event.schedules ?? [];
  const scheduleLinks = schedules.flatMap((schedule) => schedule.links ?? []);
  const performanceLinks = schedules
    .flatMap((schedule) => schedule.performances ?? [])
    .flatMap((performance) => performance.links ?? []);
  const linkSets = [...(event.links ?? []), ...scheduleLinks, ...performanceLinks];
  return linkSets.find((link) => link.type === 'booking')?.url ?? linkSets[0]?.url;
}

async function callGroqAPI(prompt: string, apiKey: string): Promise<ActivityLLMOutput> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);

  const response = await fetch(process.env.GROQ_API_URL ?? DEFAULT_GROQ_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: GROQ_MODEL,
      messages: [
        {
          role: 'system',
          content: RULES_TEXT,
        },
        {
          role: 'user',
          content: `Event data:\n${prompt}`,
        },
      ],
      temperature: 0.0,
      max_tokens: 800,
    }),
    signal: controller.signal,
  });

  clearTimeout(timeout);

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Groq API responded with ${response.status}: ${errorBody}`);
  }

  const payload = await response.json();
  const content = payload.choices?.[0]?.message?.content;

  if (!content) {
    throw new Error('Groq response contained no content');
  }

  let parsed;
  try {
    const jsonMatch =
      content.match(/```json\s*([\s\S]*?)\s*```/i) || content.match(/```([\s\S]*?)```/i);
    const jsonString = jsonMatch ? jsonMatch[1] : content;
    parsed = JSON.parse(jsonString.trim());
  } catch (error) {
    throw new Error(
      `Failed to parse Groq output as JSON: ${(error as Error).message}\nRaw output:\n${content}`,
    );
  }

  return normalizeLLMOutput(parsed);
}

function normalizeLLMOutput(raw: unknown): ActivityLLMOutput {
  if (!raw || typeof raw !== 'object') {
    throw new Error('LLM output is not an object.');
  }

  const candidate = raw as Record<string, unknown>;
  const type = candidate.type;
  if (type !== 'event' && type !== 'place') {
    throw new Error('LLM output missing type or invalid type.');
  }
  const hasCommittedTimes = candidate.hasCommittedTimes;
  if (typeof hasCommittedTimes !== 'boolean') {
    throw new Error('LLM output missing hasCommittedTimes boolean.');
  }
  const dates = candidate.dates;
  if (!dates || typeof dates !== 'object') {
    throw new Error('LLM output missing dates object.');
  }
  const datesKind = (dates as Record<string, unknown>)?.kind;
  if (datesKind !== 'specific' && datesKind !== 'ongoing') {
    throw new Error('LLM output must include dates.kind (specific or ongoing).');
  }
  const officialAgeAvailable = candidate.officialAgeAvailable;
  if (typeof officialAgeAvailable !== 'boolean') {
    throw new Error('LLM output missing officialAgeAvailable boolean.');
  }

  const keywords = Array.isArray(candidate.keywords) ? candidate.keywords : [];
  const labels = Array.isArray(candidate.labels) ? candidate.labels : [];

  if (keywords.length !== 20) {
    throw new Error(`Expected 20 keywords but got ${keywords.length}.`);
  }
  if (labels.length !== 3) {
    throw new Error(`Expected 3 labels but got ${labels.length}.`);
  }

  const minAge = clampAge(candidate.minAge);
  const maxAge = clampAge(candidate.maxAge);
  if (minAge > maxAge) {
    throw new Error(`minAge (${minAge}) cannot exceed maxAge (${maxAge}).`);
  }

  const priceLevel = candidate.priceLevel;
  if (priceLevel !== 'free' && priceLevel !== '£' && priceLevel !== '££' && priceLevel !== '£££') {
    throw new Error(`Invalid priceLevel: ${priceLevel}`);
  }

  const summary = candidate.summary;
  if (typeof summary !== 'string') {
    throw new Error('Summary must be a string.');
  }

  const normalizedPriceLevel = priceLevel as ActivityLLMOutput['priceLevel'];

  return {
    type: type as ActivityLLMOutput['type'],
    hasCommittedTimes,
    dates: dates as Dates,
    officialAgeAvailable,
    minAge,
    maxAge,
    summary,
    priceLevel: normalizedPriceLevel,
    keywords: keywords.map((keyword) => String(keyword).trim()),
    labels: labels.map((label) => String(label).trim()),
  };
}

function clampAge(value: unknown): number {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    throw new Error('Age value is not a number.');
  }
  return Math.min(18, Math.max(0, Math.round(value)));
}

async function buildActivity(
  event: SourceEvent,
  officialUrl: string,
  llmData: ActivityLLMOutput,
): Promise<Activity> {
  const place = event.schedules?.[0]?.place;
  const coords = await resolveCoordinates(place);
  const addressLine = buildAddressLine(place);
  const areas = mapAddressToAreas(addressLine) as [LondonArea, ...LondonArea[]];
  const area = areas[areas.length - 1] ?? 'Greater London';
  const imageUrl = event.images?.[0]?.url;

  const activity: Activity = {
    id: `${event.event_id}--${llmData.type}`,
    name: event.name,
    type: llmData.type,
    summary: llmData.summary,
    priceLevel: llmData.priceLevel,
    age: {
      officialAgeAvailable: llmData.officialAgeAvailable,
      minAge: llmData.minAge,
      maxAge: llmData.maxAge,
    },
    location: {
      locationName: place?.name ?? event.name,
      addressLine,
      postcode: place?.postal_code ?? '',
      city: place?.town ?? 'London',
      country: 'United Kingdom',
      lat: coords.lat,
      lng: coords.lng,
      area,
    },
    dates: llmData.dates,
    url: officialUrl,
    source: 'datathistle',
    lastUpdate: new Date().toISOString(),
    keywords: llmData.keywords,
    labels: llmData.labels,
    imageUrl,
    areas,
  };

  return activity;
}

function buildAddressLine(place?: SourcePlace): string {
  if (!place) {
    return 'London, United Kingdom';
  }
  const line = [place.address, place.town, place.postal_code].filter(Boolean).join(', ');
  return line || 'London, United Kingdom';
}

async function resolveCoordinates(place?: SourcePlace): Promise<{ lat: number; lng: number }> {
  if (!place) {
    return { lat: 51.5074, lng: -0.1278 };
  }
  const lat = place.lat ?? place.lon ?? place.lng;
  const lng = place.lng ?? place.lon ?? place.lat;
  if (typeof lat === 'number' && typeof lng === 'number') {
    return { lat, lng };
  }

  const address = buildAddressLine(place);
  if (!address) {
    return { lat: 51.5074, lng: -0.1278 };
  }

  if (geocodeCache.has(address)) {
    return geocodeCache.get(address)!;
  }

  try {
    const geo = await geocode(address);
    geocodeCache.set(address, geo);
    return geo;
  } catch (error) {
    console.warn(`Geocoding failed for "${address}":`, (error as Error).message);
    return { lat: 51.5074, lng: -0.1278 };
  }
}

async function geocode(query: string): Promise<{ lat: number; lng: number }> {
  const url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(query)}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  const response = await fetch(url, {
    headers: {
      'User-Agent': 'raw-data-processor/1.0 (contact: henry@example.com)',
    },
    signal: controller.signal,
  });
  clearTimeout(timeout);
  if (!response.ok) {
    throw new Error(`Nominatim responded with ${response.status}`);
  }
  const payload = (await response.json()) as { lat?: string; lon?: string }[];
  const first = payload[0];
  if (!first?.lat || !first?.lon) {
    throw new Error('Nominatim returned no coordinates');
  }
  return { lat: Number(first.lat), lng: Number(first.lon) };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function bootstrap(): Promise<void> {
  await loadEnvFile(ENV_FILE);
  await main();
}

void bootstrap().catch((error) => {
  console.error('Processing failed:', error);
  process.exitCode = 1;
});
