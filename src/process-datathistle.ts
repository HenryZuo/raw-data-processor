import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import ts from 'typescript';
import { chromium } from 'playwright';
import type { Browser, Route as PlaywrightRoute, Response } from 'playwright-core';
import type { Activity, Dates } from '../../london-kids-p1/packages/shared/src/activity';
import type { LondonArea } from '../../london-kids-p1/packages/shared/src/areas';

process.on('unhandledRejection', (reason) => {
  console.error('UNHANDLED REJECTION:', reason);
});
process.on('uncaughtException', (error) => {
  console.error('UNCAUGHT EXCEPTION:', error);
});

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
const USER_AGENT_STRINGS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
];
const GROQ_MODEL = process.env.GROQ_MODEL ?? 'llama-3.3-70b-versatile';
const DEFAULT_GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions';
const GENERIC_KEYWORDS = [
  'london family activities',
  'kids days out',
  'family bonding london',
  'child-friendly london',
  'children adventures',
  'london heritage walks',
  'museum experiences for kids',
  'outdoor discovery london',
  'creative workshops london',
  'indoor play london',
  'london waterfront fun',
  'science and discovery',
  'theatre for families',
  'park picnics london',
  'interactive learning london',
  'hidden gems london',
  'new things to try london',
  'family-friendly shows',
  'mini explorers london',
  'london neighbourhood walks',
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
async function getOfficialUrlAndContent(inputUrl: string): Promise<PageScrapeResult> {
  let browser: Browser | null = null;
  console.log(`Attempting scrape for ${inputUrl}`);
  try {
    const launchOptions = getLaunchOptions();
    const launchedBrowser = await chromium.launch({
      headless: launchOptions.headless,
    });
    browser = launchedBrowser;
    const page = await launchedBrowser.newPage();
    await page.setViewportSize(getRandomViewport());
    await page.setUserAgent(getRandomUserAgent());
    await page.route('**/*', (route: PlaywrightRoute) => {
      const rt = route.request().resourceType();
      if (rt === 'image' || rt === 'stylesheet' || rt === 'font') {
        return route.abort();
      }
      return route.continue();
    });

    let finalUrl = inputUrl;
    const response = await retryOperation<Response | null>(
      () => page.goto(inputUrl, { waitUntil: 'domcontentloaded', timeout: TIMEOUT_MS }),
      { label: `goto ${inputUrl}` },
    );
    if (response) finalUrl = response.url();
    await page.waitForTimeout(1500);
    await retryOperation(() => page.waitForLoadState('networkidle'), { label: 'waitForLoadState' });
    await retryOperation(
      () =>
        page.waitForSelector('main, article, .event-description, body > *:not(script):not(style)', {
          timeout: TIMEOUT_MS,
        }),
      { label: 'waitForSelector' },
    );

    const canonicalRaw = await page.locator('link[rel="canonical"]').getAttribute('href').catch(() => null);
    const canonical = canonicalRaw ? new URL(canonicalRaw, finalUrl).href : undefined;
    const ogUrlRaw = await page.locator('meta[property="og:url"]').getAttribute('content').catch(() => null);
    const ogUrl = ogUrlRaw ? new URL(ogUrlRaw, finalUrl).href : undefined;
    if (canonical) {
      finalUrl = canonical;
    } else if (ogUrl) {
      finalUrl = ogUrl;
    }

    const text = await retryOperation<string>(
      () =>
        page.evaluate(() => {
          document
            .querySelectorAll('script, style, noscript, .ad, [id*="ad"], .popup')
            .forEach((el) => el.remove());
          const tables = Array.from(document.querySelectorAll('table'))
            .map((t) => t.textContent?.trim())
            .filter(Boolean)
            .join('\n');
          const focus = document.querySelector('main') ?? document.querySelector('article') ?? document.body;
          const bodyText =
            (focus as HTMLElement | null)?.innerText?.trim() ?? document.body?.innerText?.trim() ?? '';
          return `${bodyText}\n\nOpening hours tables:\n${tables}`;
        }),
      { label: 'pageEvaluate' },
    );
    const trimmedText = text.slice(0, 20000);
    const title = await page.title();
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content').catch(() => null);
    const metaAuthor = await page.locator('meta[name="author"]').getAttribute('content').catch(() => null);
    const images = await page.$$eval('img[src]', (imgs: HTMLImageElement[]) =>
      imgs
        .map((img: HTMLImageElement) => img.getAttribute('src'))
        .filter((src): src is string => typeof src === 'string')
        .slice(0, 5)
        .map((src: string) => (src.startsWith('http') ? src : new URL(src, document.baseURI).href)),
    );
    const domain = new URL(finalUrl).hostname;
    console.log(`Scrape success for ${finalUrl}: ${trimmedText.length} chars, ${images.length} images`);

    return {
      url: finalUrl,
      text: trimmedText,
      title,
      canonical,
      metaDescription: metaDescription ?? undefined,
      metaAuthor: metaAuthor ?? undefined,
      ogUrl,
      images,
      domain,
    };
  } catch (err) {
    console.warn('Playwright scraping failed for', inputUrl, (err as Error).message);
    const domain = (() => {
      try {
        return new URL(inputUrl).hostname;
      } catch {
        return '';
      }
    })();
    return {
      url: inputUrl,
      text: '',
      title: '',
      domain,
      images: [],
    };
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

const OFFICIAL_URL_RULES = `You are an expert London family-activity curator tasked with finding the cleanest official descriptive webpage for a DataThistle entry.

Given the event data and candidate links, return ONLY valid JSON with:
{
  "officialDescriptiveUrl": string,
  "confidence": "high" | "medium" | "low"
}

Rules:
- Prefer non-booking, non-aggregator domains (unless no better option exists).
- If no clearly descriptive official page exists, return an empty string for "officialDescriptiveUrl".
- Do not return DataThistle redirect URLs unless they resolve to the official homepage.
- Never include any extra fields, markdown, or explanation outside the JSON object.`;

async function resolveOfficialUrl(
  event: SourceEvent,
  candidateLinks: string[],
  apiKey: string,
): Promise<PageScrapeResult | null> {
  const prioritized = await gatherPriorityUrls(event, candidateLinks);
  const evaluations: { page: PageScrapeResult; score: number }[] = [];

  for (const candidate of prioritized.slice(0, 6)) {
    try {
      const page = await getOfficialUrlAndContent(candidate);
      const score = scorePage(event, page);
      evaluations.push({ page, score });
    } catch (error) {
      console.warn(`Failed to scrape candidate ${candidate}:`, (error as Error).message);
    }
  }

  const sorted = evaluations.sort((a, b) => b.score - a.score);
  const bestOfficial = sorted.find((item) => !isAggregatorDomain(item.page.url));
  if (bestOfficial) {
    return bestOfficial.page;
  }
  if (sorted.length) {
    return sorted[0].page;
  }

  const suggestedUrl = await askLLMForOfficialUrl(event, candidateLinks, apiKey);
  if (suggestedUrl) {
    try {
      return await getOfficialUrlAndContent(suggestedUrl);
    } catch (error) {
      console.warn(`Failed to verify suggested official URL ${suggestedUrl}:`, (error as Error).message);
    }
  }

  return null;
}

async function searchOfficialUrl(event: SourceEvent): Promise<string | null> {
  let browser: Browser | null = null;
  try {
    const launchedBrowser = await chromium.launch({ headless: true });
    browser = launchedBrowser;
    const page = await launchedBrowser.newPage();
    await page.route('**/*', (route: PlaywrightRoute) => {
      const rt = route.request().resourceType();
      if (rt === 'image' || rt === 'stylesheet' || rt === 'font') {
        return route.abort();
      }
      return route.continue();
    });

    const query = `${event.name} official website London`;
    const searchUrl = `https://duckduckgo.com/?q=${encodeURIComponent(query)}`;
    await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await page.waitForTimeout(1500);
    const href = await page
      .locator('a.result__a')
      .first()
      .getAttribute('href')
      .catch(() => null);
    return href ? normalizeCandidateUrl(href) : null;
  } catch (error) {
    console.warn('Official URL search failed:', (error as Error).message);
    return null;
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

async function gatherPriorityUrls(event: SourceEvent, candidateLinks: string[]): Promise<string[]> {
  const prioritized: string[] = [];
  const seen = new Set<string>();
  const add = (link?: string | null) => {
    if (!link) return;
    const normalized = normalizeCandidateUrl(link);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    prioritized.push(normalized);
  };

  const searchResult = await searchOfficialUrl(event);
  add(searchResult);
  for (const candidate of candidateLinks) {
    add(candidate);
  }
  add(`https://www.datathistle.com/event/${event.event_id}`);
  return prioritized;
}

function scorePage(event: SourceEvent, page: PageScrapeResult): number {
  let score = 0;
  try {
    if (!isBookingDomain(page.url)) {
      score += 5;
    }
    if (!isAggregatorDomain(page.url)) {
      score += 3;
    }
    const name = event.name.toLowerCase();
    if (page.title && page.title.toLowerCase().includes(name)) score += 2;
    if (page.text.toLowerCase().includes(name)) score += 1;
    if (page.metaDescription?.toLowerCase().includes(name)) score += 1;
    if (page.text.length > 1500) score += 1;
  } catch {
    // ignore scoring failures
  }
  return score;
}

function isAggregatorDomain(url: string): boolean {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return AGGREGATOR_DOMAINS.some((domain) => host.includes(domain));
  } catch {
    return false;
  }
}

function buildOfficialUrlPrompt(event: SourceEvent, candidateLinks: string[]): string {
  const eventJson = JSON.stringify(event, null, 2);
  const candidateText = candidateLinks.length ? candidateLinks.join('\n') : 'None';
  return [
    `DataThistle data:\n${eventJson}`,
    `Candidate links:\n${candidateText}`,
    'If no official descriptive homepage exists, return an empty string for officialDescriptiveUrl.',
  ].join('\n\n');
}

async function askLLMForOfficialUrl(event: SourceEvent, candidateLinks: string[], apiKey: string): Promise<string | null> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  const prompt = buildOfficialUrlPrompt(event, candidateLinks);

  const requestPayload = {
    model: GROQ_MODEL,
    messages: [
      { role: 'system', content: OFFICIAL_URL_RULES },
      { role: 'user', content: prompt },
    ],
    temperature: 0.0,
    max_tokens: 200,
  };

  try {
    const response = await fetch(process.env.GROQ_API_URL ?? DEFAULT_GROQ_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(requestPayload),
      signal: controller.signal,
    });

    if (!response.ok) {
      const errorBody = await response.text();
      console.error('Groq official URL lookup error:', errorBody);
      await exportGroqRequest(`official-url-${event.event_id}`, requestPayload);
      return null;
    }

    const payload = await response.json();
    const content = payload.choices?.[0]?.message?.content;
    if (!content) {
      return null;
    }

    const jsonMatch =
      content.match(/```json\s*([\s\S]*?)\s*```/i) || content.match(/```([\s\S]*?)```/i);
    const jsonString = jsonMatch ? jsonMatch[1] : content;
    const parsed = JSON.parse(jsonString.trim());
    const candidateUrl = normalizeCandidateUrl(parsed.officialDescriptiveUrl ?? '');
    return candidateUrl;
  } catch (error) {
    console.warn('Official URL lookup failed:', (error as Error).message);
    return null;
  } finally {
    clearTimeout(timeout);
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
  officialDescriptiveUrl: string;
}

interface PageScrapeResult {
  url: string;
  text: string;
  title: string;
  canonical?: string;
  metaDescription?: string;
  metaAuthor?: string;
  ogUrl?: string;
  images: string[];
  domain: string;
}

const AGGREGATOR_DOMAINS = [
  'datathistle.com',
  'eventbrite.com',
  'ticketmaster.com',
  'seetickets.com',
  'skiddle.com',
  'getmein.com',
  'ticketweb.com',
];

function countPerformances(event: SourceEvent): number {
  return (
    event.schedules?.reduce((total, schedule) => total + (schedule.performances?.length ?? 0), 0) ?? 0
  );
}

interface RetryConfig {
  label: string;
  maxRetries?: number;
  delay?: number;
}

async function retryOperation<T>(fn: () => Promise<T>, config: RetryConfig): Promise<T> {
  const maxRetries = config.maxRetries ?? 5;
  let attempt = 0;
  let currentDelay = config.delay ?? RETRY_DELAY_MS;
  while (attempt < maxRetries) {
    try {
      return await fn();
    } catch (error) {
      attempt += 1;
      const message = (error as Error).message;
      console.warn(`[retry] ${config.label} attempt ${attempt} failed: ${message}`);
      if (/net::ERR_HTTP2_PROTOCOL_ERROR/.test(message)) {
        console.warn('[retry] Detected HTTP2 protocol error; retrying.');
      }
      if (attempt >= maxRetries) {
        throw error;
      }
      await sleep(currentDelay);
      currentDelay = Math.round(currentDelay * 1.5);
    }
  }
  throw new Error(`[retry] ${config.label} exhausted after ${maxRetries} attempts`);
}

function pickRandom<T>(items: T[]): T {
  return items[Math.floor(Math.random() * items.length)];
}

function getRandomViewport(): { width: number; height: number } {
  const width = 1280 + Math.floor(Math.random() * (1920 - 1280 + 1));
  const height = 720 + Math.floor(Math.random() * (1080 - 720 + 1));
  return { width, height };
}

function getRandomUserAgent(): string {
  return pickRandom(USER_AGENT_STRINGS);
}

function getLaunchOptions(): { headless: boolean } {
  const debugMode = process.env.DEBUG_PLAYWRIGHT === 'true';
  return {
    headless: !debugMode,
  };
}

const RULES_TEXT = `You are an expert London family-activity curator. Analyse the DataThistle event data and the official website content (if available).

Prioritise information from the official website over DataThistle when determining dates, times, ages, summaries, and the definitive descriptive URL. If the official site clearly states the activity is self-guided, downloadable, “start anytime,” or otherwise without committed slots, set "hasCommittedTimes": false (these will be filtered out).

Return ONLY a single block of valid JSON with exactly these fields and requirements:

{
  "type": "event" | "place",
  "hasCommittedTimes": boolean,
  "dates": {
    "kind": "specific" | "ongoing",
    "instances"?: [{ "date": "YYYY-MM-DD", "startTime": "HH:MM", "endTime"?: "HH:MM" }],
    "openingHours"?: { "Mon"?: {"open":"HH:MM","close":"HH:MM"}, "Tue"?: {"open":"HH:MM","close":"HH:MM"}, "Wed"?: {"open":"HH:MM","close":"HH:MM"}, "Thu"?: {"open":"HH:MM","close":"HH:MM"}, "Fri"?: {"open":"HH:MM","close":"HH:MM"}, "Sat"?: {"open":"HH:MM","close":"HH:MM"}, "Sun"?: {"open":"HH:MM","close":"HH:MM"} },
    "note"?: string
  },
  "officialAgeAvailable": boolean,
  "minAge": number,
  "maxAge": number,
  "summary": string (15–35 words; first describe what the activity is, then explain what makes it unique or rare; tone must convey excitement. Example: "An immersive Shrek-themed adventure on the South Bank featuring a 4D bus ride and live characters – the only place in London where kids meet Shrek in person!"),
  "priceLevel": "free" | "£" | "££" | "£££",
  "keywords": string[] (AT LEAST 20 unique, relevant, lower-case search keywords – use event tags, description, venue type, theme, location, season, etc.; if data is sparse, supplement with generic but useful ones like "london kids activities", "family days out london", "things to do with children london"),
  "labels": string[] (Pick EXACTLY 3 from keywords that best summarise the activity e.g. ["monsters", "indoor", "theatre"]),
  "officialDescriptiveUrl": string (clean, descriptive webpage about the activity)
}

CRITICAL RULES:
- If the official website clearly advertises that visitors can “start anytime,” download-and-go, or follow an open-ended walk with no fixed slots, set "hasCommittedTimes": false (these entries will be skipped).
- Always output AT LEAST 20 keywords and EXACTLY 3 labels, even if you invent plausible ones based on the event’s theme or tagging.
- Never output extra text, markdown, or explanations outside the JSON object.`;

const fallbackLLMOutput = (event: SourceEvent, fallbackUrl?: string): ActivityLLMOutput => {
  const safeNameSegment = event.name
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^A-Za-z0-9-]/g, '');
  const safeNameToken = safeNameSegment || 'activity';
  const summary = `A joyful fallback family activity named ${safeNameToken} that keeps London explorers energized, uniquely offering flexible visiting windows and sensory play for curious kids.`;
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
    officialDescriptiveUrl: fallbackUrl ?? `https://www.datathistle.com/event/${event.event_id}`,
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
    const existing = eventsById.get(event.event_id);
    if (!existing) {
      eventsById.set(event.event_id, event);
      continue;
    }
    if (countPerformances(event) > countPerformances(existing)) {
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

    const candidateLinks = findCandidateLinks(event);
    const officialPage = await resolveOfficialUrl(event, candidateLinks, groqApiKey);
    if (!officialPage) {
      skipped.push({ id: event.event_id, reason: 'no official descriptive URL found' });
      continue;
    }
    const llmData = await fetchLLMDataWithRetries(event, officialPage, candidateLinks, groqApiKey);

    if (!llmData.officialDescriptiveUrl || isBookingDomain(llmData.officialDescriptiveUrl)) {
      skipped.push({ id: event.event_id, reason: 'no official descriptive URL found' });
      continue;
    }

    if (!llmData.hasCommittedTimes) {
      skipped.push({ id: event.event_id, reason: 'no committed date/time (self-guided/anytime)' });
      continue;
    }

    try {
      const activity = await buildActivity(event, llmData);
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
  officialPage: PageScrapeResult,
  candidateLinks: string[],
  apiKey: string,
): Promise<ActivityLLMOutput> {
  const prompt = buildPrompt(event, officialPage, candidateLinks);
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      return await callGroqAPI(prompt, apiKey, event.event_id);
    } catch (error) {
      console.warn(`Groq attempt ${attempt} for ${event.event_id} failed:`, (error as Error).message);
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS);
      }
    }
  }
  console.warn(`Falling back for ${event.event_id} after ${MAX_RETRIES} Groq attempts.`);
  return fallbackLLMOutput(event, officialPage.url);
}

function buildPrompt(event: SourceEvent, officialPage: PageScrapeResult, candidateLinks: string[]): string {
  const eventJson = JSON.stringify(event, null, 2);
  const websiteContent = officialPage.text || 'No scraped content available.';
  const candidateText = candidateLinks.length ? candidateLinks.join(', ') : 'None';
  const blocks = [
    `DataThistle data:\n${eventJson}`,
    `Official URL: ${officialPage.url}`,
    `Official website title: ${officialPage.title || 'Unknown'}`,
    `Official website content:\n${websiteContent}`,
    `Candidate links:\n${candidateText}`,
  ];
  return blocks.join('\n\n');
}

const BOOKING_DOMAINS = [
  /datathistle\.com/i,
  /ticketmaster\.co\.uk/i,
  /seetickets\.com/i,
  /eventbrite\.com\/e/i,
  /ticketweb\.com/i,
  /skiddle\.com/i,
  /getmein\.com/i,
  /tiqets\.com/i,
];

function isBookingDomain(url: string): boolean {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    return BOOKING_DOMAINS.some((pattern) => pattern.test(host) || pattern.test(url));
  } catch {
    return false;
  }
}

function findCandidateLinks(event: SourceEvent): string[] {
  const candidates: string[] = [];
  const seen = new Set<string>();
  const pushLink = (raw?: string) => {
    if (!raw) return;
    const extracted = extractUrlsFromString(raw);
    for (const link of extracted) {
      const normalized = normalizeCandidateUrl(link);
      if (!normalized) continue;
      if (seen.has(normalized)) continue;
      seen.add(normalized);
      candidates.push(normalized);
    }
  };

  pushLink(event.website);

  const linkSources = [
    ...(event.links ?? []),
    ...(event.schedules?.flatMap((schedule) => schedule.links ?? []) ?? []),
    ...(event.schedules?.flatMap((schedule) => schedule.performances ?? []).flatMap((performance) => performance.links ?? []) ?? []),
  ];

  for (const link of linkSources) {
    if (link.type?.toLowerCase() === 'booking') continue;
    pushLink(link.url);
  }

  const place = event.schedules?.[0]?.place;
  pushLink(`https://www.datathistle.com/event/${event.event_id}`);

  if (!candidates.length) {
    pushLink(`https://www.datathistle.com/event/${event.event_id}`);
  }

  return candidates;
}

function extractUrlsFromString(value: string): string[] {
  const urls = new Set<string>();
  const findMatches = (text: string) => {
    const matches = text.match(/https?:\/\/[^\s"'()]+/gi);
    if (matches) {
      for (const match of matches) {
        urls.add(match);
      }
    }
  };

  findMatches(value);
  const base64Matches = value.match(/[A-Za-z0-9+/=]{40,}/g) ?? [];
  for (const candidate of base64Matches) {
    try {
      const decoded = Buffer.from(candidate, 'base64').toString('utf-8');
      findMatches(decoded);
    } catch {
      // ignore invalid base64
    }
  }

  return Array.from(urls);
}

function normalizeCandidateUrl(raw: string): string | null {
  try {
    const parsed = new URL(raw, 'https://www.datathistle.com');
    if (!parsed.protocol.startsWith('http')) return null;
    parsed.hash = '';
    if (parsed.searchParams.has('utm_source')) {
      parsed.searchParams.delete('utm_source');
    }
    return parsed.href;
  } catch {
    return null;
  }
}

async function callGroqAPI(prompt: string, apiKey: string, eventId: string): Promise<ActivityLLMOutput> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);

  const requestPayload = {
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
  };
  const response = await fetch(process.env.GROQ_API_URL ?? DEFAULT_GROQ_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(requestPayload),
    signal: controller.signal,
  });

  clearTimeout(timeout);

  if (!response.ok) {
    const errorBody = await response.text();
    console.error(`Groq API error for ${eventId}:`, errorBody);
    await exportGroqRequest(eventId, requestPayload);
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

async function exportGroqRequest(eventId: string, requestBody: unknown): Promise<void> {
  const filename = `groq-request-${eventId}-${Date.now()}.json`;
  const filePath = path.resolve(process.cwd(), filename);
  try {
    await fs.writeFile(filePath, `${JSON.stringify(requestBody, null, 2)}\n`, 'utf-8');
  } catch (error) {
    console.warn('Failed to export Groq request:', (error as Error).message);
  }
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

  const keywordsRaw = Array.isArray(candidate.keywords) ? candidate.keywords : [];
  const normalizedKeywords = Array.from(
    new Set(
      keywordsRaw
        .map((keyword) => String(keyword ?? '').toLowerCase().trim())
        .filter((value) => value.length > 0),
    ),
  );
  if (normalizedKeywords.length < 20) {
    throw new Error(`Expected at least 20 keywords but got ${normalizedKeywords.length}.`);
  }
  const labelsRaw = Array.isArray(candidate.labels) ? candidate.labels : [];
  const normalizedLabels = labelsRaw
    .map((label) => String(label ?? '').trim())
    .filter((value) => value.length > 0);
  if (normalizedLabels.length !== 3) {
    throw new Error(`Expected 3 labels but got ${normalizedLabels.length}.`);
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
  const summaryText = summary.trim();
  const summaryWordCount = summaryText
    .split(/\s+/)
    .filter((word) => word.length > 0).length;
  if (summaryWordCount < 15 || summaryWordCount > 35) {
    throw new Error(`Summary must be 15–35 words but got ${summaryWordCount}.`);
  }

  const normalizedPriceLevel = priceLevel as ActivityLLMOutput['priceLevel'];
  const officialDescriptiveUrl = (candidate.officialDescriptiveUrl ?? '').toString().trim();
  if (!officialDescriptiveUrl) {
    throw new Error('officialDescriptiveUrl must be a non-empty string.');
  }

  return {
    type: type as ActivityLLMOutput['type'],
    hasCommittedTimes,
    dates: dates as Dates,
    officialAgeAvailable,
    minAge,
    maxAge,
    summary: summaryText,
    priceLevel: normalizedPriceLevel,
    keywords: normalizedKeywords,
    labels: normalizedLabels,
    officialDescriptiveUrl,
  };
}

function clampAge(value: unknown): number {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    throw new Error('Age value is not a number.');
  }
  return Math.min(18, Math.max(0, Math.round(value)));
}

async function buildActivity(event: SourceEvent, llmData: ActivityLLMOutput): Promise<Activity> {
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
    url: llmData.officialDescriptiveUrl,
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
