import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import ts from 'typescript';
import { chromium } from 'playwright';
import type { Browser, Route as PlaywrightRoute, Response } from 'playwright-core';
import type { Activity, Dates } from '../../london-kids-p1/packages/shared/src/activity.js';
import type { LondonArea } from '../../london-kids-p1/packages/shared/src/areas.js';

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

const projectRoot = path.resolve(process.cwd());
const sharedSrcDir = path.resolve(projectRoot, '../london-kids-p1/packages/shared/src');
const sharedRequire = createRequire(path.join(sharedSrcDir, 'activity.ts'));
const zodUrl = pathToFileURL(sharedRequire.resolve('zod')).href;

const tsCompilerOptions: ts.CompilerOptions = {
  module: ts.ModuleKind.ES2020,
  target: ts.ScriptTarget.ES2020,
  esModuleInterop: true,
};

let activitySchema: typeof import('../../london-kids-p1/packages/shared/src/activity.js').activitySchema;
let mapAddressToAreas: typeof import('../../london-kids-p1/packages/shared/src/areas.js').mapAddressToAreas;

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

const TASK_NAMES = [
  '1. Identify the official webpage URL',
  '2. Scrape the official webpage',
  '3. Send scraped content + raw info to LLM',
  '4. Receive LLM response',
  '5. Combine LLM response and raw data into final activity',
] as const;

type TaskStatusValue = 'not started' | 'success' | 'fail';

interface TaskStatus {
  name: typeof TASK_NAMES[number];
  status: TaskStatusValue;
  message?: string;
}

function createTaskStatuses(): TaskStatus[] {
  return TASK_NAMES.map((name) => ({ name, status: 'not started' }));
}

function setTaskStatus(statuses: TaskStatus[], index: number, status: TaskStatusValue, message?: string): void {
  statuses[index] = { ...statuses[index], status, message };
}

function logTaskStatuses(eventId: string, statuses: TaskStatus[]): void {
  const summary = statuses
    .map((task) => {
      const base = `${task.name}: ${task.status}`;
      return task.message ? `${base} (${task.message})` : base;
    })
    .join(' | ');
  console.log(`[${eventId}] Status overview → ${summary}`);
}

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

  const adjustedOutput =
    name === 'activity'
      ? outputText.replace(/\.\/areas\.js/g, './areas.mjs')
      : outputText;

  const outPath = path.join(compiledDir, `${name}.mjs`);
  await fs.writeFile(outPath, adjustedOutput, 'utf-8');
  return import(pathToFileURL(outPath).href);
}
const SCRAPE_TIMEOUT_MS = 15_000;
const MAX_SCRAPE_RETRIES = 2;

function cleanText(text: string): string {
  const collapsed = text
    .replace(/[\t\r]/g, ' ')
    .replace(/\s*\n\s*/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/ {2,}/g, ' ')
    .trim();
  return collapsed;
}

function parseJsonLd(jsonLdStrings: string[]): Record<string, unknown>[] {
  const parsed: Record<string, unknown>[] = [];
  for (const raw of jsonLdStrings) {
    try {
      const loaded = JSON.parse(raw);
      const entries = Array.isArray(loaded) ? loaded : [loaded];
      for (const entry of entries) {
        if (entry && typeof entry === 'object') {
          parsed.push(entry as Record<string, string>);
        }
      }
    } catch {
      // ignore malformed JSON-LD
    }
  }
  return parsed;
}

async function getOfficialUrlAndContent(inputUrl: string): Promise<PageScrapeResult | null> {
  for (let attempt = 1; attempt <= MAX_SCRAPE_RETRIES; attempt += 1) {
    let browser: Browser | null = null;
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
        if (['image', 'stylesheet', 'font', 'media'].includes(rt)) {
          return route.abort();
        }
        return route.continue();
      });

      const response = await page.goto(inputUrl, {
        waitUntil: 'domcontentloaded',
        timeout: SCRAPE_TIMEOUT_MS,
      });
      await page.waitForLoadState('networkidle', { timeout: SCRAPE_TIMEOUT_MS }).catch(() => null);
      await page.waitForTimeout(1000);
      const bodyHeight = await page.evaluate(() => document.body.scrollHeight);
      if (bodyHeight > 5000) {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await page.waitForTimeout(500);
      }

      const finalUrl = response?.url() ?? inputUrl;
      const scraped = await page.evaluate(() => {
        const removeSelectors =
          'script, style, nav, header, footer, .cookie, .popup, .advert, .sidebar';
        document.querySelectorAll(removeSelectors).forEach((el) => el.remove());

        const gather = (selectors: string[]): string => {
          for (const selector of selectors) {
            const element = document.querySelector(selector) as HTMLElement | null;
            if (element) {
              const value = element.innerText.trim();
              if (value) return value;
            }
          }
          return '';
        };

        const getMeta = (name: string, attr = 'name'): string => {
          const selector = attr === 'property' ? `meta[property="${name}"]` : `meta[name="${name}"]`;
          const el = document.querySelector(selector) as HTMLMetaElement | null;
          return el?.content?.trim() ?? '';
        };

        const jsonLdScripts = Array.from(
          document.querySelectorAll('script[type="application/ld+json"]'),
        )
          .map((node) => node.textContent?.trim())
          .filter((value): value is string => Boolean(value));
        const descriptionSelectors = ['article', '.event-description', '.description', '.content', 'main p'];
        const priceSelectors = ['.price', '.ticket-price', '[itemprop="price"]', '.cost'];
        const ageSelectors = ['.age', '.age-restriction', '.suitable-for'];
        const hoursSelectors = ['.opening-hours', 'table', '.hours'];
        const addressSelectors = ['.address', '[itemprop="address"]', 'footer address'];

        return {
          title: document.title ?? '',
          rawText: document.body?.innerText ?? '',
          jsonLdScripts,
          meta: {
            description: getMeta('description') || getMeta('og:description', 'property'),
            price: getMeta('og:price:amount', 'property'),
            age: getMeta('og:age', 'property'),
            address: getMeta('og:street-address', 'property'),
          },
          selectors: {
            description: gather(descriptionSelectors),
            price: gather(priceSelectors),
            age: gather(ageSelectors),
            hours: gather(hoursSelectors),
            address: gather(addressSelectors),
          },
        };
      });

      const jsonLdEntries = parseJsonLd(scraped.jsonLdScripts);
      const structured: PageScrapeResult['structured'] = {};

      const assignIfEmpty = (field: keyof PageScrapeResult['structured'], value?: string) => {
        if (!value) return;
        if (!structured[field]) {
          structured[field] = cleanText(value);
        }
      };

      for (const entry of jsonLdEntries) {
        const data = entry as Record<string, any>;
        assignIfEmpty('description', data.description ?? data.name);
        if (data.price) {
          assignIfEmpty('priceText', String(data.price));
        }
        if (data.offers) {
          const offers = Array.isArray(data.offers) ? data.offers[0] : data.offers;
          if (offers?.price) {
            assignIfEmpty('priceText', String(offers.price));
          }
        }
        assignIfEmpty('ageText', String(data.ageRestriction ?? data.suitableFor ?? ''));
        if (data.openingHours) {
          const hours = Array.isArray(data.openingHours) ? data.openingHours.join(', ') : data.openingHours;
          assignIfEmpty('openingHoursText', String(hours));
        }
        if (data.openingHoursSpecification) {
          const spec = Array.isArray(data.openingHoursSpecification)
            ? data.openingHoursSpecification
                .map((item) => (item?.opens ? `${item.opens}–${item?.closes ?? ''}` : ''))
                .filter(Boolean)
                .join(', ')
            : data.openingHoursSpecification.opens
            ? `${data.openingHoursSpecification.opens}–${data.openingHoursSpecification.closes ?? ''}`
            : '';
          assignIfEmpty('openingHoursText', spec);
        }
        if (data.address) {
          const addressValue =
            typeof data.address === 'string'
              ? data.address
              : data.address?.streetAddress ?? data.address?.addressLocality ?? '';
          assignIfEmpty('addressText', addressValue);
        }
      }

      assignIfEmpty('description', scraped.selectors.description || scraped.meta.description);
      assignIfEmpty('priceText', scraped.selectors.price || scraped.meta.price);
      assignIfEmpty('ageText', scraped.selectors.age || scraped.meta.age);
      assignIfEmpty('openingHoursText', scraped.selectors.hours);
      assignIfEmpty('addressText', scraped.selectors.address || scraped.meta.address);

      const textCandidates = [
        structured.description,
        scraped.meta.description,
        scraped.selectors.description,
        scraped.rawText,
      ].filter(Boolean);
      const combinedText = cleanText(textCandidates.join('\n\n')).slice(0, 20_000);
      if (!combinedText) {
        console.warn(`Scrape returned empty text for ${inputUrl}`);
        continue;
      }

      const finalTitle = cleanText(scraped.title) || '';
      return {
        url: finalUrl,
        title: finalTitle,
        text: combinedText,
        structured,
      };
    } catch (error) {
      console.warn(`Scrape attempt ${attempt} failed for ${inputUrl}:`, (error as Error).message);
    } finally {
      if (browser) {
        await browser.close();
      }
    }
  }
  return null;
}

const OFFICIAL_SIGNAL_REGEX = /official|book tickets|opening times|visit us|family|kids|age \d|merlin|©/i;
const AGGREGATOR_REGEX = /ticketmaster|eventbrite|timeout\.com|visitlondoncom|datathistle|seetickets|axs|ticketweb|skiddle/i;

function guessOfficialUrls(event: SourceEvent): string[] {
  const name = event.name.toLowerCase().replace(/['’]/g, '').trim();
  const slug = name.replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  const noHyphen = slug.replace(/-/g, '');
  const withLondon = name.includes('london') ? '' : '-london';

  const bases = [slug, noHyphen, slug + withLondon, noHyphen + withLondon];
  const tlds = ['.com', '.co.uk', '.org.uk', '.london'];
  const urls = new Set<string>();

  for (const base of bases) {
    for (const tld of tlds) {
      if (!base) continue;
      urls.add(`https://${base}${tld}`);
      urls.add(`https://www.${base}${tld}`);
    }
  }
  return Array.from(urls);
}

async function verifyCandidateUrl(url: string, activityName: string): Promise<boolean> {
  try {
    const head = await fetch(url, { method: 'HEAD', redirect: 'follow', signal: AbortSignal.timeout(8000) });
    if (!head.ok || !head.headers.get('content-type')?.includes('text/html')) return false;

    const res = await fetch(url, { signal: AbortSignal.timeout(12000) });
    if (!res.ok) return false;
    const buffer = await res.arrayBuffer();
    if (buffer.byteLength < 5000) return false;

    const html = new TextDecoder().decode(buffer.slice(0, 15000)).toLowerCase();
    const name = activityName.toLowerCase();
    const hasName = html.includes(name) || html.includes(name.replace(/['’]/g, ''));
    const hasOfficialSignal = OFFICIAL_SIGNAL_REGEX.test(html);
    const noAggregator = !AGGREGATOR_REGEX.test(html);

    return hasName && hasOfficialSignal && noAggregator;
  } catch {
    return false;
  }
}

async function resolveOfficialUrl(event: SourceEvent): Promise<string | null> {
  const activityName = event.name.trim();
  const candidates: string[] = [...guessOfficialUrls(event)];

  const priorityLinks = await gatherPriorityUrls(event);
  for (const link of priorityLinks) {
    if (!/ticketmaster|eventbrite|axs|seetickets|datathistle.*details/i.test(link)) {
      candidates.push(link);
    }
  }

  const uniqueCandidates = Array.from(new Set(candidates));
  const results = await mapWithConcurrency(
    uniqueCandidates,
    async (url) => ({ url, valid: await verifyCandidateUrl(url, activityName) }),
    10,
  );

  const winner = results.find((result) => result.valid);
  if (winner) return winner.url;

  const ddgoUrl = await searchOfficialUrl(event);
  if (ddgoUrl && (await verifyCandidateUrl(ddgoUrl, activityName))) {
    return ddgoUrl;
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

async function gatherPriorityUrls(event: SourceEvent): Promise<string[]> {
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
  for (const candidate of findCandidateLinks(event)) {
    add(candidate);
  }
  add(`https://www.datathistle.com/event/${event.event_id}`);
  return prioritized;
}

async function mapWithConcurrency<T, U>(
  items: T[],
  mapper: (item: T) => Promise<U>,
  concurrency: number,
): Promise<U[]> {
  const results: U[] = [];
  const queue = [...items];
  const workers = Array(Math.min(concurrency, queue.length))
    .fill(null)
    .map(async () => {
      while (queue.length) {
        const item = queue.shift();
        if (!item) break;
        results.push(await mapper(item));
      }
    });
  await Promise.all(workers);
  return results;
}

function describeRawSchedules(event: SourceEvent): string {
  const lines: string[] = [];
  const performances = event.schedules?.flatMap((schedule) => schedule.performances ?? []) ?? [];
  if (!performances.length) {
    return 'No raw schedule details provided.';
  }
  performances.forEach((performance, index) => {
    const timestamp = performance.ts;
    let dateLabel = 'unknown date';
    let timeLabel = 'unknown time';
    if (timestamp) {
      const [datePart, timePart] = timestamp.split('T');
      dateLabel = datePart ?? 'unknown date';
      timeLabel = timePart?.slice(0, 5) ?? 'unknown time';
    }
    const duration = performance.duration ?? 'unknown duration';
    const timeNote = performance.time_unknown ? ' (time unknown)' : '';
    lines.push(
      `Performance ${index + 1}: ${dateLabel} at ${timeLabel}${timeNote} – duration ${duration} mins`,
    );
  });
  return lines.join('\n');
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
  title: string;
  text: string;
  structured: {
    description?: string;
    priceText?: string;
    ageText?: string;
    openingHoursText?: string;
    addressText?: string;
  };
}

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
    openingHours: {},
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
    const statuses = createTaskStatuses();
    const logStatuses = () => logTaskStatuses(event.event_id, statuses);

    const place = event.schedules?.[0]?.place;
    if (!place) {
      setTaskStatus(statuses, 0, 'fail', 'missing location data');
      logStatuses();
      skipped.push({ id: event.event_id, reason: 'missing location data' });
      continue;
    }

    const candidateLinks = findCandidateLinks(event);
    const officialUrl = await resolveOfficialUrl(event);
    if (!officialUrl) {
      setTaskStatus(statuses, 0, 'fail', 'no official descriptive URL found');
      logStatuses();
      skipped.push({ id: event.event_id, reason: 'no official descriptive URL found' });
      continue;
    }
    setTaskStatus(statuses, 0, 'success', 'URL identified');
    console.log(`[${event.event_id}] Step 1: identified official URL ${officialUrl}`);

    const officialPage = await getOfficialUrlAndContent(officialUrl);
    if (!officialPage) {
      setTaskStatus(statuses, 1, 'fail', 'official page scrape failed');
      console.log(`[${event.event_id}] Step 2: scraping failed for ${officialUrl}`);
      console.warn(`Scrape failed for ${event.event_id}: ${officialUrl}`);
      logStatuses();
      skipped.push({ id: event.event_id, reason: 'official page scrape failed' });
      continue;
    }
    setTaskStatus(statuses, 1, 'success', 'official page scraped');
    console.log(`[${event.event_id}] Step 2: scraped official content (${officialPage.text.length} chars)`);

    const missingAttributes = [
      'type',
      'dates',
      'summary',
      'priceLevel',
      'officialAgeAvailable',
      'minAge',
      'maxAge',
      'keywords',
      'labels',
    ];
    const rawScheduleSummary = describeRawSchedules(event);
    console.log(`[${event.event_id}] Step 3: sending scraped content + raw data to Groq`);
    const llmData = await fetchLLMDataWithRetries(
      event,
      officialPage,
      candidateLinks,
      missingAttributes,
      rawScheduleSummary,
      groqApiKey,
    );
    setTaskStatus(statuses, 2, 'success', 'Groq request finished');

    console.log(
      `[${event.event_id}] Step 4: received LLM response (type=${llmData.type}, hasCommittedTimes=${llmData.hasCommittedTimes})`,
    );
    if (!llmData.hasCommittedTimes) {
      setTaskStatus(statuses, 3, 'fail', 'LLM indicated no committed times');
      logStatuses();
      skipped.push({ id: event.event_id, reason: 'no committed date/time (self-guided/anytime)' });
      continue;
    }
    setTaskStatus(statuses, 3, 'success', 'LLM response valid');

    try {
      const activity = await buildActivity(event, llmData, officialUrl);
      activitySchema.parse(activity);
      activities.push(activity);
      setTaskStatus(statuses, 4, 'success', 'final activity constructed');
      console.log(`[${event.event_id}] Step 5: combined LLM response and raw data into final activity`);
      logStatuses();
    } catch (error) {
      setTaskStatus(statuses, 4, 'fail', 'validation failed');
      logStatuses();
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
  missingAttributes: string[],
  rawScheduleSummary: string,
  apiKey: string,
): Promise<ActivityLLMOutput> {
  const prompt = buildPrompt(event, officialPage, candidateLinks, missingAttributes, rawScheduleSummary);
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      const llmOutput = await callGroqAPI(prompt, apiKey, event.event_id, attempt);
      llmOutput.officialDescriptiveUrl = officialPage.url;
      return llmOutput;
    } catch (error) {
      console.warn(`Groq attempt ${attempt} for ${event.event_id} failed:`, (error as Error).message);
      console.log(`[${event.event_id}] Step 3: Groq attempt ${attempt} failed (${(error as Error).message})`);
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS);
      }
    }
  }
  console.warn(`Falling back for ${event.event_id} after ${MAX_RETRIES} Groq attempts.`);
  await exportGroqInteraction(
    event.event_id,
    { fallback: true, officialUrl: officialPage.url },
    null,
    true,
    MAX_RETRIES + 1,
  );
  return fallbackLLMOutput(event, officialPage.url);
}

function buildPrompt(
  event: SourceEvent,
  officialPage: PageScrapeResult,
  candidateLinks: string[],
  missingAttributes: string[],
  rawScheduleSummary: string,
): string {
  const tags = event.tags?.join(', ') ?? 'None';
  const rawDescriptions =
    event.descriptions
      ?.map((description) => `${description.type ?? 'description'}: ${description.description}`)
      .join('\n') ?? 'None';
  const candidateText = candidateLinks.length ? candidateLinks.join(', ') : 'None';
  const missingText = missingAttributes.length ? missingAttributes.join(', ') : 'None';
  const structured = officialPage.structured;
  const structuredLines = [
    `Scraped description: ${structured.description ?? 'None'}`,
    `Scraped price: ${structured.priceText ?? 'None'}`,
    `Scraped age guidance: ${structured.ageText ?? 'None'}`,
    `Scraped opening hours: ${structured.openingHoursText ?? 'None'}`,
    `Scraped address detail: ${structured.addressText ?? 'None'}`,
  ];
  const blocks = [
    `Activity name: ${event.name}`,
    `Tags: ${tags}`,
    `Raw descriptions:\n${rawDescriptions}`,
    `Raw schedule data:\n${rawScheduleSummary || 'None'}`,
    `Official URL: ${officialPage.url}`,
    `Scraped title: ${officialPage.title || 'Unknown'}`,
    `Scraped structured data:\n${structuredLines.join('\n')}`,
    `Scraped website content:\n${officialPage.text}`,
    `Missing schema attributes: ${missingText}`,
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

async function callGroqAPI(
  prompt: string,
  apiKey: string,
  eventId: string,
  attempt: number,
): Promise<ActivityLLMOutput> {
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
  await exportGroqInteraction(
    eventId,
    requestPayload,
    undefined,
    attempt > 1,
    attempt,
  );
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
    await exportGroqInteraction(
      eventId,
      requestPayload,
      { status: response.status, body: errorBody },
      attempt > 1,
      attempt,
    );
    throw new Error(`Groq API responded with ${response.status}: ${errorBody}`);
  }

  const responseText = await response.text();
  let payload;
  try {
    payload = JSON.parse(responseText);
  } catch (error) {
    await exportGroqInteraction(
      eventId,
      requestPayload,
      { status: response.status, body: responseText },
      attempt > 1,
      attempt,
    );
    throw error;
  }
  const content = payload.choices?.[0]?.message?.content;

  await exportGroqInteraction(
    eventId,
    requestPayload,
    { status: response.status, body: payload },
    attempt > 1,
    attempt,
  );

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

  try {
    return normalizeLLMOutput(parsed);
  } catch (error) {
    await exportGroqInteraction(
      eventId,
      requestPayload,
      { status: response.status, body: payload },
      attempt > 1,
      attempt,
    );
    throw error;
  }
}

async function exportGroqInteraction(
  eventId: string,
  payload: unknown,
  response: unknown,
  isRetry = false,
  attempt = 1,
): Promise<void> {
  const dir = path.resolve(process.cwd(), 'groq-logs');
  try {
    await fs.mkdir(dir, { recursive: true });
    const filename = `groq-${eventId}--attempt${attempt}--${Date.now()}.json`;
    const filePath = path.join(dir, filename);
    const entry = {
      eventId,
      timestamp: new Date().toISOString(),
      attempt,
      isRetry,
      request: payload,
      response: response ?? null,
      success: response != null,
    };
    await fs.writeFile(filePath, `${JSON.stringify(entry, null, 2)}\n`, 'utf-8');
  } catch (error) {
    console.warn('Failed to export Groq interaction:', (error as Error).message);
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

async function buildActivity(
  event: SourceEvent,
  llmData: ActivityLLMOutput,
  officialUrl: string,
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
