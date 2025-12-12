import { chromium } from 'playwright';
import type { Browser, LaunchOptions, Page, Response, Route as PlaywrightRoute } from 'playwright-core';
import { existsSync } from 'node:fs';
import {
  cleanText,
  extractCalendarExceptionsFromText,
  extractDatesAndTimesFromPage,
  extractOpeningHours,
  extractStructuredDatesFromPage,
  hasActualTimeInfo,
  classifyDateTimeFormat,
  extractRawDateTimeInstancesFromPage,
  parseAndClassifyDates,
  normalizeCalendarApiPayload,
  logDebug,
  logWarn,
  isUrlValidAndHtml,
  quickPreScoreUrl,
  generateSmartHoursGuesses,
  parseTimeTo24h,
  ensureIsoDate,
} from './utils.js';
import { normalizeActivityForHostname } from './url-resolver.js';
import { DAY_LABELS } from './types.js';
import type { DayLabel, JsonLdEvent, JsonLdHoursResult, PageScrapeResult, RawDateTimeInstance } from './types.js';
import type {
  Dates,
  Exception as SharedException,
  Location as SharedLocation,
  PlaceDates,
} from '../../london-kids-p1/packages/shared/src/activity.js';

function scoreUrlForTimeInfo(page: PageScrapeResult): number {
  let score = 0;
  const text = page.text.toLowerCase();
  const title = page.title.toLowerCase();
  const url = page.url.toLowerCase();

  const timePatterns = /(\d{1,2}(:\d{2})?\s*(am|pm))/gi;
  const datePatterns = /(\d{1,2}(st|nd|rd|th)?\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)?\s*\d{4}?)/gi;
  const dayPatterns = /(mon|tue|wed|thu|fri|sat|sun)(day)?/gi;
  const rangePatterns = /(to|-|–|—|from|until)/gi;
  score += (text.match(timePatterns)?.length || 0) * 20;
  score += (text.match(datePatterns)?.length || 0) * 15;
  score += (text.match(dayPatterns)?.length || 0) * 10;
  score += (text.match(rangePatterns)?.length || 0) * 5;
  if (score > 400) score = 400;

  const keywords = ['opening hours', 'schedule', 'times', 'calendar', 'operating', 'event dates', 'start time', 'end time'];
  keywords.forEach((kw) => {
    if (text.includes(kw)) score += 30;
    if (title.includes(kw)) score += 20;
  });
  if (score > 700) score = 700;

  if (page.structured.openingHoursText || page.structured.extractedHours) score += 100;
  if (text.includes('<table') && text.match(dayPatterns)) score += 50;
  if (text.match(/class\s*=\s*["']calendar["']/i)) score += 50;

  const urlBoosts = ['hours', 'times', 'schedule', 'calendar', 'dates'];
  urlBoosts.forEach((kw) => {
    if (url.includes(kw)) score += 20;
  });
  score += /(opening hours|opening times|visit us|plan your visit).*hours/i.test(title) ? 80 : 0;
  if (text.includes('ticket') || text.includes('price') || text.includes('book now') || text.includes('£')) {
    score -= 120;
  }
  score -= (url.split('/').length - 3) * 5;

  const extractable = hasActualTimeInfo(page);
  if (extractable) {
    score *= 2.5;
  } else {
    score *= 0.4;
  }

  const finalScore = Math.min(Math.max(score, 0), 1500);
  logDebug(
    `[HOURS SCORE] ${page.url} → raw:${score.toFixed(0)} → final:${finalScore.toFixed(0)} (extractable=${extractable})`,
    page.url,
  );
  return finalScore;
}

const SCRAPE_TIMEOUT_MS = 15_000;
const MAX_SCRAPE_RETRIES = 2;

interface JsonLdEntry {
  description?: string;
  name?: string;
  price?: string | number;
  offers?: { price?: string | number } | Array<{ price?: string | number }>;
  ageRestriction?: string;
  suitableFor?: string;
  openingHours?: string[] | string;
  openingHoursSpecification?: any;
  address?: any;
}

export function parseJsonLd(jsonLdStrings: string[]): Record<string, unknown>[] {
  const parsed: Record<string, unknown>[] = [];
  for (const raw of jsonLdStrings) {
    try {
      const loaded = JSON.parse(raw);
      const entries = Array.isArray(loaded) ? loaded : [loaded];
      for (const entry of entries) {
        if (entry && typeof entry === 'object') {
          parsed.push(entry as Record<string, unknown>);
        }
      }
    } catch {
      // ignore malformed JSON-LD
    }
  }
  return parsed;
}

function parseJsonLdHours(specEntries: any[]): JsonLdHoursResult | null {
  if (!specEntries?.length) return null;

  const hours: Partial<Record<DayLabel, { open: string; close: string } | { closed: true }>> = {};
  const exceptions = new Map<string, SharedException>();

  const dayAlias: Record<string, DayLabel> = {
    sun: 'Sun',
    sunday: 'Sun',
    mon: 'Mon',
    monday: 'Mon',
    tue: 'Tue',
    tuesday: 'Tue',
    wed: 'Wed',
    wednesday: 'Wed',
    thu: 'Thu',
    thursday: 'Thu',
    fri: 'Fri',
    friday: 'Fri',
    sat: 'Sat',
    saturday: 'Sat',
  };

  const normalizeDay = (token: string): DayLabel | null => {
    if (!token) return null;
    const cleaned = token
      .replace(/https?:\/\/schema\.org\//i, '')
      .replace(/[^a-z]/gi, '')
      .toLowerCase();
    return dayAlias[cleaned] ?? null;
  };

  const expandDayRange = (start: DayLabel, end: DayLabel): DayLabel[] => {
    const order = DAY_LABELS;
    const startIdx = order.indexOf(start);
    const endIdx = order.indexOf(end);
    if (startIdx === -1 || endIdx === -1) return [start];
    const days: DayLabel[] = [];
    let index = startIdx;
    do {
      days.push(order[index]);
      if (index === endIdx) break;
      index = (index + 1) % order.length;
    } while (days.length < order.length);
    return days;
  };

  const parseDays = (value: unknown): DayLabel[] => {
    const collected: DayLabel[] = [];
    const capture = (day: DayLabel | null) => {
      if (day && !collected.includes(day)) {
        collected.push(day);
      }
    };
    const processToken = (token: string) => {
      if (!token) return;
      if (token.includes('-')) {
        const [start, end] = token.split('-').map((segment) => segment.trim());
        const startLabel = normalizeDay(start);
        const endLabel = normalizeDay(end);
        if (startLabel && endLabel) {
          expandDayRange(startLabel, endLabel).forEach(capture);
          return;
        }
      }
      capture(normalizeDay(token));
    };
    if (Array.isArray(value)) {
      for (const item of value) {
        if (typeof item === 'string') {
          item.split(/[,;]+/).forEach((token) => processToken(token.trim()));
        } else if (item && typeof item === 'object') {
          const text = (item as Record<string, unknown>).name ?? (item as Record<string, unknown>)['@id'];
          if (typeof text === 'string') {
            text.split(/[,;]+/).forEach((token) => processToken(token.trim()));
          }
        }
      }
      return collected;
    }
    if (typeof value === 'string') {
      value.split(/[,;]+/).forEach((token) => processToken(token.trim()));
      return collected;
    }
    if (value && typeof value === 'object') {
      const text = (value as Record<string, unknown>).name ?? (value as Record<string, unknown>)['@id'];
      if (typeof text === 'string') {
        text.split(/[,;]+/).forEach((token) => processToken(token.trim()));
      }
    }
    return collected;
  };

  const parseLegacy = (value: string) => {
    const entries: Array<{ days: DayLabel[]; open?: string; close?: string; closed?: boolean }> = [];
    const pattern = /([A-Za-z]{2,9}(?:-[A-Za-z]{2,9})?)\s+(\d{1,2}(?::\d{2})?(?:am|pm)?)[\s–—-]+(\d{1,2}(?::\d{2})?(?:am|pm)?)/gi;
    const segments = value.split(/[;/]/);
    for (const segment of segments) {
      let match: RegExpExecArray | null;
      while ((match = pattern.exec(segment.trim()))) {
        const daysPart = match[1];
        const open = parseTimeTo24h(match[2]);
        const close = parseTimeTo24h(match[3]);
        entries.push({
          days: parseDays(daysPart),
          open: open ?? undefined,
          close: close ?? undefined,
          closed: !open || !close || open === close,
        });
      }
    }
    return entries;
  };

  const addDayHours = (days: DayLabel[], open?: string, close?: string, closedOverride = false) => {
    for (const day of days) {
      if (closedOverride || !open || !close) {
        hours[day] = { closed: true };
      } else {
        hours[day] = { open, close };
      }
    }
  };

  const addException = (entry: SharedException) => {
    if (!entry.date) return;
    exceptions.set(entry.date, entry);
  };

  const applySeasonal = (
    from: string | undefined,
    through: string | undefined,
    days: DayLabel[],
    closed: boolean,
    open?: string,
    close?: string,
  ) => {
    const startIso = ensureIsoDate(from ?? '');
    const endIso = ensureIsoDate(through ?? '');
    if (!startIso || !endIso) return;
    const startDate = new Date(`${startIso}T00:00:00`);
    const endDate = new Date(`${endIso}T00:00:00`);
    if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) return;
    const cursor = new Date(startDate);
    while (cursor <= endDate) {
      const dow = DAY_LABELS[cursor.getDay()];
      if (days.length && !days.includes(dow)) {
        cursor.setDate(cursor.getDate() + 1);
        continue;
      }
      const isoDate = cursor.toISOString().split('T')[0];
      if (closed) {
        addException({ status: 'closed', date: isoDate });
      } else if (open && close) {
        addException({ status: 'open', date: isoDate, open, close });
      }
      cursor.setDate(cursor.getDate() + 1);
    }
  };

  const resolveTimeValue = (...values: unknown[]): string | undefined => {
    for (const value of values) {
      if (typeof value === 'string' && value.trim()) {
        return value;
      }
      if (typeof value === 'number') {
        return String(value);
      }
    }
    return undefined;
  };

  const processEntry = (entry: Record<string, unknown>) => {
    const legacy = typeof entry.openingHours === 'string' ? entry.openingHours : undefined;
    if (legacy) {
      parseLegacy(legacy).forEach((item) => {
        if (item.days.length) {
          addDayHours(item.days, item.open, item.close, Boolean(item.closed));
        }
      });
    }
    const days = parseDays(entry.dayOfWeek ?? entry.day ?? entry.days ?? entry['validDay']);
    const open = parseTimeTo24h(
      resolveTimeValue(
        entry.opens,
        entry.open,
        entry['startTime'],
        entry['start'],
        entry['openingTime'],
        entry['availableAtOrFrom'],
      ),
    );
    const close = parseTimeTo24h(
      resolveTimeValue(
        entry.closes,
        entry.close,
        entry['endTime'],
        entry['end'],
        entry['closingTime'],
        entry['availableThrough'],
      ),
    );
    const isClosed = !open || !close || open === close;
    if (days.length) {
      addDayHours(days, open ?? undefined, close ?? undefined, isClosed);
      applySeasonal(
        entry.validFrom as string | undefined,
        entry.validThrough as string | undefined,
        days,
        isClosed,
        open ?? undefined,
        close ?? undefined,
      );
    }
  };

  for (const spec of specEntries) {
    if (!spec) continue;
    if (typeof spec === 'string') {
      parseLegacy(spec).forEach((item) => {
        if (item.days.length) {
          addDayHours(item.days, item.open, item.close, Boolean(item.closed));
        }
      });
      continue;
    }
    if (Array.isArray(spec)) {
      spec.forEach((item) => {
        if (typeof item === 'string') {
          parseLegacy(item).forEach((entry) => {
            if (entry.days.length) {
              addDayHours(entry.days, entry.open, entry.close, Boolean(entry.closed));
            }
          });
        } else if (item && typeof item === 'object') {
          processEntry(item as Record<string, unknown>);
        }
      });
      continue;
    }
    if (typeof spec === 'object') {
      processEntry(spec as Record<string, unknown>);
    }
  }

  const finalHours = Object.keys(hours).length ? (hours as Record<DayLabel, { open: string; close: string } | { closed: true }>) : {};
  const finalExceptions = Array.from(exceptions.values());
  if (!Object.keys(finalHours).length && !finalExceptions.length) {
    return null;
  }
  return {
    hours: finalHours,
    exceptions: finalExceptions.length ? finalExceptions : undefined,
  };
}

function normalizeQueueUrl(raw: string): string | null {
  try {
    const parsed = new URL(raw);
    parsed.hash = '';
    parsed.search = '';
    let href = parsed.href;
    if (href.endsWith('/') && href !== `${parsed.origin}/`) {
      href = href.slice(0, -1);
    }
    return href;
  } catch {
    return null;
  }
}

async function loadSitemap(origin: string, depth = 0): Promise<string[]> {
  if (depth > 3) {
    logDebug(`[Sitemap] Max depth reached for ${origin}`, 'scraper');
    return [];
  }
  try {
    const normalizedOrigin = origin.endsWith('/') ? origin : `${origin}/`;
    const res = await fetch(`${normalizedOrigin}sitemap.xml`, { signal: AbortSignal.timeout(8_000) });
    if (!res.ok) return [];
    const text = await res.text();
    const urls: string[] = [];
    let isIndex = false;

    const sitemapMatcher = /<sitemap>.*?<loc>(.*?)<\/loc>.*?<\/sitemap>/gis;
    let match;
    while ((match = sitemapMatcher.exec(text)) !== null) {
      isIndex = true;
      const fullNestedUrl = match[1];
      const nestedBase = fullNestedUrl.replace(/\/sitemap\.xml.*$/i, '');
      const nestedUrls = await loadSitemap(nestedBase, depth + 1);
      urls.push(...nestedUrls);
    }

    if (!isIndex) {
      const locMatcher = /<loc>(.*?)<\/loc>/gis;
      while ((match = locMatcher.exec(text)) !== null) {
        if (match[1]) {
          urls.push(match[1]);
        }
      }
    }

    logDebug(
      `[Sitemap] Loaded ${urls.length} URLs from ${normalizedOrigin}sitemap.xml (index: ${isIndex}, depth: ${depth})`,
      'scraper',
    );
    return urls.slice(0, 200);
  } catch (error) {
    logDebug(`[Sitemap] Failed for ${origin}: ${(error as Error).message}`, 'scraper');
    return [];
  }
}

function pickRandom<T>(items: T[]): T {
  return items[Math.floor(Math.random() * items.length)];
}

export function getRandomViewport(): { width: number; height: number } {
  const width = 1280 + Math.floor(Math.random() * (1920 - 1280 + 1));
  const height = 720 + Math.floor(Math.random() * (1080 - 720 + 1));
  return { width, height };
}

export function getRandomUserAgent(userAgents: string[]): string {
  return pickRandom(userAgents);
}

const DEFAULT_CHROMIUM_PATH = '/Applications/Chromium.app/Contents/MacOS/Chromium';

export function getLaunchOptions(headless = true): LaunchOptions {
  const debugMode = process.env.DEBUG_PLAYWRIGHT === 'true';
  const options: LaunchOptions = {
    headless: debugMode ? false : headless,
  };
  const userPath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  if (userPath && existsSync(userPath)) {
    options.executablePath = userPath;
  } else if (existsSync(DEFAULT_CHROMIUM_PATH)) {
    options.executablePath = DEFAULT_CHROMIUM_PATH;
  }
  return options;
}

const CRAWL_BUDGET_SOFT = 60;
const CRAWL_BUDGET_HARD = 80;
let pagesCrawled = 0;

const RELEVANT_PATH_KEYWORDS = [
  'visit',
  'plan-your-visit',
  'opening-hours',
  'opening-times',
  'hours',
  'times',
  'tickets',
  'ticket-information',
  'prices',
  'pricing',
  'book',
  'plan-visit',
  'visit-us',
  'getting-here',
  'info',
  'practical-information',
  'before-you-visit',
  'visitor-information',
  'about',
  'overview',
  'experience',
  'age',
  'family',
  'children',
  'accessibility',
  'faq',
  'contact',
];
const MAX_CRAWL_PAGES = 20;
const MAX_CRAWL_PAGES_FALLBACK = 60;
const MIN_URL_PRE_SCORE = 180;

interface CrawlTask {
  id: 'hours' | 'age' | 'price' | 'description';
  urlKeywords: string[];
  textKeywords: string[];
  scoreBoost: number;
}

const CRAWL_TASKS: CrawlTask[] = [
  {
    id: 'hours',
    urlKeywords: [
      'opening-hours',
      'opening-times',
      'hours',
      'times',
      'operating-hours',
      'visitor-information',
      'plan-your-visit',
      'before-you-visit',
      'visit-us',
      'opening',
      'daily-hours',
      'schedule',
    ],
    textKeywords: ['open daily', 'monday', 'tuesday', '10:00', '10am', '6:00pm', 'closed on'],
    scoreBoost: 150,
  },
  {
    id: 'age',
    urlKeywords: ['age', 'suitable-for', 'recommended-age', 'family', 'children', 'kids', 'height-restrictions'],
    textKeywords: ['years and over', 'suitable for ages', 'minimum age', 'under', 'accompanied by adult'],
    scoreBoost: 80,
  },
  {
    id: 'price',
    urlKeywords: ['tickets', 'prices', 'pricing', 'admission', 'book-tickets', 'ticket-information'],
    textKeywords: ['adult', 'child', 'family ticket', 'from £', 'concession'],
    scoreBoost: 60,
  },
  {
    id: 'description',
    urlKeywords: ['about', 'overview', 'what-to-expect', 'experience', 'attraction', 'visit'],
    textKeywords: [],
    scoreBoost: 30,
  },
];

interface ScrapeCandidate {
  page: PageScrapeResult;
  status: number | null;
  links: string[];
  semanticHoursLinks?: string[];
}

function cleanedMatch(match: RegExpMatchArray | null): string {
  return match ? cleanText(match[0]) : '';
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function scorePage(page: PageScrapeResult, activityName: string): number {
  const textLower = page.text.toLowerCase();
  const normalizedName = activityName.trim().toLowerCase();
  const regex = normalizedName ? new RegExp(escapeRegExp(normalizedName), 'gi') : null;
  const matches = regex ? textLower.match(regex)?.length ?? 0 : 0;
  let score = matches * 100;
  if (/opening\s*(hours?|times?)|daily\s*hours|open\s*daily|mon.*sun|operating\s*hours/i.test(textLower)) {
    score += 120;
  }
  if (/price|ticket|from £|adult|child|family ticket/i.test(textLower)) score += 50;
  if (/age|recommended age|suitable for|years old|minimum age/i.test(textLower)) score += 60;
  if (/shrek|ogre|far far away|4d ride|mirror maze|flying bus/i.test(textLower)) score += 40;
  if (/book|buy tickets|checkout|basket/i.test(textLower) && score < 100) score -= 70;
  if (textLower.includes('monday') || textLower.includes('sunday') || /am|pm/.test(textLower)) {
    score += 30;
  }
  return score;
}

async function scrapeSinglePage(url: string, userAgents: string[]): Promise<ScrapeCandidate | null> {
  for (let attempt = 1; attempt <= MAX_SCRAPE_RETRIES; attempt += 1) {
    let browser: Browser | null = null;
    try {
      const launchOptions = getLaunchOptions();
      const launchedBrowser = await chromium.launch(launchOptions);
      browser = launchedBrowser;
      const launchViewport = getRandomViewport();
      const context = await launchedBrowser.newContext({
        viewport: launchViewport,
        userAgent: getRandomUserAgent(userAgents),
      });
      const page = await context.newPage();
      await page.route('**/*', (route: PlaywrightRoute) => {
        const rt = route.request().resourceType();
        if (['image', 'stylesheet', 'font', 'media'].includes(rt)) {
          return route.abort();
        }
        return route.continue();
      });

      const response = await page.goto(url, {
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

      const finalUrl = response?.url() ?? url;
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
        const linkElements = Array.from(
          document.querySelectorAll('a[href]'),
        ) as HTMLAnchorElement[];

        return {
          title: document.title ?? '',
          rawText: document.body?.innerText ?? '',
          html: document.documentElement?.outerHTML ?? '',
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
          links: linkElements
            .map((node) => node.href?.trim())
            .filter((value): value is string => Boolean(value)),
        };
      });

      const jsonLdEntries = parseJsonLd(scraped.jsonLdScripts);
      const structured: PageScrapeResult['structured'] = {
        description: '',
        priceText: '',
        ageText: '',
        openingHoursText: '',
        addressText: '',
        extractedHours: '',
        extractedAge: '',
        extractedPrice: '',
        extractedDescription: '',
        jsonLdHours: undefined,
        jsonLdEvents: [],
      };

      const assignIfEmpty = (
        field: Exclude<keyof PageScrapeResult['structured'], 'jsonLdHours' | 'jsonLdEvents'>,
        value?: string,
      ) => {
        if (!value) return;
        if (!structured[field]) {
          structured[field] = cleanText(value);
        }
      };

      for (const entry of jsonLdEntries) {
        const data = entry as Record<string, any>;
        assignIfEmpty('description', (data.description ?? data.name) as string);
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
          const hours = Array.isArray(data.openingHours)
            ? data.openingHours.join(', ')
            : String(data.openingHours);
          assignIfEmpty('openingHoursText', hours);
        }
        if (data.openingHoursSpecification) {
          const spec = Array.isArray(data.openingHoursSpecification)
            ? data.openingHoursSpecification
                .map((item: any) => (item?.opens ? `${item.opens}–${item?.closes ?? ''}` : ''))
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
      ]
        .filter(Boolean)
        .join('\n\n');
      const combinedText = cleanText(textCandidates).slice(0, 20_000);
      const hoursLinkPatterns = [
        /opening (?:hours?|times?)[^\.]*?\b(see|check|view|full|detailed|current)[^\.]*?\b(here|below|this page|on this page|this link)/gi,
        /(?:see|check|view|full|detailed|current)[^\.]*?\bopening (?:hours?|times?)[^\.]*?\b(here|below|this page)/gi,
        /hours?[^\.]{0,80}\bhere\b/gi,
      ];
        const semanticLinks: string[] = [];
        for (const pattern of hoursLinkPatterns) {
          const matches = [...combinedText.matchAll(pattern)];
          for (const match of matches) {
            const snippet = match[0];
            const links = await page.evaluate(
              (snip: string) => {
                const anchors = Array.from(document.querySelectorAll<HTMLAnchorElement>('a[href]'));
                return anchors
                  .filter((a) => {
                    const text = a.textContent?.toLowerCase() || '';
                    const href = a.getAttribute('href') || '';
                    const lowered = snip.toLowerCase();
                    return (
                      text.includes('here') ||
                      href.includes('opening') ||
                      lowered.includes(text) ||
                      lowered.includes(href)
                    );
                  })
                  .map((a) => a.href);
              },
              snippet,
            );
            semanticLinks.push(...links);
          }
        }
      const pageText = scraped.html ?? '';
      const jsonLdMatches = pageText.match(/<script type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
      const jsonLdEntriesFromHtml = parseJsonLd(
        jsonLdMatches.map((match: string) => match.replace(/<script[^>]*>|<\/script>/gi, '')),
      );

      let jsonLdHours: JsonLdHoursResult | undefined;
      const jsonLdEvents: JsonLdEvent[] = [];
      for (const entry of jsonLdEntriesFromHtml) {
        const rawType = entry['@type'];
        const typeCandidates: string[] = [];
        if (Array.isArray(rawType)) {
          for (const candidate of rawType) {
            if (typeof candidate === 'string') {
              typeCandidates.push(candidate.toLowerCase());
            }
          }
        } else if (typeof rawType === 'string') {
          typeCandidates.push(rawType.toLowerCase());
        }
        if (typeCandidates.some((value) => value.includes('event'))) {
          jsonLdEvents.push({
            startDate: typeof entry.startDate === 'string' ? entry.startDate : undefined,
            endDate: typeof entry.endDate === 'string' ? entry.endDate : undefined,
            startTime: typeof entry.startTime === 'string' ? entry.startTime : undefined,
            doorTime: typeof entry.doorTime === 'string' ? entry.doorTime : undefined,
            eventStatus: typeof entry.eventStatus === 'string' ? entry.eventStatus : undefined,
            name: typeof entry.name === 'string' ? entry.name : undefined,
          });
        }
        const rawSpec = entry.openingHoursSpecification ?? entry.openingHours;
        if (!rawSpec) continue;
        const specArray = Array.isArray(rawSpec) ? rawSpec : [rawSpec];
        const parsed = parseJsonLdHours(specArray);
        if (!parsed) continue;
        jsonLdHours = parsed;
        if (Object.keys(parsed.hours ?? {}).length >= 4) {
          break;
        }
      }

      structured.jsonLdEvents = jsonLdEvents;
      structured.jsonLdHours = jsonLdHours;

      if (jsonLdHours && !structured.extractedHours) {
        structured.extractedHours = Object.entries(jsonLdHours.hours ?? {})
          .filter(([, value]) => value && typeof value === 'object' && 'open' in (value as Record<string, unknown>))
          .map(([day, value]) => `${day}: ${(value as { open: string }).open}–${(value as { close: string }).close}`)
          .join(', ');
      }
      if (!combinedText) {
        logDebug(`Scrape returned empty text for ${url}`, 'scraper');
        continue;
      }

      if (!structured.extractedHours) {
        structured.extractedHours = cleanedMatch(
          combinedText.match(/opening\s*(hours?|times?)[\s\S]{0,600}?(?=\n\n|$)/i),
        );
      }
      structured.extractedAge =
        cleanedMatch(
          combinedText.match(
            /(age|recommended|suitable|years?.old|minimum|under\s*\d|from\s*\d\s*years)[\s\S]{0,400}/i,
          ),
        );
      structured.extractedPrice =
        cleanedMatch(combinedText.match(/(price|ticket|cost|from\s*£|adult|child|family.*ticket)[\s\S]{0,500}/i));
      structured.extractedDescription =
        cleanedMatch(
          combinedText.match(/(welcome|step into|experience|journey|meet.*shrek|far far away)[\s\S]{0,1200}/i),
        );

      const normalizedSemanticLinks = Array.from(
        new Set(
          semanticLinks
            .map((link) => normalizeQueueUrl(link))
            .filter((link): link is string => Boolean(link)),
        ),
      );
      if (normalizedSemanticLinks.length) {
        logDebug(
          `[Semantic] Found ${normalizedSemanticLinks.length} high-confidence hours link(s) on ${url}`,
          'scraper',
        );
      }
      const finalTitle = cleanText(scraped.title) || '';
      await page.waitForTimeout(1200);
      return {
        page: {
          url: finalUrl,
          title: finalTitle,
          text: combinedText,
          structured,
          html: scraped.html ?? '',
        },
        status: response?.status() ?? null,
        links: Array.from(new Set(scraped.links)),
        semanticHoursLinks: normalizedSemanticLinks,
      };
    } catch (error) {
      logDebug(
        `Scrape attempt ${attempt} failed for ${url}: ${(error as Error).message}`,
        'scraper',
      );
    } finally {
      if (browser) {
        await browser.close();
      }
    }
  }
  return null;
}

export interface OfficialContentResult {
  pages: PageScrapeResult[];
  scoredUrls: { url: string; score: number }[];
  scores: { url: string; score: number }[];
  dateTimePage: PageScrapeResult | null;
  generalPages: PageScrapeResult[];
  allScrapedPages: PageScrapeResult[];
  preExtractedDates: Dates | null;
  preClassifiedType: 'place' | 'event' | null;
  deepScrapeUrl: string | null;
}

function aggregateDatesFromPages(
  pages: PageScrapeResult[],
  location: SharedLocation,
): { dates: Dates | null; type: 'place' | 'event' | null } {
  const allInstances = pages.flatMap((page) =>
    page.deepDateTimeData && page.deepDateTimeData.length
      ? page.deepDateTimeData
      : extractRawDateTimeInstancesFromPage(page, undefined, {
          jsonLdHours: page.structured.jsonLdHours,
        }),
  );
  const derived = parseAndClassifyDates(allInstances, location);
  if (derived.dates) {
    return derived;
  }

  return aggregateDatesFromChrono(pages, location);
}

function aggregateDatesFromChrono(
  pages: PageScrapeResult[],
  location: SharedLocation,
): { dates: Dates | null; type: 'place' | 'event' | null } {
  type ChronoResult = Extract<
    ReturnType<typeof extractDatesAndTimesFromPage>,
    { source: 'chrono' }
  >;
  const chronoResults = pages
    .map((page) =>
      extractDatesAndTimesFromPage(page.text, page.url, {
        jsonLdHours: page.structured.jsonLdHours,
      }),
    )
    .filter((result): result is ChronoResult => result.source === 'chrono');

  if (!chronoResults.length) {
    return { dates: null, type: null };
  }

  let bestPlaceResult: ChronoResult | undefined;
  let bestDayCount = 0;
  for (const result of chronoResults) {
    if (result.openingHours) {
      const dayCount = Object.keys(result.openingHours).length;
      if (dayCount > bestDayCount) {
        bestDayCount = dayCount;
        bestPlaceResult = result;
      }
    }
  }

  if (bestPlaceResult && bestDayCount >= 4 && bestPlaceResult.openingHours) {
    return {
      dates: {
        kind: 'place',
        location,
        openingHours: bestPlaceResult.openingHours,
      },
      type: 'place',
    };
  }

  const eventInstances = chronoResults
    .flatMap((result) => result.eventInstances ?? [])
    .map((instance) => ({
      ...instance,
      endTime: instance.endTime ?? instance.startTime,
      location,
    }));

  if (eventInstances.length) {
    return {
      dates: {
        kind: 'event',
        instances: eventInstances,
      },
      type: 'event',
    };
  }

  return { dates: null, type: null };
}

export async function getOfficialUrlAndContent(
  inputUrl: string,
  activityName: string,
  userAgents: string[],
  location: SharedLocation,
): Promise<OfficialContentResult> {
  const origin = (() => {
    try {
      return new URL(inputUrl).origin;
    } catch {
      return null;
    }
  })();
  pagesCrawled = 0;
  const activityTokens = (normalizeActivityForHostname(activityName) || '').match(/.{3,}/g) || [];
  const allKeywords = [...RELEVANT_PATH_KEYWORDS, ...activityTokens];
  const allPotentialLinks = new Set<string>();
  const semanticPriorityLinks = new Set<string>();
  const getBudgetState = (highPriority = false): 'ok' | 'soft' | 'hard' => {
    if (pagesCrawled >= CRAWL_BUDGET_HARD) return 'hard';
    if (!highPriority && pagesCrawled >= CRAWL_BUDGET_SOFT) return 'soft';
    return 'ok';
  };

  const canAddNewUrl = (highPriority = false): boolean => getBudgetState(highPriority) === 'ok';

  const tryAddLink = (set: Set<string>, url: string, highPriority = false): boolean => {
    if (!canAddNewUrl(highPriority)) return false;
    set.add(url);
    return true;
  };

  const urlValidityCache = new Map<string, boolean>();
  async function passesUrlFilters(candidateUrl: string): Promise<boolean> {
    const preScore = quickPreScoreUrl(candidateUrl);
    if (preScore < MIN_URL_PRE_SCORE) {
      logDebug(`[Pre-filter] ${candidateUrl} scored ${preScore} – skipping`, 'scraper');
      return false;
    }
    if (urlValidityCache.has(candidateUrl)) {
      return urlValidityCache.get(candidateUrl)!;
    }
    const isValid = await isUrlValidAndHtml(candidateUrl);
    urlValidityCache.set(candidateUrl, isValid);
    if (!isValid) {
      logDebug(`[Pre-filter] ${candidateUrl} failed HEAD/html`, 'scraper');
    }
    return isValid;
  }

  const normalizedInput = normalizeQueueUrl(inputUrl);
  const rootUrl = origin ? (origin.endsWith('/') ? origin : `${origin}/`) : null;
  const normalizedRoot = rootUrl ? normalizeQueueUrl(rootUrl) : null;
  const guessSeedUrl = normalizedRoot ?? normalizedInput ?? inputUrl;

  if (origin) {
    const sitemapUrls = await loadSitemap(origin);
    if (sitemapUrls.length) {
      logDebug(`[${activityName}] Loaded ${sitemapUrls.length} links from sitemap`, activityName);
    }
    for (const url of sitemapUrls) {
      const normalized = normalizeQueueUrl(url);
      if (normalized) {
        if (!(await passesUrlFilters(normalized))) {
          continue;
        }
        if (!tryAddLink(allPotentialLinks, normalized, true)) {
          break;
        }
      }
    }
  }
  if (normalizedInput) {
    tryAddLink(allPotentialLinks, normalizedInput, true);
  } else {
    tryAddLink(allPotentialLinks, inputUrl, true);
  }
  if (normalizedRoot) {
    tryAddLink(allPotentialLinks, normalizedRoot, true);
  } else if (rootUrl) {
    tryAddLink(allPotentialLinks, rootUrl, true);
  }
  const queue: Array<{ url: string; depth: number }> = [
    { url: normalizedInput ?? inputUrl, depth: 0 },
  ];
  if (origin) {
    queue.unshift({ url: normalizedRoot ?? rootUrl ?? `${origin}/`, depth: 0 });
  }
  const visited = new Set<string>();
  const scored = new Map<string, { page: PageScrapeResult; score: number }>();
  const allPages = new Map<string, PageScrapeResult>();
  let crawled = 0;
  let maxCrawlPages = MAX_CRAWL_PAGES;

  function urlScore(url: string): number {
    if (semanticPriorityLinks.has(url)) return 9999;
    try {
      const path = new URL(url).pathname.toLowerCase();
      let score = 0;
      if (/opening[- ]?(hours?|times?)/.test(path)) score += 50;
      if (/hours|times/.test(path)) score += 20;
      if (/visit|plan|info|faq|practical|before/.test(path)) score += 15;
      RELEVANT_PATH_KEYWORDS.forEach((kw) => {
        if (path.includes(kw)) score += 5;
      });
      if (/book|ticket|checkout|buy/.test(path)) score -= 10;
      return score;
    } catch {
      return 0;
    }
  }

  const addCandidate = (candidate: ScrapeCandidate) => {
    const score = scorePage(candidate.page, activityName);
    const existing = scored.get(candidate.page.url);
    if (!existing || score > existing.score) {
      scored.set(candidate.page.url, { page: candidate.page, score });
    }
    allPages.set(candidate.page.url, candidate.page);
  };

  let smartGuessesInjected = false;
  async function maybeInjectSmartGuesses(
    page: PageScrapeResult,
    depth: number,
    targetQueue: Array<{ url: string; depth: number }>,
  ) {
    if (smartGuessesInjected) return;
    const base = guessSeedUrl ?? page.url;
    const guesses = generateSmartHoursGuesses(page.text, base);
    if (!guesses.length) {
      smartGuessesInjected = true;
      return;
    }
    smartGuessesInjected = true;
    for (const guess of guesses) {
      const normalized = normalizeQueueUrl(guess);
      if (!normalized) continue;
      if (visited.has(normalized) || allPotentialLinks.has(normalized)) continue;
      if (!(await passesUrlFilters(normalized))) continue;
      if (!tryAddLink(allPotentialLinks, normalized, true)) continue;
      targetQueue.push({ url: normalized, depth: depth + 1 });
    }
    targetQueue.sort((a, b) => urlScore(b.url) - urlScore(a.url));
  }

  const crawlQueue = async (seedQueue?: Array<{ url: string; depth: number }>) => {
    const workQueue = seedQueue ?? queue;
    while (workQueue.length && crawled < maxCrawlPages && pagesCrawled < CRAWL_BUDGET_HARD) {
      const { url, depth } = workQueue.shift()!;
      if (visited.has(url)) continue;
      visited.add(url);
      const candidate = await scrapeSinglePage(url, userAgents);
      if (!candidate) continue;
      crawled += 1;
      pagesCrawled += 1;
      addCandidate(candidate);
      await maybeInjectSmartGuesses(candidate.page, depth, workQueue);
      if (candidate.semanticHoursLinks?.length) {
        const goldenUrls = new Set<string>();

        for (const raw of candidate.semanticHoursLinks) {
          try {
            let u = new URL(raw, url).toString();
            u = u.split('#')[0].split('?')[0].replace(/\/$/, '');
            if (!visited.has(u)) {
              goldenUrls.add(u);
            }
          } catch {}
        }

        if (goldenUrls.size > 0) {
          logDebug(
            `[GOLDEN] Injecting ${goldenUrls.size} unvisited hours pages into crawl queue NOW`,
            'scraper',
          );
          for (const u of goldenUrls) {
            if (visited.has(u) || allPotentialLinks.has(u)) continue;
            if (!(await passesUrlFilters(u))) continue;
            if (!tryAddLink(allPotentialLinks, u, true)) continue;
            queue.unshift({ url: u, depth: 0 });
          }
        }
      }

      const currentUrlScore = urlScore(url);
      if (depth >= 4) {
        if (currentUrlScore < 60) continue;
      } else if (depth >= 3) {
        continue;
      }
      for (const link of candidate.links) {
        try {
          const resolved = new URL(link, url);
          if (origin && resolved.origin !== origin) continue;
          const normalized = normalizeQueueUrl(resolved.toString());
          if (!normalized || visited.has(normalized) || allPotentialLinks.has(normalized)) continue;
          if (!(await passesUrlFilters(normalized))) continue;
          if (!tryAddLink(allPotentialLinks, normalized, true)) continue;
          const pathLower = new URL(normalized).pathname.toLowerCase();
          const isRelevant = allKeywords.some((kw) => pathLower.includes(kw));
          if (!isRelevant && depth >= 1) continue;
          const targetQueue = seedQueue ? workQueue : queue;
          targetQueue.push({ url: normalized, depth: depth + 1 });
          targetQueue.sort((a, b) => urlScore(b.url) - urlScore(a.url));
        } catch {
          continue;
        }
      }
    }
  };

  const runTaskSpecificCrawls = async (
    originParam: string | null,
    potentialLinks: Set<string>,
    visitedLinks: Set<string>,
    agents: string[],
    name: string,
    tasks: CrawlTask[] = CRAWL_TASKS,
  ): Promise<void> => {
    const sanitizedOrigin = originParam ? (originParam.endsWith('/') ? originParam : `${originParam}/`) : '';
    const miniCrawledTasks = new Set<string>();
    for (const task of tasks) {
      if (crawled >= MAX_CRAWL_PAGES_FALLBACK) break;
      const candidates = new Set<string>();
      for (const link of potentialLinks) {
        const budgetState = getBudgetState();
        if (budgetState === 'hard') return;
        if (budgetState === 'soft') continue;
        const normalizedLink = normalizeQueueUrl(link);
        if (!normalizedLink) continue;
        const lowered = normalizedLink.toLowerCase();
        if (task.urlKeywords.some((kw) => lowered.includes(kw))) {
          candidates.add(normalizedLink);
        }
      }
      const scored = Array.from(candidates)
        .filter((link) => link && !visitedLinks.has(link))
        .map((link) => {
          const lower = link.toLowerCase();
          const textBonus = task.textKeywords.some((kw) => lower.includes(kw)) ? 10 : 0;
          return { url: link, score: urlScore(link) + task.scoreBoost + textBonus };
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 8);
      if (!scored.length) continue;
      logDebug(
        `[${name}] Task-specific crawl (${task.id}) driving ${scored.length} URLs: ${scored
          .map((item) => item.url)
          .join(', ')}`,
        name,
      );
      const hoursTaskDef = CRAWL_TASKS.find((t) => t.id === 'hours');
      for (const candidate of scored) {
        if (crawled >= MAX_CRAWL_PAGES_FALLBACK) break;
        if (visitedLinks.has(candidate.url)) continue;
        if (!(await passesUrlFilters(candidate.url))) continue;
        if (!canAddNewUrl(true)) {
          if (pagesCrawled >= CRAWL_BUDGET_HARD) return;
          continue;
        }
        const extra = await scrapeSinglePage(candidate.url, agents);
        if (extra) {
          crawled += 1;
          pagesCrawled += 1;
          addCandidate(extra);
          visitedLinks.add(candidate.url);
          if (
            task.id === 'hours' &&
            !miniCrawledTasks.has(task.id) &&
            extractOpeningHours([extra.page]).source === 'none'
          ) {
            logDebug(`[Recursive] No hours in ${candidate.url}; mini-crawling its links`, 'scraper');
            const miniQueue: { url: string; depth: number; score: number }[] = [];
            const seen = new Set<string>();
            for (const link of extra.links ?? []) {
              if (!canAddNewUrl(true)) {
                if (pagesCrawled >= CRAWL_BUDGET_HARD) return;
                break;
              }
              try {
                const resolvedRaw = new URL(link, candidate.url).toString();
                const resolved = normalizeQueueUrl(resolvedRaw);
                if (!resolved) continue;
                if (
                  visitedLinks.has(resolved) ||
                  seen.has(resolved) ||
                  allPotentialLinks.has(resolved)
                )
                  continue;
                seen.add(resolved);
                if (!(await passesUrlFilters(resolved))) continue;
                if (!tryAddLink(potentialLinks, resolved, true)) {
                  if (pagesCrawled >= CRAWL_BUDGET_HARD) return;
                }
                let miniScore = urlScore(resolved);
                if (
                  hoursTaskDef &&
                  hoursTaskDef.urlKeywords.some((kw) => resolved.toLowerCase().includes(kw))
                ) {
                  miniScore += 200;
                }
                if (getBudgetState(true) === 'hard') return;
                miniQueue.push({ url: resolved, depth: 1, score: miniScore });
              } catch {
                // ignore
              }
            }
            if (miniQueue.length) {
              miniQueue.sort((a, b) => b.score - a.score);
              await crawlQueue(miniQueue.slice(0, 8).map((item) => ({ url: item.url, depth: item.depth })));
              miniCrawledTasks.add(task.id);
            }
          }
        }
      }
    }
  };

  const buildSortedResults = () => {
    const hoursTask = CRAWL_TASKS.find((task) => task.id === 'hours');
    const hoursCandidates = hoursTask
      ? Array.from(scored.entries())
          .filter(
            ([url, entry]) =>
              hoursTask.urlKeywords.some((kw) => url.toLowerCase().includes(kw)) ||
              Boolean(entry.page.structured.extractedHours),
          )
          .map(([url, entry]) => {
            let boostedScore = entry.score;
            if (hoursTask.textKeywords.some((kw) => entry.page.text.toLowerCase().includes(kw))) {
              boostedScore += hoursTask.scoreBoost;
            }
            return { url, score: boostedScore, page: entry.page };
          })
          .sort((a, b) => b.score - a.score)
          .slice(0, 1)
          .map((item) => item.page)
      : [];

    const hoursUrls = new Set(hoursCandidates.map((page) => page.url));
    const generalCandidates = Array.from(scored.entries())
      .filter(([url]) => !hoursUrls.has(url))
      .sort((a, b) => b[1].score - a[1].score)
      .slice(0, 2)
      .map((entry) => entry[1].page);

    logDebug(
      `[${activityName}] Tracks selected: Hours (${hoursCandidates.map((p) => p.url).join(', ')}) | General (${generalCandidates
        .map((p) => p.url)
        .join(', ')})`,
      activityName,
    );

    return [...hoursCandidates, ...generalCandidates];
  };

  await crawlQueue();
  let sorted = buildSortedResults();
  if (sorted.length < 2 && maxCrawlPages < MAX_CRAWL_PAGES_FALLBACK) {
    maxCrawlPages = MAX_CRAWL_PAGES_FALLBACK;
    await crawlQueue();
    sorted = buildSortedResults();
  }

  const hoursResult = extractOpeningHours(sorted);
  if (hoursResult.source === 'none') {
    logDebug(
      `[${activityName}] No opening hours found in top pages – launching targeted hours crawl`,
      activityName,
    );
    const hoursCandidates = Array.from(allPotentialLinks)
      .filter((link) => !visited.has(link))
      .sort((a, b) => urlScore(b) - urlScore(a))
      .slice(0, 6);

    for (const url of hoursCandidates) {
      if (crawled >= MAX_CRAWL_PAGES_FALLBACK) break;
      if (!canAddNewUrl(true)) break;
      if (!(await passesUrlFilters(url))) continue;
      const extra = await scrapeSinglePage(url, userAgents);
      if (extra) {
        addCandidate(extra);
        crawled += 1;
        pagesCrawled += 1;
        visited.add(url);
      }
    }
    sorted = buildSortedResults();
  }

  sorted = buildSortedResults();
  let extraction = extractOpeningHours(sorted);
  if (extraction.source === 'none') {
    logDebug('[Efficiency] Hours still missing – running hours-only task crawls', activityName);
    const hoursTask = CRAWL_TASKS.filter((task) => task.id === 'hours');
    await runTaskSpecificCrawls(origin, allPotentialLinks, visited, userAgents, activityName, hoursTask);
  } else {
    logDebug('[Efficiency] Hours found early; skipping hours-only task crawls', activityName);
  }
  sorted = buildSortedResults();
  extraction = extractOpeningHours(sorted);
  const otherDataMissing = sorted.every(
    (page) =>
      !page.structured.extractedAge &&
      !page.structured.extractedPrice &&
      !page.structured.extractedDescription &&
      !page.structured.description,
  );
  if (extraction.source === 'none' || otherDataMissing) {
    logDebug(
      `[Efficiency] Additional data missing (hoursMissing=${
        extraction.source === 'none'
      }, otherMissing=${otherDataMissing}) – running remaining task crawls`,
      activityName,
    );
    const remainingTasks = CRAWL_TASKS.filter((task) => task.id !== 'hours');
    await runTaskSpecificCrawls(origin, allPotentialLinks, visited, userAgents, activityName, remainingTasks);
  } else {
    logDebug('[Efficiency] Other data looks complete; skipping remaining task-specific crawls', activityName);
  }
  sorted = buildSortedResults();

  const scoredUrls = Array.from(scored.entries())
    .map(([url, entry]) => ({ url, score: entry.score }))
    .sort((a, b) => b.score - a.score);

  const hoursScoreAll = Array.from(allPages.entries()).map(([url, page]) => ({
    url,
    page,
    score: scoreUrlForTimeInfo(page),
  }));
  const hoursScoreSorted = [...hoursScoreAll].sort((a, b) => b.score - a.score);

  const candidateList: Array<{ url: string; page: PageScrapeResult; score: number }> = [];
  const seenCandidates = new Set<string>();
  for (const candidate of hoursScoreSorted) {
    if (seenCandidates.has(candidate.url)) continue;
    seenCandidates.add(candidate.url);
    candidateList.push(candidate);
    if (candidateList.length >= 3) break;
  }

  const scoreMap = new Map<string, number>();
  for (const { url, score } of hoursScoreAll) {
    const existing = scoreMap.get(url);
    scoreMap.set(url, existing !== undefined ? Math.max(existing, score) : score);
  }
  const scores = Array.from(scoreMap.entries())
    .map(([url, score]) => ({ url, score }))
    .sort((a, b) => b.score - a.score);

  let dateTimePage: PageScrapeResult | null = null;
  let winningClassification: { dates: Dates | null; type: 'place' | 'event' | null } = {
    dates: null,
    type: null,
  };
  let finalDeepScrapeUrl: string | null = null;

  const topCandidates = candidateList.slice(0, 3);
  for (const candidate of topCandidates) {
    let pageResult = candidate.page;
    if (!pageResult) {
      const extra = await scrapeSinglePage(candidate.url, userAgents);
      if (!extra) {
        logWarn(
          `[${activityName}] Unable to rescrape ${candidate.url}; skipping to next candidate`,
          activityName,
        );
        continue;
      }
      addCandidate(extra);
      pageResult = extra.page;
    }

    const structured = extractStructuredDatesFromPage(pageResult, location);
    if (structured) {
      dateTimePage = pageResult;
      winningClassification = { dates: structured.dates, type: structured.type };
      break;
    }

    const calendarResult = extractCalendarExceptionsFromText(pageResult.text);
    if (
      calendarResult.exceptions.length >= 5 &&
      calendarResult.openingHours &&
      Object.keys(calendarResult.openingHours).length >= 4
    ) {
      const placeDates: PlaceDates = {
        kind: 'place',
        location,
        openingHours: calendarResult.openingHours as Record<DayLabel, { open: string; close: string }>,
        exceptions: calendarResult.exceptions,
      };
      dateTimePage = pageResult;
      winningClassification = { dates: placeDates, type: 'place' };
      break;
    }

    let instances = extractRawDateTimeInstancesFromPage(pageResult, undefined, {
      jsonLdHours: pageResult.structured.jsonLdHours,
    });
    let classification = parseAndClassifyDates(instances, location);
    if (!classification.dates && hasActualTimeInfo(pageResult)) {
      const format = classifyDateTimeFormat(pageResult);
      if (format === 'js-dynamic') {
        try {
          const deepInstances = await deepScrapeDynamicDateTime(pageResult.url, userAgents);
          if (deepInstances.length) {
            pageResult.deepDateTimeData = deepInstances;
            finalDeepScrapeUrl = pageResult.url;
            classification = parseAndClassifyDates(deepInstances, location);
            instances = deepInstances;
          }
        } catch (error) {
          logDebug(
            `[${activityName}] Deep date/time scrape failed for ${pageResult.url}: ${(error as Error).message}`,
            activityName,
          );
        }
      }
    }
    if (classification.dates) {
      dateTimePage = pageResult;
      winningClassification = classification;
      break;
    }

    logWarn(
      `[${activityName}] Extraction insufficient on ${candidate.url} (score ${candidate.score}); trying next`,
      activityName,
    );
  }

  const generalCandidates = buildSortedResults();
  const generalPages = generalCandidates
    .filter((page) => !dateTimePage || page.url !== dateTimePage.url)
    .slice(0, 2);
  const finalPages = [...(dateTimePage ? [dateTimePage] : []), ...generalPages];
  const allScrapedPages = Array.from(allPages.values());

  return {
    pages: finalPages,
    scoredUrls,
    scores,
    dateTimePage,
    generalPages,
    allScrapedPages,
    preExtractedDates: winningClassification.dates,
    preClassifiedType: winningClassification.type,
    deepScrapeUrl: finalDeepScrapeUrl,
  };
}

const CALENDAR_NEXT_SELECTORS = [
  'button[aria-label*="next month"]',
  'button[aria-label*="Next month"]',
  'button[aria-label*="next"]',
  'button:has-text("Next")',
  'button:has-text("›")',
  '.fc-next-button',
  '.rc-arrow-next',
  '.calendar-nav .next',
  '.next-month',
  '.month-nav-next',
  '.calendar-next',
  '.arrow-right',
  '[data-action="next"]',
];

const API_CALENDAR_PATTERN = /\/api\/calendar/i;

function extractMonthKeyFromHtml(html: string): string | null {
  const match = html.match(/(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}/i);
  return match ? match[0].toLowerCase() : null;
}

async function clickNextCalendar(page: Page): Promise<boolean> {
  for (const selector of CALENDAR_NEXT_SELECTORS) {
    const element = await page.$(selector);
    if (element) {
      await element.click().catch(() => null);
      return true;
    }
  }
  await page
    .evaluate(() => {
      const nextBtn =
        document.querySelector('.fc-next-button') || document.querySelector('[data-next-month]');
      if (nextBtn) {
        (nextBtn as HTMLElement).click();
      }
    })
    .catch(() => null);
  return false;
}

async function deepScrapeDynamicDateTime(url: string, userAgents: string[]): Promise<RawDateTimeInstance[]> {
  return retryOperation(async () => {
    let browser: Browser | null = null;
    try {
      const launchedBrowser = await chromium.launch(getLaunchOptions());
      browser = launchedBrowser;
      const context = await launchedBrowser.newContext({
        viewport: getRandomViewport(),
        userAgent: getRandomUserAgent(userAgents),
      });
      const page = await context.newPage();
      await page.route('**/*', (route: PlaywrightRoute) => {
        const rt = route.request().resourceType();
        if (['image', 'stylesheet', 'font', 'media'].includes(rt)) {
          return route.abort();
        }
        return route.continue();
      });
      const response = await page.goto(url, {
        waitUntil: 'domcontentloaded',
        timeout: SCRAPE_TIMEOUT_MS,
      });
      if (response) {
        await response.finished().catch(() => null);
      }
      await page.waitForLoadState('networkidle', { timeout: SCRAPE_TIMEOUT_MS }).catch(() => null);
      await page.waitForTimeout(900);

      const structuredTemplate = {
        description: '',
        priceText: '',
        ageText: '',
        openingHoursText: '',
        addressText: '',
        extractedHours: '',
        extractedAge: '',
        extractedPrice: '',
        extractedDescription: '',
        jsonLdHours: undefined,
        jsonLdEvents: [],
      };

      const aggregated = new Map<string, RawDateTimeInstance>();
      const recordInstance = (instance: RawDateTimeInstance) => {
        const key = `${instance.date}|${instance.startTime ?? ''}|${instance.endTime ?? ''}|${instance.note ?? ''}`;
        if (!aggregated.has(key)) {
          aggregated.set(key, instance);
        }
      };
      page.on('response', async (response: Response) => {
        const url = response.url();
        if (!API_CALENDAR_PATTERN.test(url)) return;
        const type = response.headers()['content-type'] ?? '';
        if (!type.includes('json')) return;
        try {
          const payload = await response.json();
          const parsed = normalizeCalendarApiPayload(payload);
          parsed.forEach(recordInstance);
        } catch {
          // ignore API parse failures
        }
      });

      const captureSnapshot = async () => {
        const snapshotText = await page.evaluate(() => document.body?.innerText ?? '');
        const snapshotHtml = await page.content();
        const stubPage: PageScrapeResult = {
          url,
          title: (await page.title()) || '',
          text: snapshotText,
          html: snapshotHtml,
          structured: { ...structuredTemplate },
        };
        const instances = extractRawDateTimeInstancesFromPage(stubPage);
        instances.forEach(recordInstance);
      };

      await captureSnapshot();

      const monthsSeen = new Set<string>();
      const initialMonth = extractMonthKeyFromHtml(await page.content());
      if (initialMonth) {
        monthsSeen.add(initialMonth);
        logDebug(`Captured month: ${initialMonth}, total unique months: ${monthsSeen.size}`, url);
      }

      const MIN_INTERACTIONS = 10;
      const TARGET_MONTHS = 3;
      const MAX_MONTHS = 12;
      const MAX_INTERACTIONS = 30;
      let interactions = 0;

      while (
        interactions < MAX_INTERACTIONS &&
        (interactions < MIN_INTERACTIONS || monthsSeen.size < TARGET_MONTHS) &&
        monthsSeen.size < MAX_MONTHS
      ) {
        const moved = await clickNextCalendar(page);
        interactions += 1;
        if (!moved) {
          await page.keyboard.press('PageDown').catch(() => null);
          await page.waitForTimeout(600);
          continue;
        }
        await page.waitForTimeout(1200);
        await captureSnapshot();
        const nextMonth = extractMonthKeyFromHtml(await page.content());
        if (nextMonth) {
          monthsSeen.add(nextMonth);
          logDebug(`Captured month: ${nextMonth}, total unique months: ${monthsSeen.size}`, url);
        }
      }

      return Array.from(aggregated.values());
    } finally {
      if (browser) {
        await browser.close();
      }
    }
  }, 'deep dynamic date/time scrape', 3);
}

async function retryOperation<T>(fn: () => Promise<T>, label: string, maxRetries = 5, delay = 2_000): Promise<T> {
  let attempt = 0;
  let currentDelay = delay;
  while (attempt < maxRetries) {
    try {
      return await fn();
    } catch (error) {
      attempt += 1;
      const message = (error as Error).message;
      logDebug(`[retry] ${label} attempt ${attempt} failed: ${message}`, 'retry');
      if (/net::ERR_HTTP2_PROTOCOL_ERROR/.test(message)) {
        logDebug('[retry] Detected HTTP2 protocol error; retrying.', 'retry');
      }
      if (attempt >= maxRetries) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, currentDelay));
      currentDelay = Math.round(currentDelay * 1.5);
    }
  }
  throw new Error(`[retry] ${label} exhausted after ${maxRetries} attempts`);
}
