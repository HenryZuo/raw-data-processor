import { chromium } from 'playwright';
import type { Browser, LaunchOptions, Route as PlaywrightRoute } from 'playwright-core';
import { existsSync } from 'node:fs';
import { cleanText, extractOpeningHours } from './utils.js';
import { normalizeActivityForHostname } from './url-resolver.js';
import type { PageScrapeResult } from './types.js';

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

function parseJsonLdHours(spec: any[]): Record<string, { open: string; close: string }> | null {
  const result: Record<string, { open: string; close: string }> = {};
  for (const item of spec) {
    if (!item?.opens || !item?.closes) continue;
    const days = Array.isArray(item.dayOfWeek) ? item.dayOfWeek : [item.dayOfWeek];
    for (const d of days) {
      const day =
        d?.includes('Monday')
          ? 'Mon'
          : d?.includes('Tuesday')
          ? 'Tue'
          : d?.includes('Wednesday')
          ? 'Wed'
          : d?.includes('Thursday')
          ? 'Thu'
          : d?.includes('Friday')
          ? 'Fri'
          : d?.includes('Saturday')
          ? 'Sat'
          : d?.includes('Sunday')
          ? 'Sun'
          : null;
      if (day) {
        result[day] = { open: String(item.opens).slice(0, 5), close: String(item.closes).slice(0, 5) };
      }
    }
  }
  return Object.keys(result).length >= 4 ? result : null;
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

const RELEVANT_PATH_KEYWORDS = [
  'visit',
  'plan-your-visit',
  'opening-hours',
  'opening-times',
  'hours',
  'tickets',
  'ticket-information',
  'prices',
  'plan-visit',
  'visit-us',
  'getting-here',
  'info',
  'practical-information',
];
const MAX_CRAWL_PAGES = 20;
const MAX_CRAWL_PAGES_FALLBACK = 40;

interface ScrapeCandidate {
  page: PageScrapeResult;
  status: number | null;
  links: string[];
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
      };

      const assignIfEmpty = (field: keyof PageScrapeResult['structured'], value?: string) => {
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
      const pageText = scraped.html ?? '';
      const jsonLdMatches = pageText.match(/<script type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
      const jsonLdEntriesFromHtml = parseJsonLd(
        jsonLdMatches.map((match: string) => match.replace(/<script[^>]*>|<\/script>/gi, '')),
      );

      let jsonLdHours: Record<string, { open: string; close: string }> | undefined;
      for (const entry of jsonLdEntriesFromHtml) {
        const spec = entry.openingHoursSpecification || entry.openingHours;
        if (Array.isArray(spec)) {
          const parsed = parseJsonLdHours(spec);
          if (parsed && Object.keys(parsed).length >= 4) {
            jsonLdHours = parsed;
            break;
          }
        }
      }

      if (jsonLdHours && !structured.extractedHours) {
        structured.extractedHours = Object.entries(jsonLdHours)
          .map(([day, { open, close }]) => `${day}: ${open}–${close}`)
          .join(', ');
      }
      if (!combinedText) {
        console.warn(`Scrape returned empty text for ${url}`);
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
      };
    } catch (error) {
      console.warn(`Scrape attempt ${attempt} failed for ${url}:`, (error as Error).message);
    } finally {
      if (browser) {
        await browser.close();
      }
    }
  }
  return null;
}

export async function getOfficialUrlAndContent(
  inputUrl: string,
  activityName: string,
  userAgents: string[],
): Promise<PageScrapeResult[]> {
  const origin = (() => {
    try {
      return new URL(inputUrl).origin;
    } catch {
      return null;
    }
  })();
  const activityTokens = (normalizeActivityForHostname(activityName) || '').match(/.{3,}/g) || [];
  const allKeywords = [...RELEVANT_PATH_KEYWORDS, ...activityTokens];
  const allPotentialLinks = new Set<string>();
  const queue: Array<{ url: string; depth: number }> = [{ url: inputUrl, depth: 0 }];
  if (origin) {
    queue.unshift({ url: `${origin}/`, depth: 0 });
  }
  const visited = new Set<string>();
  const scored = new Map<string, { page: PageScrapeResult; score: number }>();
  let crawled = 0;
  let maxCrawlPages = MAX_CRAWL_PAGES;

  function urlScore(url: string): number {
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
  };

  const crawlQueue = async () => {
    while (queue.length && crawled < maxCrawlPages) {
      const { url, depth } = queue.shift()!;
      if (visited.has(url)) continue;
      visited.add(url);
      const candidate = await scrapeSinglePage(url, userAgents);
      if (!candidate) continue;
      crawled += 1;
      addCandidate(candidate);

      if (depth >= 2) continue;
      for (const link of candidate.links) {
        try {
          const resolved = new URL(link, url);
          if (origin && resolved.origin !== origin) continue;
          const normalized = resolved.toString();
          allPotentialLinks.add(normalized);
          if (visited.has(normalized)) continue;
          const pathLower = resolved.pathname.toLowerCase();
          const isRelevant = allKeywords.some((kw) => pathLower.includes(kw));
          if (!isRelevant && depth >= 1) continue;
          queue.push({ url: normalized, depth: depth + 1 });
          queue.sort((a, b) => urlScore(b.url) - urlScore(a.url));
        } catch {
          continue;
        }
      }
    }
  };

  const buildSortedResults = () =>
    Array.from(scored.values())
      .sort((a, b) => b.score - a.score)
      .slice(0, 3)
      .map((entry) => entry.page);

  await crawlQueue();
  let sorted = buildSortedResults();
  if (sorted.length < 2 && maxCrawlPages < MAX_CRAWL_PAGES_FALLBACK) {
    maxCrawlPages = MAX_CRAWL_PAGES_FALLBACK;
    await crawlQueue();
    sorted = buildSortedResults();
  }

  const hoursResult = extractOpeningHours(sorted, []);
  if (hoursResult.source === 'none') {
    console.log(`[${activityName}] No opening hours found in top pages – launching targeted hours crawl`);
    const hoursCandidates = Array.from(allPotentialLinks)
      .filter((link) => !visited.has(link))
      .sort((a, b) => urlScore(b) - urlScore(a))
      .slice(0, 6);

    for (const url of hoursCandidates) {
      if (crawled >= MAX_CRAWL_PAGES_FALLBACK) break;
      const extra = await scrapeSinglePage(url, userAgents);
      if (extra) {
        addCandidate(extra);
        crawled += 1;
        visited.add(url);
      }
    }
    sorted = buildSortedResults();
  }

  return sorted;
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
      console.warn(`[retry] ${label} attempt ${attempt} failed: ${message}`);
      if (/net::ERR_HTTP2_PROTOCOL_ERROR/.test(message)) {
        console.warn('[retry] Detected HTTP2 protocol error; retrying.');
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
