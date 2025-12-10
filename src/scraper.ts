import { chromium } from 'playwright';
import type { Browser, LaunchOptions, Route as PlaywrightRoute } from 'playwright-core';
import { existsSync } from 'node:fs';
import { cleanText, extractDatesAndTimesFromPage, extractOpeningHours } from './utils.js';
import { normalizeActivityForHostname } from './url-resolver.js';
import type { PageScrapeResult } from './types.js';

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
  score -= (url.split('/').length - 3) * 10;

  return Math.min(score, 1000);
}

function hasActualTimeInfo(page: PageScrapeResult): boolean {
  const parsed = extractDatesAndTimesFromPage(page.text, page.url);

  if (parsed.source === 'none') return false;

  if (parsed.openingHours) {
    const daysWithHours = Object.values(parsed.openingHours).filter(
      (h) => h.open && h.close && h.open !== h.close,
    );
    return daysWithHours.length >= 2;
  }

  if (parsed.eventInstances && parsed.eventInstances.length > 0) {
    return parsed.eventInstances.some((inst) => Boolean(inst.startTime));
  }

  return true;
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
    console.log(`[Sitemap] Max depth reached for ${origin}`);
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

    console.log(
      `[Sitemap] Loaded ${urls.length} URLs from ${normalizedOrigin}sitemap.xml (index: ${isIndex}, depth: ${depth})`,
    );
    return urls.slice(0, 200);
  } catch (error) {
    console.warn(`[Sitemap] Failed for ${origin}: ${(error as Error).message}`);
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

const COMMON_ORIGIN_PATHS = [
  'opening-hours/',
  'opening-times/',
  'plan-your-visit/',
  'visit-us/',
  'before-you-visit/',
  'visitor-information/',
  'daily-hours/',
  'experience/',
];

const COMMON_PREFIXES = [
  'plan-your-visit/',
  'visitor-information/',
  'info/',
  'visit-us/',
  'before-you-visit/',
  'prepare-your-visit/',
  'practical-information/',
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

      const normalizedSemanticLinks = Array.from(
        new Set(
          semanticLinks
            .map((link) => normalizeQueueUrl(link))
            .filter((link): link is string => Boolean(link)),
        ),
      );
      if (normalizedSemanticLinks.length) {
        console.log(
          `[Semantic] Found ${normalizedSemanticLinks.length} high-confidence hours link(s) on ${url}`,
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
      console.warn(`Scrape attempt ${attempt} failed for ${url}:`, (error as Error).message);
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
}

export async function getOfficialUrlAndContent(
  inputUrl: string,
  activityName: string,
  userAgents: string[],
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
  if (origin) {
    const sitemapUrls = await loadSitemap(origin);
    if (sitemapUrls.length) {
      console.log(`[${activityName}] Loaded ${sitemapUrls.length} links from sitemap`);
    }
    for (const url of sitemapUrls) {
      const normalized = normalizeQueueUrl(url);
      if (normalized) {
        if (!tryAddLink(allPotentialLinks, normalized, true)) {
          break;
        }
      }
    }
  }
  const normalizedInput = normalizeQueueUrl(inputUrl);
  if (normalizedInput) {
    tryAddLink(allPotentialLinks, normalizedInput, true);
  } else {
    tryAddLink(allPotentialLinks, inputUrl, true);
  }
  if (origin) {
    const rootUrl = origin.endsWith('/') ? origin : `${origin}/`;
    const normalizedRoot = normalizeQueueUrl(rootUrl);
    if (normalizedRoot) {
      tryAddLink(allPotentialLinks, normalizedRoot, true);
    } else {
      tryAddLink(allPotentialLinks, rootUrl, true);
    }
  }
  const queue: Array<{ url: string; depth: number }> = [
    { url: normalizedInput ?? inputUrl, depth: 0 },
  ];
  if (origin) {
    queue.unshift({ url: normalizeQueueUrl(`${origin}/`) ?? `${origin}/`, depth: 0 });
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
          console.log(`[GOLDEN] Injecting ${goldenUrls.size} unvisited hours pages into crawl queue NOW`);
          for (const u of goldenUrls) {
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
          if (!normalized) continue;
          if (!tryAddLink(allPotentialLinks, normalized, true)) continue;
          if (visited.has(normalized)) continue;
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
      if (sanitizedOrigin) {
        for (const suffix of COMMON_ORIGIN_PATHS) {
          const target = `${sanitizedOrigin}${suffix}`;
          const normalizedTarget = normalizeQueueUrl(target);
          if (!normalizedTarget) continue;
          if (getBudgetState(true) === 'hard') return;
          candidates.add(normalizedTarget);
          if (!tryAddLink(potentialLinks, normalizedTarget, true)) {
            if (pagesCrawled >= CRAWL_BUDGET_HARD) return;
          }
        }
        if (task.id === 'hours') {
          const PRIORITY_SUFFIXES = ['opening-hours', 'opening-times', 'hours', 'times', 'schedule'];
          const PREFIXES = [
            'plan-your-visit/',
            'visit-us/',
            'visitor-information/',
            'before-you-visit/',
            'info/',
            'practical-information/',
          ];
          const combos = new Set<string>();
          for (const prefix of PREFIXES) {
            for (const suffix of PRIORITY_SUFFIXES) {
              const path1 = `${prefix}${suffix}/`;
              const path2 = `${prefix}before-you-visit/${suffix}/`;
              combos.add(`${sanitizedOrigin}${path1}`);
              combos.add(`${sanitizedOrigin}${path2}`);
            }
          }
          const finalList = Array.from(combos);
          console.log(`[Task hours] Smart combos generated: ${finalList.length}`);
          for (const url of finalList.slice(0, 25)) {
            if (getBudgetState(true) === 'hard') return;
            const normalized = normalizeQueueUrl(url);
            if (!normalized) continue;
            candidates.add(normalized);
            if (!tryAddLink(potentialLinks, normalized, true)) {
              if (pagesCrawled >= CRAWL_BUDGET_HARD) return;
            }
          }
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
      console.log(
        `[${name}] Task-specific crawl (${task.id}) driving ${scored.length} URLs: ${scored
          .map((item) => item.url)
          .join(', ')}`,
      );
      const hoursTaskDef = CRAWL_TASKS.find((t) => t.id === 'hours');
      for (const candidate of scored) {
        if (crawled >= MAX_CRAWL_PAGES_FALLBACK) break;
        if (visitedLinks.has(candidate.url)) continue;
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
            console.log(`[Recursive] No hours in ${candidate.url}; mini-crawling its links`);
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
                if (visitedLinks.has(resolved) || seen.has(resolved)) continue;
                seen.add(resolved);
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

    console.log(
      `[${activityName}] Tracks selected: Hours (${hoursCandidates.map((p) => p.url).join(', ')}) | General (${generalCandidates
        .map((p) => p.url)
        .join(', ')})`,
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
    console.log(`[${activityName}] No opening hours found in top pages – launching targeted hours crawl`);
    const hoursCandidates = Array.from(allPotentialLinks)
      .filter((link) => !visited.has(link))
      .sort((a, b) => urlScore(b) - urlScore(a))
      .slice(0, 6);

    for (const url of hoursCandidates) {
      if (crawled >= MAX_CRAWL_PAGES_FALLBACK) break;
      if (!canAddNewUrl(true)) break;
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
    console.log(`[Efficiency] Hours still missing – running hours-only task crawls`);
    const hoursTask = CRAWL_TASKS.filter((task) => task.id === 'hours');
    await runTaskSpecificCrawls(origin, allPotentialLinks, visited, userAgents, activityName, hoursTask);
  } else {
    console.log('[Efficiency] Hours found early; skipping hours-only task crawls');
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
    console.log(
      `[Efficiency] Additional data missing (hoursMissing=${
        extraction.source === 'none'
      }, otherMissing=${otherDataMissing}) – running remaining task crawls`,
    );
    const remainingTasks = CRAWL_TASKS.filter((task) => task.id !== 'hours');
    await runTaskSpecificCrawls(origin, allPotentialLinks, visited, userAgents, activityName, remainingTasks);
  } else {
    console.log('[Efficiency] Other data looks complete; skipping remaining task-specific crawls');
  }
  sorted = buildSortedResults();

  const scoredUrls = Array.from(scored.entries())
    .map(([url, entry]) => ({ url, score: entry.score }))
    .sort((a, b) => b.score - a.score);

  const timeScored = Array.from(allPages.entries())
    .map(([url, page]) => ({ url, page, score: scoreUrlForTimeInfo(page) }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);

  let hoursRep: PageScrapeResult | null = null;
  for (const candidate of timeScored) {
    if (hasActualTimeInfo(candidate.page)) {
      hoursRep = candidate.page;
      console.log(`[HOURS TRACK] Selected ${candidate.url} as time-info rep (score: ${candidate.score})`);
      break;
    }
    console.log(`[HOURS TRACK] Rejected ${candidate.url} - no strict time info`);
  }

  if (!hoursRep && timeScored.length > 0) {
    hoursRep = timeScored[0].page;
    console.log(
      `[HOURS TRACK] Fallback (no strict match) → using highest-scored page: ${timeScored[0].url} (score: ${timeScored[0].score})`,
    );
  }

  if (!hoursRep) {
    console.log(`[HOURS TRACK] No valid time-info page found - excluding activity`);
    return { pages: [], scoredUrls };
  }

  const generalCandidates = buildSortedResults();
  const filteredGeneral = generalCandidates.filter((page) => page.url !== hoursRep?.url).slice(0, 2);
  const finalPages = [hoursRep, ...filteredGeneral];

  return { pages: finalPages, scoredUrls };
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
