import { execFile } from 'node:child_process';
import fs from 'fs/promises';
import path from 'path';
import { mapAddressToAreas, activitySchema } from './schema-loader.js';
import { resolveOfficialUrl } from './url-resolver.js';
import { getOfficialUrlAndContent, type OfficialContentResult } from './scraper.js';
import { fetchLLMDataWithRetries, ActivityLLMOutput } from './llm.js';
import {
  resolveCoordinates,
  buildAddressLine,
  isBookingDomain,
  USER_AGENT_STRINGS,
  formatExportTimestamp,
  sanitizeNameForFilename,
  logInfo,
  logDebug,
  logWarn,
} from './utils.js';
import { DAY_LABELS } from './types.js';
import { extractHoursFromText } from './utils/opening-hours-parser.js';
import type { DayLabel, PageScrapeResult, SourceEvent } from './types.js';
import type {
  Activity,
  Dates,
  Exception as SharedException,
  EventDates,
  Location as SharedLocation,
  PlaceDates,
  PriceLevel,
} from '../../london-kids-p1/packages/shared/src/activity.js';

const SCRAPE_LOG_DIR = path.resolve(process.cwd(), 'scrape-logs');
const SCORED_URLS_DIR = path.resolve(process.cwd(), 'scored-urls');

async function exportScrapedContent(
  eventId: string,
  eventName: string,
  page: PageScrapeResult,
  pageIndex: number,
): Promise<void> {
  try {
    await fs.mkdir(SCRAPE_LOG_DIR, { recursive: true });
    const timestamp = formatExportTimestamp();
    const nameSegment = sanitizeNameForFilename(eventName);
    const filename = `scrape-${timestamp}-${nameSegment}-p${pageIndex + 1}.json`;
    const payload = {
      eventId,
      url: page.url,
      title: page.title,
      structured: page.structured,
      text: page.text,
    };
    await fs.writeFile(path.join(SCRAPE_LOG_DIR, filename), `${JSON.stringify(payload, null, 2)}\n`, 'utf-8');
  } catch (error) {
    logWarn(`Failed to export scraped content: ${(error as Error).message}`, eventId);
  }
}

async function exportScoredUrls(
  eventId: string,
  eventName: string,
  pageScores: { url: string; score: number }[],
  scores: { url: string; score: number }[],
  deepScrapeUrl: string | null,
): Promise<void> {
  try {
    await fs.mkdir(SCORED_URLS_DIR, { recursive: true });
    const timestamp = formatExportTimestamp();
    const nameSegment = sanitizeNameForFilename(eventName);
    const filename = `scores-${timestamp}-${nameSegment}.json`;
    const payload = {
      eventId,
      eventName,
      pageScores,
      scores,
      deepScrapeUrl,
      generatedAt: new Date().toISOString(),
    };
    await fs.writeFile(path.join(SCORED_URLS_DIR, filename), `${JSON.stringify(payload, null, 2)}\n`, 'utf-8');
  } catch (error) {
    logWarn(`Failed to export scored URLs: ${(error as Error).message}`, eventId);
  }
}

const TASK_NAMES = [
  '1. Identify the official webpage URL',
  '2. Scrape the official webpage',
  '3. Extract dates/times',
  '4. Send scraped content + raw info to LLM',
  '5. Receive LLM response',
  '6. Combine LLM response and raw data into final activity',
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
  logInfo(`Status overview → ${summary}`, eventId);
}

function dedupeEvents(events: SourceEvent[]): SourceEvent[] {
  const eventsById = new Map<string, SourceEvent>();
  for (const event of events) {
    if (!eventsById.has(event.event_id)) {
      eventsById.set(event.event_id, event);
    }
  }
  return Array.from(eventsById.values());
}

const PRICE_REGEX = /£\s*([\d,]+(?:\.\d{1,2})?)/gi;

function normalizeCurrencyValue(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string') {
    const numeric = value.replace(/[^0-9.]/g, '');
    const parsed = Number(numeric);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return null;
}

function parsePricesFromText(text?: string): number[] {
  if (!text) return [];
  const sanitized = text.replace(/\u00A0/g, ' ');
  const matches: number[] = [];
  for (const match of sanitized.matchAll(PRICE_REGEX)) {
    const raw = match[1]?.replace(/,/g, '') ?? '';
    const parsed = Number(raw);
    if (!Number.isNaN(parsed)) {
      matches.push(parsed);
    }
  }
  return matches;
}

function findEventPrice(event: SourceEvent): { value: number | null; source: string | null } {
  let lowest: number | null = null;
  let source: string | null = null;
  const consider = (candidate: number | null, note: string) => {
    if (candidate === null || Number.isNaN(candidate)) return;
    if (candidate < 0) return;
    if (lowest === null || candidate < lowest) {
      lowest = candidate;
      source = note;
    }
  };

  for (const schedule of event.schedules ?? []) {
    for (const ticket of schedule.tickets ?? []) {
      consider(normalizeCurrencyValue(ticket.min_price ?? ticket.max_price), 'datathistle schedule ticket');
    }
    for (const performance of schedule.performances ?? []) {
      for (const ticket of performance.tickets ?? []) {
        consider(normalizeCurrencyValue(ticket.min_price ?? ticket.max_price), 'datathistle performance ticket');
      }
    }
  }

  const descriptionTexts: string[] = [];
  if (Array.isArray(event.descriptions)) {
    for (const entry of event.descriptions) {
      if (entry?.description) {
        descriptionTexts.push(entry.description);
      }
    }
  }
  const candidateDescription = (event as unknown as Record<string, unknown>).description;
  if (typeof candidateDescription === 'string') {
    descriptionTexts.push(candidateDescription);
  }
  for (const description of descriptionTexts) {
    const matches = parsePricesFromText(description);
    if (matches.length) {
      consider(Math.min(...matches), 'datathistle description text');
    }
  }

  return { value: lowest, source };
}

function findPriceFromScrapedPages(pages: PageScrapeResult[]): { value: number | null; source: string | null } {
  for (const page of pages) {
    const fields: Array<{ value?: string; label: string }> = [
      { value: page.structured.extractedPrice, label: 'structured.extractedPrice' },
      { value: page.structured.priceText, label: 'structured.priceText' },
    ];
    for (const field of fields) {
      if (field.value) {
        const matches = parsePricesFromText(field.value);
        if (matches.length) {
          return { value: Math.min(...matches), source: `${field.label} on ${page.url}` };
        }
      }
    }
    const textMatches = parsePricesFromText(page.text);
    if (textMatches.length) {
      return { value: Math.min(...textMatches), source: `page text on ${page.url}` };
    }
  }
  return { value: null, source: null };
}

function mapPriceToLevel(price: number | null): PriceLevel {
  if (price === null) return '££';
  if (price <= 0) return 'free';
  if (price < 20) return '£';
  if (price < 50) return '££';
  return '£££';
}

function determinePriceLevel(event: SourceEvent, pages: PageScrapeResult[]): {
  level: PriceLevel;
  value: number | null;
  source: string;
} {
  const rawCandidate = findEventPrice(event);
  if (rawCandidate.value !== null) {
    return {
      level: mapPriceToLevel(rawCandidate.value),
      value: rawCandidate.value,
      source: rawCandidate.source ?? 'datathistle source data',
    };
  }
  const scrapedCandidate = findPriceFromScrapedPages(pages);
  if (scrapedCandidate.value !== null) {
    return {
      level: mapPriceToLevel(scrapedCandidate.value),
      value: scrapedCandidate.value,
      source: scrapedCandidate.source ?? 'scraped page text',
    };
  }
  return { level: '££', value: null, source: 'fallback default' };
}

function choosePrimaryDateUrl(result: OfficialContentResult, officialUrl: string): { url: string; source: string } {
  const structuredPage = result.dateTimePage;
  const hasJsonHours = Boolean(structuredPage?.structured.jsonLdHours);
  const hasJsonEvents = Boolean(structuredPage?.structured.jsonLdEvents?.length);
  if (structuredPage && (hasJsonHours || hasJsonEvents)) {
    return { url: structuredPage.url, source: 'structured date/time page' };
  }
  if (result.deepScrapeUrl) {
    return { url: result.deepScrapeUrl, source: 'deep scrape' };
  }
  if (structuredPage) {
    return { url: structuredPage.url, source: 'primary date/time page' };
  }
  return { url: officialUrl, source: 'official URL fallback' };
}

function toMinutes(time: string): number {
  const [hour, minute] = time.split(':').map((segment) => Number(segment));
  if (Number.isNaN(hour)) return 0;
  if (Number.isNaN(minute)) return hour * 60;
  return hour * 60 + minute;
}

function applyExceptionNotesToHours(
  exceptions: SharedException[],
  openingHours: Record<string, { open: string; close: string }>,
) {
  for (const ex of exceptions) {
    if (ex.status !== 'open') continue;
    const date = new Date(ex.date);
    if (Number.isNaN(date.getTime())) continue;
    const dayName = DAY_LABELS[date.getDay()];
    openingHours[dayName] = { open: ex.open, close: ex.close };
  }
}

function cleanExceptions(exceptions?: PlaceDates['exceptions']): SharedException[] {
  if (!exceptions || !exceptions.length) return [];
  const futureLimit = new Date();
  futureLimit.setMonth(futureLimit.getMonth() + 6);
  const normalized: SharedException[] = [];
  for (const ex of exceptions) {
    const dateObj = new Date(ex.date);
    if (Number.isNaN(dateObj.getTime())) continue;
    if (dateObj > futureLimit) continue;
    normalized.push({ ...ex, date: dateObj.toISOString().slice(0, 10) });
  }
  if (!normalized.length) return [];
  const deduped = new Map<string, SharedException>();
  for (const ex of normalized) {
    const key = ex.date;
    const existing = deduped.get(key);
    if (!existing) {
      deduped.set(key, ex);
      continue;
    }
    if (existing.status === 'closed' && ex.status === 'open') {
      deduped.set(key, ex);
      continue;
    }
    if (existing.status === 'open' && ex.status === 'closed') {
      continue;
    }
    if (ex.status === 'open' && existing.status === 'open') {
      const currentDuration = toMinutes(ex.close) - toMinutes(ex.open);
      const existingDuration = toMinutes(existing.close) - toMinutes(existing.open);
      if (currentDuration !== existingDuration) {
        if (currentDuration > existingDuration) {
          deduped.set(key, ex);
        }
        continue;
      }
      if (toMinutes(ex.close) > toMinutes(existing.close)) {
        deduped.set(key, ex);
      }
    }
  }
  return Array.from(deduped.values());
}

const CALENDAR_FALLBACK_SCRIPT = `
import sys, json, re, datetime

DAY_LABELS = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']
DAY_MAP = {
  'sun': 'Sun', 'sunday': 'Sun',
  'mon': 'Mon', 'monday': 'Mon',
  'tue': 'Tue', 'tues': 'Tue', 'tuesday': 'Tue',
  'wed': 'Wed', 'weds': 'Wed', 'wednesday': 'Wed',
  'thu': 'Thu', 'thur': 'Thu', 'thurs': 'Thu', 'thursday': 'Thu',
  'fri': 'Fri', 'friday': 'Fri',
  'sat': 'Sat', 'saturday': 'Sat'
}

def parse_time(token):
  token = token.strip().lower()
  if not token:
    return None
  match = re.match(r'(\\d{1,2})(?::(\\d{2}))?\\s*(am|pm)?', token)
  if not match:
    return None
  hour = int(match.group(1))
  minute = int(match.group(2) or 0)
  suffix = match.group(3)
  if suffix == 'pm' and hour < 12:
    hour += 12
  if suffix == 'am' and hour == 12:
    hour = 0
  if hour >= 24:
    hour = hour % 24
  return f"{hour:02d}:{minute:02d}"

def time_to_minutes(value):
  parts = value.split(':')
  return int(parts[0]) * 60 + int(parts[1])

lines = [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]
now = datetime.datetime.now()
year = now.year
month = now.month
exceptions = []
counts = {day: {} for day in DAY_LABELS}
seen = set()

for idx in range(len(lines) - 1):
  header = lines[idx]
  body = lines[idx + 1]
  match = re.match(r'^(\\d{1,2})(?:st|nd|rd|th)?\\s+([A-Za-z]+)', header)
  if not match:
    continue
  day_num = int(match.group(1))
  if day_num < 1 or day_num > 31:
    continue
  dow_key = match.group(2).lower()
  dow_label = DAY_MAP.get(dow_key[:3]) or DAY_MAP.get(dow_key)
  iso_date = f"{year:04d}-{month:02d}-{day_num:02d}"
  if iso_date in seen:
    continue
  seen.add(iso_date)
  if re.search(r'closed', body, re.IGNORECASE):
    exceptions.append({'status': 'closed', 'date': iso_date})
    continue
  parts = re.split(r'[–—\\-]', body)
  if len(parts) < 2:
    continue
  open_time = parse_time(parts[0])
  close_time = parse_time(parts[1])
  if not open_time or not close_time:
    continue
  exceptions.append({'status': 'open', 'date': iso_date, 'open': open_time, 'close': close_time})
  if dow_label:
    key = f"{open_time}-{close_time}"
    counts[dow_label][key] = counts[dow_label].get(key, 0) + 1

opening_hours = {}
for day in DAY_LABELS:
  bucket = counts.get(day, {})
  if not bucket:
    continue
  best_key = max(bucket.items(), key=lambda pair: (pair[1], time_to_minutes(pair[0].split('-')[1])))
  range_key = best_key[0]
  open_time, close_time = range_key.split('-')
  opening_hours[day] = {'open': open_time, 'close': close_time}

print(json.dumps({'exceptions': exceptions, 'opening_hours': opening_hours}))
`;

interface CalendarFallbackResult {
  exceptions: SharedException[];
  openingHours?: Partial<Record<DayLabel, { open: string; close: string }>>;
}

async function runCalendarFallbackParser(
  text: string,
  eventId: string,
): Promise<CalendarFallbackResult | null> {
  return new Promise((resolve) => {
    const child = execFile(
      'python3',
      ['-c', CALENDAR_FALLBACK_SCRIPT],
      { maxBuffer: 8 * 1024 * 1024 },
      (error, stdout) => {
        if (error) {
          logWarn(`Calendar fallback parser failed: ${(error as Error).message}`, eventId);
          return resolve(null);
        }
        if (!stdout) {
          return resolve(null);
        }
        try {
          const parsed = JSON.parse(stdout);
          const rawExceptions = Array.isArray(parsed.exceptions) ? (parsed.exceptions as unknown[]) : [];
          const exceptions = rawExceptions
            .map((entry: unknown) => {
              if (!entry || typeof entry !== 'object') return null;
              const { status, date, open, close } = entry as Record<string, unknown>;
              if (status === 'closed' && typeof date === 'string') {
                return { status: 'closed' as const, date };
              }
              if (status === 'open' && typeof date === 'string' && typeof open === 'string' && typeof close === 'string') {
                return { status: 'open' as const, date, open, close };
              }
              return null;
            })
            .filter((entry): entry is SharedException => Boolean(entry));

          const openingHours: Partial<Record<DayLabel, { open: string; close: string }>> = {};
          const rawHours = parsed.opening_hours;
          if (rawHours && typeof rawHours === 'object') {
            Object.entries(rawHours).forEach(([key, value]) => {
              if (!DAY_LABELS.includes(key as DayLabel)) return;
              if (!value || typeof value !== 'object') return;
              const maybeOpen = (value as Record<string, unknown>).open;
              const maybeClose = (value as Record<string, unknown>).close;
              if (typeof maybeOpen === 'string' && typeof maybeClose === 'string') {
                openingHours[key as DayLabel] = { open: maybeOpen, close: maybeClose };
              }
            });
          }

          resolve({
            exceptions,
            openingHours: Object.keys(openingHours).length ? openingHours : undefined,
          });
        } catch (parseError) {
          logWarn(`Calendar fallback parser output invalid JSON: ${(parseError as Error).message}`, eventId);
          resolve(null);
        }
      },
    );
    if (child.stdin) {
      child.stdin.write(text);
      child.stdin.end();
    }
  });
}

function isSuspiciousPlaceDates(placeDates: PlaceDates): boolean {
  const entries = placeDates.exceptions ?? [];
  if (!entries.length) return false;
  if (entries.length >= 40) return true;
  const closedCount = entries.filter((ex) => ex.status === 'closed').length;
  if (closedCount / entries.length > 0.5) return true;
  if (entries.some((ex) => ex.status === 'open' && ex.open === ex.close)) return true;
  return false;
}

async function maybeApplyCalendarFallback(
  placeDates: PlaceDates,
  page: PageScrapeResult,
  eventId: string,
): Promise<PlaceDates> {
  if (!isSuspiciousPlaceDates(placeDates)) return placeDates;
  const fallback = await runCalendarFallbackParser(page.text, eventId);
  if (!fallback || !fallback.exceptions.length) return placeDates;
  if (!fallback.openingHours || Object.keys(fallback.openingHours).length < 4) return placeDates;
  logDebug(
    `Calendar fallback parser produced ${fallback.exceptions.length} exceptions`,
    eventId,
  );
  return {
    kind: 'place',
    location: placeDates.location,
    openingHours: fallback.openingHours as Record<DayLabel, { open: string; close: string }>,
    exceptions: fallback.exceptions,
  };
}

export async function processEvents(events: SourceEvent[], groqApiKey: string) {
  const activities: Activity[] = [];
  const skipped: { id: string; reason: string }[] = [];
  const uniqueEvents = dedupeEvents(events);
  const totalEvents = uniqueEvents.length;

  for (const [index, event] of uniqueEvents.entries()) {
    logInfo(`Processing event ${index + 1} of ${totalEvents}: ${event.name}`, event.event_id);
    const statuses = createTaskStatuses();
    const logAndContinue = () => logTaskStatuses(event.event_id, statuses);
    let scrapedOfficialUrl: string | null = null;

    const place = event.schedules?.[0]?.place;
    if (!place) {
      setTaskStatus(statuses, 0, 'fail', 'missing location data');
      logAndContinue();
      skipped.push({ id: event.event_id, reason: 'missing location data' });
      continue;
    }

    const coords = await resolveCoordinates(place);
    const addressLine = buildAddressLine(place);
    const areaCandidates = mapAddressToAreas(addressLine);
    const area = areaCandidates[areaCandidates.length - 1] ?? 'Greater London';
    const location: SharedLocation = {
      addressLine,
      postcode: place.postal_code ?? '',
      city: place.town ?? 'London',
      country: 'United Kingdom',
      lat: coords.lat,
      lng: coords.lng,
      area,
    };

    const officialUrl = await resolveOfficialUrl(event);
    if (!officialUrl) {
      setTaskStatus(statuses, 0, 'fail', 'no official descriptive URL found');
      logAndContinue();
      skipped.push({ id: event.event_id, reason: 'no official descriptive URL found' });
      continue;
    }
    setTaskStatus(statuses, 0, 'success', 'URL identified');
    logDebug(`Step 1: identified official URL ${officialUrl}`, event.event_id);
    scrapedOfficialUrl = officialUrl;

    let startUrl = officialUrl;
    try {
      const u = new URL(officialUrl);
      startUrl = `${u.origin}/`;
    } catch {
      // ignore invalid URL
    }
    const result = await getOfficialUrlAndContent(startUrl, event.name, USER_AGENT_STRINGS, location);
    logDebug(
      `preClassifiedType=${result.preClassifiedType ?? 'null'} preExtractedDates=${JSON.stringify(
        result.preExtractedDates ?? null,
      )}`,
      event.event_id,
    );
    await exportScoredUrls(event.event_id, event.name, result.scoredUrls, result.scores, result.deepScrapeUrl);

    if (!result.allScrapedPages.length) {
      setTaskStatus(statuses, 1, 'fail', 'official page scrape failed');
      logDebug('Step 2: scraping failed for official URL', event.event_id);
      logWarn(`Scrape failed for ${officialUrl}`, event.event_id);
      logAndContinue();
      skipped.push({ id: event.event_id, reason: 'official page scrape failed' });
      continue;
    }
    setTaskStatus(statuses, 1, 'success', 'official pages scraped');
    logDebug(
      `Step 2: scraped official content (${result.allScrapedPages.length} page(s), ${result.allScrapedPages[0].text.length} chars first page)`,
      event.event_id,
    );
    for (const [index, page] of result.allScrapedPages.entries()) {
      await exportScrapedContent(event.event_id, event.name, page, index);
    }

    if (!result.dateTimePage) {
      setTaskStatus(statuses, 2, 'fail', 'no high-quality date/time page');
      logAndContinue();
      skipped.push({
        id: event.event_id,
        reason: 'no high-quality date/time page found in top 3 hour-scored candidates',
      });
      continue;
    }
    if (!result.preClassifiedType || !result.preExtractedDates) {
      setTaskStatus(statuses, 2, 'fail', 'no valid dates/times extracted');
      logWarn(
        'Excluding activity: no valid dates or times could be extracted (neither place hours nor event instances)',
        event.event_id,
      );
      skipped.push({
        id: event.event_id,
        reason: 'no date/time information available',
      });
      logAndContinue();
      continue;
    }
    if (result.dateTimePage && result.preClassifiedType === 'place') {
      const refined = await maybeApplyCalendarFallback(
        result.preExtractedDates as PlaceDates,
        result.dateTimePage,
        event.event_id,
      );
      result.preExtractedDates = refined;
      result.preClassifiedType = 'place';
    }
    setTaskStatus(statuses, 2, 'success', 'dates/times extracted');
    logDebug('Step 3: extracted dates/times from official pages', event.event_id);

    if (isBookingDomain(officialUrl)) {
      setTaskStatus(statuses, 3, 'fail', 'official URL is a booking domain');
      logAndContinue();
      skipped.push({ id: event.event_id, reason: 'official URL points to booking domain' });
      continue;
    }

    setTaskStatus(statuses, 3, 'success', 'Groq request sent');
    logDebug('Step 4: sending scraped content to Groq', event.event_id);
    const generalPagesList = [...result.generalPages];
    if (generalPagesList.length < 2) {
      for (const page of result.allScrapedPages) {
        if (page.url === result.dateTimePage?.url) continue;
        if (generalPagesList.some((candidate) => candidate.url === page.url)) continue;
        generalPagesList.push(page);
        if (generalPagesList.length >= 2) break;
      }
    }
    const pagesForLLM = [result.dateTimePage, ...generalPagesList.slice(0, 2)];
    const llmData = await fetchLLMDataWithRetries(event, pagesForLLM, groqApiKey);
    if (!llmData) {
      setTaskStatus(statuses, 4, 'fail', 'LLM returned null (all attempts failed)');
      logAndContinue();
      skipped.push({ id: event.event_id, reason: 'LLM failed after retries' });
      continue;
    }

    if (!llmData.contentMatchesDescription) {
      setTaskStatus(statuses, 4, 'fail', 'LLM mismatch in content');
      logAndContinue();
      skipped.push({
        id: event.event_id,
        reason: 'scraped content does not match raw description',
      });
      continue;
    }
    setTaskStatus(statuses, 4, 'success', 'Groq response valid');
    logDebug('Step 5: received LLM response', event.event_id);

    const primaryDateInfo = choosePrimaryDateUrl(result, officialUrl);
    logDebug(
      `Primary date/time source resolved to ${primaryDateInfo.source} (${primaryDateInfo.url})`,
      event.event_id,
    );

    try {
      const canonicalDates: Dates =
        result.preClassifiedType === 'place'
          ? ({
              ...(result.preExtractedDates as PlaceDates),
              kind: 'place',
            } as PlaceDates)
          : ({
              ...(result.preExtractedDates as EventDates),
              kind: 'event',
            } as EventDates);
      const activity = await buildActivity(
        event,
        llmData,
        officialUrl,
        pagesForLLM,
        scrapedOfficialUrl,
        canonicalDates,
        result.preClassifiedType,
        primaryDateInfo.url,
        primaryDateInfo.source,
      );
      if (!activity) {
        setTaskStatus(statuses, 5, 'fail', 'no valid dates');
        logAndContinue();
        skipped.push({ id: event.event_id, reason: 'no valid dates extracted' });
        continue;
      }
      activitySchema.parse(activity);
      activities.push(activity);
      setTaskStatus(statuses, 5, 'success', 'final activity constructed');
      logDebug('Step 6: combined LLM response and raw data into final activity', event.event_id);
      logAndContinue();
    } catch (error) {
      setTaskStatus(statuses, 5, 'fail', 'validation failed');
      logAndContinue();
      skipped.push({
        id: event.event_id,
        reason: `validation failed: ${(error as Error).message}`,
      });
      logWarn(`Validation failed: ${(error as Error).message}`, event.event_id);
    }
  }

  return { activities, skipped };
}

async function buildActivity(
  event: SourceEvent,
  llmData: ActivityLLMOutput,
  officialUrl: string,
  scrapedPages: PageScrapeResult[],
  scrapedOfficialUrl: string | null,
  dates: Dates,
  preClassifiedType: 'place' | 'event',
  primaryDateUrl: string,
  primaryDateSource: string,
): Promise<Activity | null> {
  const imageUrl = event.images?.[0]?.url;

  const finalUrl = primaryDateUrl || scrapedOfficialUrl || officialUrl;
  const priceInfo = determinePriceLevel(event, scrapedPages);

  const typeSuffix = preClassifiedType;
  const baseActivity = {
    id: `${event.event_id}--${typeSuffix}`,
    name: event.name,
    summary: llmData.summary,
    priceLevel: '££' as PriceLevel,
    age: {
      officialAgeAvailable: llmData.officialAgeAvailable,
      minAge: llmData.minAge,
      maxAge: llmData.maxAge,
    },
    url: finalUrl,
    source: 'datathistle',
    lastUpdate: new Date().toISOString(),
    keywords: llmData.keywords,
    labels: llmData.labels,
    imageUrl,
  };

  baseActivity.priceLevel = priceInfo.level;
  baseActivity.url = finalUrl;
  logDebug(
    `[PRICE] level=${priceInfo.level} value=${priceInfo.value ?? 'n/a'} source=${priceInfo.source}`,
    event.event_id,
  );
  logDebug(`[URL] resolved to ${finalUrl} (${primaryDateSource})`, event.event_id);

  if (preClassifiedType === 'place') {
    const placeDates = dates as PlaceDates;
    if (placeDates.exceptions?.length) {
      applyExceptionNotesToHours(placeDates.exceptions, placeDates.openingHours);
      const cleaned = cleanExceptions(placeDates.exceptions);
      placeDates.exceptions = cleaned;
      if (cleaned.length) {
        const total = cleaned.length;
        const hoursCount = cleaned.filter((ex) => ex.status === 'open').length;
        if (total > 40 || hoursCount / total < 0.05) {
          placeDates.exceptions = [];
          logWarn('Exceptions too noisy; discarding', event.event_id);
        }
      }
    }
  }

  return { ...baseActivity, dates };
}
