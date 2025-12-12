import { parseChronoDate, parseChronoSegments } from './chrono-wrapper.js';
import type { ParsedResult } from 'chrono-node';
import { DAY_LABELS } from './types.js';
import type {
  DayLabel,
  JsonLdEvent,
  JsonLdHoursResult,
  PageScrapeResult,
  RawDateTimeInstance,
  SourcePlace,
} from './types.js';
import type {
  Dates,
  EventDates,
  Exception as SharedException,
  Location as SharedLocation,
  PlaceDates,
} from '../../london-kids-p1/packages/shared/src/activity.js';

export const TIMEOUT_MS = 30_000;
export const RETRY_DELAY_MS = 2_000;
export const USER_AGENT_STRINGS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
];

export function normalizeActivityForHostname(name: string): string | null {
  const cleaned = name
    .toLowerCase()
    .replace(/[’'‘`]/g, '')
    .replace(/\blondon\b/g, '')
    .replace(/[^a-z0-9]+/g, '');
  return cleaned.length >= 3 ? cleaned : null;
}

const bookingPatterns = [
  /datathistle\.com/i,
  /ticketmaster\.co\.uk/i,
  /seetickets\.com/i,
  /eventbrite\.com\/e/i,
  /ticketweb\.com/i,
  /skiddle\.com/i,
  /getmein\.com/i,
  /tiqets\.com/i,
];

const DEBUG_ENABLED = /^true$/i.test(process.env.DEBUG ?? '');

function buildLogPrefix(level: 'INFO' | 'DEBUG' | 'WARN' | 'ERROR', scope?: string): string {
  const label = scope && scope.trim().length ? scope.trim() : 'main';
  return `[${label}] [${level}]`;
}

export function logInfo(message: string, scope?: string): void {
  console.log(`${buildLogPrefix('INFO', scope)} ${message}`);
}

export function logDebug(message: string, scope?: string): void {
  if (!DEBUG_ENABLED) return;
  console.log(`${buildLogPrefix('DEBUG', scope)} ${message}`);
}

export function logWarn(message: string, scope?: string): void {
  console.warn(`${buildLogPrefix('WARN', scope)} ${message}`);
}

export function logError(message: string, scope?: string): void {
  console.error(`${buildLogPrefix('ERROR', scope)} ${message}`);
}

export const DEBUG_MODE = DEBUG_ENABLED;

const SCHEDULE_INCLUDE_KEYWORDS =
  /open|close|hours|time|schedule|calendar|visit|operating|session|tour|performance|season|daily|weekly|from.*to|until/i;
const SCHEDULE_EXCLUDE_PATTERNS =
  /expire|return|refund|policy|download|link|warranty|cancellation|terms|conditions|faq|privacy|booking|ticket|purchase|post|mail|discount|\bday\b|working/i;

export function extractRelevantSections(text: string): string[] {
  const lines = text.split(/\r?\n/);
  const sections: string[] = [];
  let buffer: string[] = [];
  const flushBuffer = () => {
    if (!buffer.length) return;
    sections.push(buffer.join(' ').trim());
    buffer = [];
  };
  const isDayTimeLine = (line: string): boolean => {
    const normalized = line.toLowerCase();
    if (SCHEDULE_EXCLUDE_PATTERNS.test(normalized)) return false;
    if (SCHEDULE_INCLUDE_KEYWORDS.test(normalized)) return true;
    return /^(mon|tue|wed|thu|fri|sat|sun)/i.test(line) && /\d{1,2}(:\d{2})?.*(am|pm)?/.test(line);
  };

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) {
      flushBuffer();
      continue;
    }
    if (SCHEDULE_EXCLUDE_PATTERNS.test(line)) {
      flushBuffer();
      continue;
    }
    if (isDayTimeLine(line) || buffer.length) {
      buffer.push(line);
      continue;
    }
    if (SCHEDULE_INCLUDE_KEYWORDS.test(line)) {
      buffer.push(line);
      continue;
    }
  }
  flushBuffer();
  return sections.filter((section) => Boolean(section));
}

export async function isUrlValidAndHtml(url: string): Promise<boolean> {
  try {
    const response = await fetch(url, {
      method: 'HEAD',
      signal: AbortSignal.timeout(7000),
      redirect: 'follow',
    });
    const contentType = response.headers.get('content-type') ?? '';
    return response.ok && contentType.toLowerCase().includes('text/html');
  } catch {
    return false;
  }
}

export function quickPreScoreUrl(url: string): number {
  const lower = url.toLowerCase();
  let score = 0;
  const highValue = [
    'opening-hours',
    'opening-times',
    'hours',
    'times',
    'dates',
    'schedule',
    'calendar',
    'faqs',
    'facilities',
  ];
  const mediumValue = [
    'plan-your-visit',
    'before-you-visit',
    'visit-us',
    'visitor-information',
    'tickets',
    'prices',
  ];
  highValue.forEach((kw) => {
    if (lower.includes(kw)) score += 300;
  });
  mediumValue.forEach((kw) => {
    if (lower.includes(kw)) score += 100;
  });
  const depth = (url.match(/\//g) || []).length;
  score -= Math.max(0, depth - 4) * 50;
  return Math.max(0, score);
}

export function generateSmartHoursGuesses(rootText: string, rootUrl: string): string[] {
  let base: string;
  try {
    base = new URL(rootUrl).origin;
  } catch {
    return [];
  }
  const lowerText = rootText.toLowerCase();
  const segments: string[] = [];
  if (lowerText.includes('plan your visit') || lowerText.includes('plan-your-visit')) {
    segments.push('plan-your-visit');
  }
  if (lowerText.includes('before you visit') || lowerText.includes('before-you-visit')) {
    segments.push('before-you-visit');
  }
  if (lowerText.includes('visit us')) {
    segments.push('visit-us');
  }
  if (lowerText.includes('visitor information')) {
    segments.push('visitor-information');
  }
  const suffixes = ['/opening-hours/', '/opening-times/', '/hours/', '/times/'];
  const guesses = new Set<string>();
  const baseSegments = segments.length ? segments : [''];
  baseSegments.forEach((segment) => {
    suffixes.forEach((suffix) => {
      const path = `${segment}${segment && !segment.endsWith('/') ? '/' : ''}${suffix.replace(/^\//, '')}`;
      try {
        guesses.add(new URL(path, base).href);
      } catch {
        // ignore invalid
      }
    });
  });
  ['/opening-hours/', '/opening-times/', '/hours/'].forEach((suffix) => {
    try {
      guesses.add(new URL(suffix, base).href);
    } catch {
      // ignore
    }
  });
  return Array.from(guesses);
}

export type DateTimeFormatType = 'table' | 'grid' | 'list' | 'text-blocks' | 'js-dynamic' | 'unknown';

export function cleanText(text: string): string {
  const collapsed = text
    .replace(/[\t\r]/g, ' ')
    .replace(/\s*\n\s*/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/ {2,}/g, ' ')
    .trim();
  return collapsed;
}

export interface CalendarParseResult {
  exceptions: SharedException[];
  openingHours?: Partial<Record<DayLabel, { open: string; close: string }>>;
}

function timeToMinutes(time: string): number {
  const [hour, minute] = time.split(':').map((segment) => Number(segment));
  if (Number.isNaN(hour)) return 0;
  if (Number.isNaN(minute)) return hour * 60;
  return hour * 60 + minute;
}

function compareHourRanges(range1: string, range2: string): number {
  if (!range1 && !range2) return 0;
  if (!range1) return -1;
  if (!range2) return 1;
  const [open1, close1] = range1.split('-');
  const [open2, close2] = range2.split('-');
  if (!open1 || !close1) return -1;
  if (!open2 || !close2) return 1;
  const duration1 = timeToMinutes(close1) - timeToMinutes(open1);
  const duration2 = timeToMinutes(close2) - timeToMinutes(open2);
  if (duration1 !== duration2) {
    return duration1 > duration2 ? 1 : -1;
  }
  const closeMinutes1 = timeToMinutes(close1);
  const closeMinutes2 = timeToMinutes(close2);
  if (closeMinutes1 !== closeMinutes2) {
    return closeMinutes1 > closeMinutes2 ? 1 : -1;
  }
  return 0;
}

export function computeModalOpeningHours(
  instances: RawDateTimeInstance[],
): Record<DayLabel, { open: string; close: string }> | undefined {
  const groups: Record<DayLabel, Map<string, number>> = {
    Sun: new Map(),
    Mon: new Map(),
    Tue: new Map(),
    Wed: new Map(),
    Thu: new Map(),
    Fri: new Map(),
    Sat: new Map(),
  };
  for (const instance of instances) {
    if (!instance.date || !instance.startTime || !instance.endTime) continue;
    const dateObj = new Date(instance.date);
    if (Number.isNaN(dateObj.getTime())) continue;
    const day = DAY_LABELS[dateObj.getDay()];
    const hoursKey = `${instance.startTime}-${instance.endTime}`;
    const count = groups[day].get(hoursKey) ?? 0;
    groups[day].set(hoursKey, count + 1);
  }

  const openingHours: Partial<Record<DayLabel, { open: string; close: string }>> = {};
  for (const day of DAY_LABELS) {
    const freqMap = groups[day];
    if (!freqMap.size) continue;
    let bestKey: string | undefined;
    let bestCount = 0;
    for (const [key, count] of freqMap.entries()) {
      if (count > bestCount || (count === bestCount && compareHourRanges(key, bestKey ?? '') > 0)) {
        bestCount = count;
        bestKey = key;
      }
    }
    if (bestKey) {
      const [open, close] = bestKey.split('-');
      if (open && close) {
        openingHours[day] = { open, close };
      }
    }
  }

  const keys = Object.keys(openingHours);
  if (!keys.length) {
    return undefined;
  }
  return openingHours as Record<DayLabel, { open: string; close: string }>;
}

function formatTimeFromDate(date: Date): string {
  const hours = date.getHours();
  const minutes = date.getMinutes();
  const pad = (value: number) => value.toString().padStart(2, '0');
  return `${pad(hours)}:${pad(minutes)}`;
}

function dayLabelFromDate(date: Date): string {
  return DAY_LABELS[date.getDay()];
}

const MONTH_REGEX = /\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i;
const YEAR_REGEX = /\b(19|20)\d{2}\b/;
const DAY_REGEX = /\b(?:mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)\b/i;
const DAILY_REGEX = /\b(daily|every day|open daily|open every day)\b/i;

export function extractDatesAndTimesFromPage(
  text: string,
  url: string,
  options?: { referenceDate?: Date; jsonLdHours?: JsonLdHoursResult },
):
  | {
      source: 'chrono';
      openingHours?: Record<'Mon' | 'Tue' | 'Wed' | 'Thu' | 'Fri' | 'Sat' | 'Sun', { open: string; close: string }>;
      eventInstances?: Array<{ date: string; startTime: string; endTime?: string }>;
      note?: string;
      derivedExceptions?: Array<{ date: string; hours?: { open: string; close: string }; note?: string }>;
    }
  | {
      source: 'jsonld';
      openingHours: Record<'Mon' | 'Tue' | 'Wed' | 'Thu' | 'Fri' | 'Sat' | 'Sun', { open: string; close: string }>;
      derivedExceptions?: SharedException[];
      note?: string;
    }
  | { source: 'none' } {
  const referenceDate = options?.referenceDate ?? new Date();
  const jsonLdHours = options?.jsonLdHours;

  if (jsonLdHours) {
    const openDays = Object.fromEntries(
      Object.entries(jsonLdHours.hours)
        .filter(([, value]) => typeof value === 'object' && 'open' in (value as { open?: string; close?: string }) && 'close' in (value as { open?: string; close?: string }))
        .map(([day, value]) => [
          day,
          {
            open: (value as { open: string }).open,
            close: (value as { close: string }).close,
          },
        ]),
    );
    if (Object.keys(openDays).length >= 4) {
      return {
        source: 'jsonld',
        openingHours: openDays as Record<'Mon' | 'Tue' | 'Wed' | 'Thu' | 'Fri' | 'Sat' | 'Sun', { open: string; close: string }>,
        derivedExceptions: jsonLdHours.exceptions,
        note: `from JSON-LD ${url}`,
      };
    }
  }

  const defaultSegments = text
    .split(/\n{1,2}/)
    .map((segment) => segment.trim())
    .filter((segment) => segment.length > 0);
  const relevantSections = extractRelevantSections(text);
  const fallbackSegments = defaultSegments.length ? defaultSegments : [cleanText(text)];

  const runChrono = (segmentsToParse: string[]) => {
    const groupedResults = new Map<string, ParsedResult[]>();
    const addResults = (segment: string, results: ParsedResult[]) => {
      if (!results.length) return;
      const existing = groupedResults.get(segment) ?? [];
      groupedResults.set(segment, existing.concat(results));
    };

    const openingHours: Record<string, { open: string; close: string }> = {};
    const calendarLines = text.split('\n').map((l) => l.trim()).filter(Boolean);
    const monthYearMatch = text.match(
      /(January|February|March|April|May|June|July|August|September|October|November|December)\s+20\d{2}/i,
    );
    const refDate = monthYearMatch ? new Date(monthYearMatch[0]) : referenceDate;

    const dayMap: Record<string, string> = {
      Mon: 'Mon',
      Monday: 'Mon',
      Tue: 'Tue',
      Tuesday: 'Tue',
      Wed: 'Wed',
      Wednesday: 'Wed',
      Thu: 'Thu',
      Thursday: 'Thu',
      Fri: 'Fri',
      Friday: 'Fri',
      Sat: 'Sat',
      Saturday: 'Sat',
      Sun: 'Sun',
      Sunday: 'Sun',
    };

    for (const line of calendarLines) {
      const lower = line.toLowerCase();
      const timeMatch = line.match(/(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s*[-–—]\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)/i);
      const closedMatch = /closed/i.test(lower);
      const dayMatch = line.match(/(Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?/i);

      if ((timeMatch || closedMatch) && dayMatch) {
        const dayAbbr = dayMap[dayMatch[0]];
        if (!openingHours[dayAbbr]) {
          if (closedMatch) {
            openingHours[dayAbbr] = { open: '00:00', close: '00:00' };
          } else if (timeMatch) {
            const [openRaw, closeRaw] = timeMatch.slice(1);
            openingHours[dayAbbr] = { open: normalizeTime(openRaw), close: normalizeTime(closeRaw) };
          }
        }
      }
    }

    function normalizeTime(t: string): string {
      const parsed = parseChronoDate(t, refDate);
      if (parsed) {
        return formatTimeFromDate(parsed);
      }
      let normalized = t.trim().toLowerCase();
      if (!normalized.includes(':')) {
        normalized = normalized.replace(/(\d{1,2})(\d{2})/, '$1:$2');
      }
      return normalized.padStart(5, '0');
    }

    for (const segment of segmentsToParse) {
      const results = parseChronoSegments(segment, refDate);
      addResults(segment, results);
    }

    const fallbackResults = parseChronoSegments(text, refDate);
    for (const result of fallbackResults) {
      addResults(result.text, [result]);
    }

    if (!groupedResults.size && Object.keys(openingHours).length === 0) {
      return { source: 'none' as const };
    }

    const eventInstances: Array<{ date: string; startTime: string; endTime?: string; note?: string }> = [];

    for (const [segment, results] of groupedResults.entries()) {
      const snippet = segment.toLowerCase();
      const timeResult = results.find(
        (result) => result.end && (result.start.isCertain('hour') || result.start.get('hour') !== null),
      );
      const dayResults = results.filter((result) => DAY_REGEX.test(result.text.toLowerCase()));
      const isDailyPattern = DAILY_REGEX.test(snippet);

      if (timeResult && (dayResults.length > 0 || isDailyPattern)) {
        const open = formatTimeFromDate(timeResult.start.date());
        const close = timeResult.end ? formatTimeFromDate(timeResult.end.date()) : open;
        const days = isDailyPattern
          ? DAY_ORDER
          : expandDayRange(
              dayLabelFromDate(dayResults[0].start.date()),
              dayResults.length > 1
                ? dayLabelFromDate(dayResults[dayResults.length - 1].start.date())
                : dayLabelFromDate(dayResults[0].start.date()),
            );
        for (const day of days) {
          if (!openingHours[day]) {
            openingHours[day] = { open, close };
          }
        }
        continue;
      }

      for (const result of results) {
        const startDate = result.start.date();
        if (!startDate) continue;
        const startTime = formatTimeFromDate(startDate);
        if (!startTime) continue;
        const endDate = result.end?.date();
        const endTime = endDate ? formatTimeFromDate(endDate) : undefined;

        const snippetWithResult = `${snippet} ${result.text.toLowerCase()}`;
        const hasMonth = MONTH_REGEX.test(snippetWithResult);
        const hasYear = YEAR_REGEX.test(snippetWithResult) || result.start.isCertain('year');
        const hasDay = DAY_REGEX.test(snippetWithResult);
        const hasDaily = DAILY_REGEX.test(snippetWithResult);

        const isSpecificDate = hasMonth || hasYear;
        const isRecurringPattern = (hasDay && !isSpecificDate) || hasDaily;

        if (isRecurringPattern && endTime) {
          const startDay = dayLabelFromDate(startDate);
          const endDay = endDate ? dayLabelFromDate(endDate) : startDay;
          const days = expandDayRange(startDay, endDay);
          for (const day of days) {
            if (!openingHours[day]) {
              openingHours[day] = { open: startTime, close: endTime };
            }
          }
          continue;
        }

        const instance = {
          date: startDate.toISOString().split('T')[0],
          startTime,
          note: snippet,
        } as {
          date: string;
          startTime: string;
          endTime?: string;
          note?: string;
        };
        if (endTime) {
          instance.endTime = endTime;
        }
        eventInstances.push(instance);
      }
    }

    const meaningfulCount = Object.keys(openingHours).length + eventInstances.length;
    if (meaningfulCount < 2) {
      return { source: 'none' as const };
    }

    const dayOfWeekTimes: Record<DayLabel, string[]> = Object.fromEntries(
      DAY_LABELS.map((day) => [day, [] as string[]]),
    ) as Record<DayLabel, string[]>;
    for (const instance of eventInstances) {
      if (!instance.startTime || !instance.endTime) continue;
      const dt = new Date(`${instance.date}T00:00:00`);
      if (Number.isNaN(dt.getTime())) continue;
      const dow = DAY_LABELS[dt.getDay()];
      dayOfWeekTimes[dow].push(`${instance.startTime}-${instance.endTime}`);
    }

    const patternHours: Partial<Record<DayLabel, { open: string; close: string }>> = {};
    for (const day of DAY_LABELS) {
      const times = dayOfWeekTimes[day];
      if (!times.length) continue;
      const freq = new Map<string, number>();
      for (const timeRange of times) {
        freq.set(timeRange, (freq.get(timeRange) ?? 0) + 1);
      }
      const sorted = Array.from(freq.entries()).sort((a, b) => b[1] - a[1]);
      const mostCommon = sorted[0]?.[0];
      if (!mostCommon) continue;
      const [open, close] = mostCommon.split('-');
      if (open && close) {
        patternHours[day] = { open, close };
      }
    }

    for (const [day, hours] of Object.entries(patternHours)) {
      const key = day as DayLabel;
      if (!openingHours[key]) {
        openingHours[key] = hours;
      }
    }

    const derivedExceptions = eventInstances
      .filter((inst) => {
        const dt = new Date(`${inst.date}T00:00:00`);
        if (Number.isNaN(dt.getTime())) return false;
        const dow = DAY_LABELS[dt.getDay()];
        const pattern = openingHours[dow];
        const instRange = inst.startTime && inst.endTime ? `${inst.startTime}-${inst.endTime}` : null;
        const patternRange = pattern ? `${pattern.open}-${pattern.close}` : null;
        return (
          !pattern ||
          (instRange && patternRange !== instRange) ||
          (inst.note && inst.note.toLowerCase().includes('closed'))
        );
      })
      .map((inst) => ({
        date: inst.date,
        hours: inst.startTime
          ? { open: inst.startTime, close: inst.endTime ?? inst.startTime }
          : undefined,
        note: inst.note,
      }));

    const modalHours = computeModalOpeningHours(eventInstances);
    if (modalHours) {
      for (const day of DAY_LABELS) {
        delete openingHours[day];
      }
      for (const [day, hours] of Object.entries(modalHours)) {
        openingHours[day as DayLabel] = hours;
      }
      logDebug(`[HOURS MODAL] Computed modal openingHours for ${url}`, 'hours-track');
    }

    const payload: {
      source: 'chrono';
      openingHours?: Record<'Mon' | 'Tue' | 'Wed' | 'Thu' | 'Fri' | 'Sat' | 'Sun', { open: string; close: string }>;
      eventInstances?: Array<{ date: string; startTime: string; endTime?: string }>;
      note?: string;
      derivedExceptions?: Array<{ date: string; hours?: { open: string; close: string }; note?: string }>;
    } = { source: 'chrono', note: `from ${url}` };

    if (Object.keys(openingHours).length) {
      payload.openingHours = openingHours as Record<'Mon' | 'Tue' | 'Wed' | 'Thu' | 'Fri' | 'Sat' | 'Sun', { open: string; close: string }>;
    }
    if (eventInstances.length) {
      payload.eventInstances = eventInstances;
    }
    if (derivedExceptions.length) {
      payload.derivedExceptions = derivedExceptions;
    }
    return payload;
  };

  const initialSegments = relevantSections.length ? relevantSections : fallbackSegments;
  let payload = runChrono(initialSegments);
  if (relevantSections.length && payload.source === 'chrono' && (payload.eventInstances?.length ?? 0) < 3) {
    payload = runChrono(fallbackSegments);
  } else if (!relevantSections.length) {
    logWarn(`[LOW CONFIDENCE] No focused schedule sections detected on ${url}`, url);
  }

  return payload.source === 'none' ? { source: 'none' } : payload;
}

export function hasActualTimeInfo(page: PageScrapeResult): boolean {
  const parsed = extractDatesAndTimesFromPage(page.text, page.url, {
    jsonLdHours: page.structured.jsonLdHours,
  });

  if (parsed.source === 'none') {
    logDebug(`[HOURS TRACK] Rejected ${page.url} – no chrono parsing at all`, 'hours-track');
    return false;
  }

  if (parsed.openingHours) {
    const validDays = Object.values(parsed.openingHours).filter(
      (h) =>
        h.open &&
        h.close &&
        h.open !== h.close &&
        h.open !== '00:00' &&
        h.close !== '00:00' &&
        h.open !== 'closed' &&
        h.close !== 'closed',
    );
    if (validDays.length >= 2) {
      logDebug(
        `[HOURS TRACK] Accepted ${page.url} – ${validDays.length} valid opening hours days`,
        'hours-track',
      );
      return true;
    }
    logDebug(
      `[HOURS TRACK] Rejected ${page.url} – only ${validDays.length} valid opening hours days`,
      'hours-track',
    );
  }

  if (parsed.source === 'chrono' && parsed.eventInstances && parsed.eventInstances.length > 0) {
    const withTime = parsed.eventInstances.some((inst) => Boolean(inst.startTime));
    if (withTime) {
      logDebug(`[HOURS TRACK] Accepted ${page.url} – has event instances with times`, 'hours-track');
      return true;
    }
  }

  logDebug(`[HOURS TRACK] Rejected ${page.url} – parsed something but no structured hours/events`, 'hours-track');
  return false;
}

function stripHtmlTags(value: string): string {
  if (!value) return '';
  return value
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function gatherDeepSegments(page: PageScrapeResult): string[] {
  const segments = new Set<string>();
  const addSegment = (value?: string) => {
    if (!value) return;
    const cleaned = cleanText(value);
    if (cleaned) {
      segments.add(cleaned);
    }
  };
  addSegment(page.text);
  addSegment(page.structured.description);
  addSegment(page.structured.openingHoursText);
  addSegment(page.structured.addressText);
  const html = page.html ?? '';
  const matches = [
    ...html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi),
    ...html.matchAll(/<li[^>]*>([\s\S]*?)<\/li>/gi),
    ...html.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/gi),
  ];
  for (const match of matches) {
    addSegment(stripHtmlTags(match[1]));
  }
  for (const line of page.text.split('\n')) {
    addSegment(line);
  }
  return Array.from(segments).slice(0, 400);
}

export function ensureIsoDate(value: string | Date): string | null {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const pad = (num: number) => num.toString().padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

export function parseTimeTo24h(timeStr: string | undefined): string | null {
  if (!timeStr) return null;
  const trimmed = timeStr.trim().toLowerCase();
  const match = trimmed.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/);
  if (!match) return null;
  let [, hourStr, minuteStr, ampm] = match;
  const hour = parseInt(hourStr, 10);
  if (Number.isNaN(hour)) return null;
  let minutes = minuteStr ? parseInt(minuteStr, 10) : 0;
  if (Number.isNaN(minutes)) minutes = 0;
  let normalizedHour = hour;
  if (ampm === 'pm' && normalizedHour < 12) {
    normalizedHour += 12;
  }
  if (ampm === 'am' && normalizedHour === 12) {
    normalizedHour = 0;
  }
  if (normalizedHour >= 24) {
    normalizedHour = normalizedHour % 24;
  }
  return `${normalizedHour.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`;
}

function parseCalendarGrid(text: string, referenceDate: Date): RawDateTimeInstance[] {
  const gridMatch = text.match(/MonTueWedThuFriSatSun\s*((?:\d+\s+\w+\n(?:Closed|.+?am\s*-\s*.+?pm)\n?)+)/i);
  if (!gridMatch) return [];
  const entries: RawDateTimeInstance[] = [];
  const pattern = /(\d+)\s+\w+\n(Closed|.+?am\s*-\s*.+?pm)/gi;
  let match;
  const month = referenceDate.getMonth() + 1;
  const year = referenceDate.getFullYear();
  while ((match = pattern.exec(gridMatch[1]))) {
    const [, dayNum, status] = match;
    const isoDay = dayNum.toString().padStart(2, '0');
    const isoDate = `${year}-${month.toString().padStart(2, '0')}-${isoDay}`;
    if (/closed/i.test(status)) {
      entries.push({ date: isoDate, note: 'Closed' });
      continue;
    }
    const times = status.split(/[-–]/).map((item) => item.trim());
    if (times.length < 2) continue;
    const start = parseTimeTo24h(times[0]);
    const end = parseTimeTo24h(times[1]);
    if (start && end) {
      entries.push({ date: isoDate, startTime: start, endTime: end, note: status });
    }
  }
  return entries;
}

const DAY_NAME_TO_LABEL: Record<string, DayLabel> = {
  mon: 'Mon',
  monday: 'Mon',
  tue: 'Tue',
  tues: 'Tue',
  tuesday: 'Tue',
  wed: 'Wed',
  wednesday: 'Wed',
  thu: 'Thu',
  thur: 'Thu',
  thursday: 'Thu',
  fri: 'Fri',
  friday: 'Fri',
  sat: 'Sat',
  saturday: 'Sat',
  sun: 'Sun',
  sunday: 'Sun',
};

export function extractCalendarExceptionsFromText(
  text: string,
  yearMonth?: string,
): CalendarParseResult {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const now = new Date();
  let targetYear = now.getFullYear();
  let targetMonth = now.getMonth() + 1;
  if (yearMonth) {
    const match = yearMonth.match(/^(\d{4})-(\d{2})$/);
    if (match) {
      targetYear = Number(match[1]);
      targetMonth = Number(match[2]);
      if (Number.isNaN(targetMonth) || targetMonth < 1 || targetMonth > 12) {
        targetMonth = now.getMonth() + 1;
      }
    }
  }
  const isoMonth = targetMonth.toString().padStart(2, '0');
  const exceptions: SharedException[] = [];
  const weeklyBuckets: Record<DayLabel, Map<string, number>> = {
    Sun: new Map(),
    Mon: new Map(),
    Tue: new Map(),
    Wed: new Map(),
    Thu: new Map(),
    Fri: new Map(),
    Sat: new Map(),
  };
  const seenDates = new Set<string>();
  for (let index = 0; index < lines.length - 1; index += 1) {
    const header = lines[index];
    const nextLine = lines[index + 1];
    const match = header.match(/^(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)/);
    if (!match) continue;
    const dayNumber = Number(match[1]);
    if (Number.isNaN(dayNumber) || dayNumber < 1 || dayNumber > 31) continue;
    const isoDate = `${targetYear}-${isoMonth}-${dayNumber.toString().padStart(2, '0')}`;
    if (seenDates.has(isoDate)) continue;
    seenDates.add(isoDate);
    const cleanedNext = nextLine.trim();
    if (/closed/i.test(cleanedNext)) {
      exceptions.push({ status: 'closed', date: isoDate });
      continue;
    }
    const timeParts = cleanedNext.split(/[-–—]/).map((segment) => segment.trim()).filter(Boolean);
    if (timeParts.length < 2) continue;
    const open = parseTimeTo24h(timeParts[0]);
    const close = parseTimeTo24h(timeParts[1]);
    if (!open || !close) continue;
    exceptions.push({ status: 'open', date: isoDate, open, close });
    const dayKey = DAY_NAME_TO_LABEL[(match[2] ?? '').toLowerCase()];
    if (dayKey) {
      const key = `${open}-${close}`;
      const bucket = weeklyBuckets[dayKey];
      bucket.set(key, (bucket.get(key) ?? 0) + 1);
    }
  }

  const openingHours: Partial<Record<DayLabel, { open: string; close: string }>> = {};
  for (const day of DAY_LABELS) {
    const bucket = weeklyBuckets[day];
    if (!bucket.size) continue;
    const [bestRange] = Array.from(bucket.entries()).sort((a, b) => b[1] - a[1]);
    if (!bestRange) continue;
    const [open, close] = bestRange[0].split('-');
    if (open && close) {
      openingHours[day] = { open, close };
    }
  }

  return {
    exceptions,
    openingHours: Object.keys(openingHours).length ? openingHours : undefined,
  };
}

export function normalizeCalendarApiPayload(payload: unknown): RawDateTimeInstance[] {
  if (!payload) return [];
  const entries = Array.isArray(payload)
    ? payload
    : (payload as Record<string, unknown>)['entries'] ??
      (payload as Record<string, unknown>)['dates'] ??
      (payload as Record<string, unknown>)['months'];
  if (!Array.isArray(entries)) return [];
  const results: RawDateTimeInstance[] = [];
  for (const entry of entries) {
    if (!entry || typeof entry !== 'object') continue;
    const record = entry as Record<string, unknown>;
    const dateValue =
      record['date'] ?? record['day'] ?? record['dayOfMonth'] ?? record['isoDate'] ?? record['day_number'];
    const isoDate = ensureIsoDate(String(dateValue ?? ''));
    if (!isoDate) continue;
    const startTime =
      parseTimeTo24h(String(record['startTime'] ?? record['open'] ?? record['from'])) ?? undefined;
    const endTime =
      parseTimeTo24h(String(record['endTime'] ?? record['close'] ?? record['to'])) ?? undefined;
    const note =
      typeof record['note'] === 'string'
        ? record['note']
        : typeof record['status'] === 'string'
        ? record['status']
        : undefined;
    results.push({ date: isoDate, startTime, endTime, note });
  }
  return results;
}

export function extractRawDateTimeInstancesFromPage(
  page: PageScrapeResult,
  referenceDate = new Date(),
  options?: { jsonLdHours?: JsonLdHoursResult },
): RawDateTimeInstance[] {
  const relevantSections = extractRelevantSections(page.text);
  const fallbackSegments = gatherDeepSegments(page);
  const defaultSegments = fallbackSegments.length ? fallbackSegments : [cleanText(page.text)];
  const seen = new Set<string>();
  const instances: RawDateTimeInstance[] = [];
  const recordInstance = (instance: RawDateTimeInstance) => {
    const key = `${instance.date}|${instance.startTime ?? ''}|${instance.endTime ?? ''}|${instance.note ?? ''}`;
    if (seen.has(key)) return;
    seen.add(key);
    instances.push(instance);
  };

  const parseFromSegments = (segmentsToParse: string[]) => {
    for (const segment of segmentsToParse) {
      const results = parseChronoSegments(segment, referenceDate);
      for (const result of results) {
        const startDate = result.start?.date();
        if (!startDate) continue;
        const iso = ensureIsoDate(startDate);
        if (!iso) continue;
        const rawStartTime = result.start.isCertain('hour') ? formatTimeFromDate(startDate) : undefined;
        if (!rawStartTime || rawStartTime === '00:00') continue;
        const rawEnd = result.end?.date();
        const rawEndTime = rawEnd ? formatTimeFromDate(rawEnd) : undefined;
        const startTimeValue = rawStartTime;
        const endTimeValue = rawEndTime && rawEndTime !== '00:00' ? rawEndTime : startTimeValue;
        const keyNote = segment.length > 200 ? `${segment.slice(0, 200)}…` : segment;
        recordInstance({
          date: iso,
          startTime: startTimeValue,
          endTime: endTimeValue,
          note: keyNote || undefined,
        });
      }
    }
  };

  const targetSegments = relevantSections.length ? relevantSections : defaultSegments;
  parseFromSegments(targetSegments);
  if (relevantSections.length && instances.length < 3) {
    parseFromSegments(defaultSegments);
  } else if (!relevantSections.length) {
    logWarn(`[LOW CONFIDENCE] No focused schedule sections detected on ${page.url}`, page.url);
  }

  const gridInstances = parseCalendarGrid(page.text, referenceDate);
  for (const instance of gridInstances) {
    recordInstance(instance);
  }

  const jsonLdHours = options?.jsonLdHours;
  if (jsonLdHours?.exceptions?.length) {
    for (const exception of jsonLdHours.exceptions) {
      if (exception.status !== 'open') continue;
      if (!exception.open || !exception.close) continue;
      recordInstance({
        date: exception.date,
        startTime: exception.open,
        endTime: exception.close,
      });
    }
  }

  return instances;
}

const DYNAMIC_INDICATORS = [
  'data-month',
  'data-calendar',
  'fc-daygrid',
  'fc-event',
  'fc-next-button',
  'calendar-nav',
  'aria-live',
  'next month',
  'button.--next',
  'react-calendar',
];

export function classifyDateTimeFormat(page: PageScrapeResult): DateTimeFormatType {
  const html = (page.html ?? '').toLowerCase();
  const text = (page.text ?? '').toLowerCase();
  if (DYNAMIC_INDICATORS.some((marker) => html.includes(marker) || text.includes(marker))) {
    return 'js-dynamic';
  }
  if (/<table[^>]*>/i.test(html) && /(mon|tue|wed|thu|fri|sat|sun)/i.test(text)) {
    return 'table';
  }
  if (/(calendar-grid|fc-daygrid|month-view|day-grid|calendar-cell)/i.test(html)) {
    return 'grid';
  }
  if (/<(ul|ol)[^>]*>/.test(html) && /(\d{1,2}(:\d{2})?\s*(am|pm)?)/i.test(text)) {
    return 'list';
  }
  if (/(opening hours|schedule|calendar|timetable|opening times)/i.test(text)) {
    return 'text-blocks';
  }
  return 'unknown';
}

type NormalizedInstance = RawDateTimeInstance & { date: string };

function normalizeInstances(rawData: RawDateTimeInstance[]): NormalizedInstance[] {
  const normalized: NormalizedInstance[] = [];
  for (const entry of rawData) {
    const iso = ensureIsoDate(entry.date);
    if (iso) {
      normalized.push({ ...entry, date: iso });
    }
  }
  return normalized;
}

function getDayLabel(dateStr: string): (typeof DAY_LABELS)[number] | null {
  const parsed = new Date(`${dateStr}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return null;
  return DAY_LABELS[parsed.getDay()];
}

function buildMostCommonRange(days: Map<string, Map<string, { open: string; close: string; count: number }>>) {
  const openingHours: Record<string, { open: string; close: string }> = {};
  for (const day of DAY_LABELS) {
    const bucket = days.get(day);
    if (!bucket) continue;
    let best: { open: string; close: string; count: number } | null = null;
    for (const range of bucket.values()) {
      if (!best || range.count > best.count) {
        best = range;
      }
    }
    if (best) {
      openingHours[day] = { open: best.open, close: best.close };
    }
  }
  return openingHours;
}

function exceptionDuration(entry: SharedException): number {
  if (entry.status !== 'open') return 0;
  return timeToMinutes(entry.close) - timeToMinutes(entry.open);
}

function buildExceptionFromInstance(instance: NormalizedInstance): SharedException | null {
  const date = instance.date;
  const isClosedNote = instance.note?.toLowerCase().includes('closed');
  const hasTimes = Boolean(instance.startTime && (instance.endTime || instance.startTime));
  if (isClosedNote && !hasTimes) {
    return { status: 'closed', date };
  }
  if (hasTimes) {
    const open = instance.startTime ?? '00:00';
    const close = instance.endTime ?? instance.startTime ?? '00:00';
    return { status: 'open', date, open, close };
  }
  if (isClosedNote) {
    return { status: 'closed', date };
  }
  return null;
}

function collectExceptions(
  instances: NormalizedInstance[],
  schedule: Record<string, { open: string; close: string }>,
): SharedException[] {
  const exceptions = new Map<string, SharedException>();
  for (const instance of instances) {
    const day = getDayLabel(instance.date);
    const matchesSchedule =
      day &&
      instance.startTime &&
      instance.endTime &&
      schedule[day] &&
      schedule[day].open === instance.startTime &&
      schedule[day].close === instance.endTime;
    if (!matchesSchedule) {
      const candidate = buildExceptionFromInstance(instance);
      if (!candidate) continue;
      const existing = exceptions.get(candidate.date);
      if (!existing) {
        exceptions.set(candidate.date, candidate);
        continue;
      }
      if (existing.status === 'closed' && candidate.status === 'open') {
        exceptions.set(candidate.date, candidate);
        continue;
      }
      if (existing.status === 'open' && candidate.status === 'closed') {
        continue;
      }
      if (candidate.status === 'open' && existing.status === 'open') {
        const candidateDuration = exceptionDuration(candidate);
        const existingDuration = exceptionDuration(existing);
        if (candidateDuration !== existingDuration) {
          if (candidateDuration > existingDuration) {
            exceptions.set(candidate.date, candidate);
          }
          continue;
        }
        if (timeToMinutes(candidate.close) > timeToMinutes(existing.close)) {
          exceptions.set(candidate.date, candidate);
        }
      }
    }
  }
  return Array.from(exceptions.values());
}

function buildWeeklySchedule(instances: NormalizedInstance[]) {
  const dayBuckets = new Map<string, Map<string, { open: string; close: string; count: number }>>();
  for (const instance of instances) {
    if (!instance.startTime) continue;
    const day = getDayLabel(instance.date);
    if (!day) continue;
    const close = instance.endTime ?? instance.startTime;
    const key = `${instance.startTime}-${close}`;
    const bucket = dayBuckets.get(day) ?? new Map();
    const existing = bucket.get(key);
    if (existing) {
      existing.count += 1;
    } else {
      bucket.set(key, { open: instance.startTime, close, count: 1 });
    }
    dayBuckets.set(day, bucket);
  }
  const openingHours = buildMostCommonRange(dayBuckets);
  if (Object.keys(openingHours).length < 4) {
    return null;
  }
  return {
    openingHours,
    exceptions: collectExceptions(instances, openingHours),
  };
}

function buildEventDates(instances: NormalizedInstance[], location: SharedLocation): { dates: Dates; type: 'event' } {
  const deduped = new Map<string, NormalizedInstance>();
  for (const instance of instances) {
    const key = `${instance.date}|${instance.startTime ?? ''}|${instance.endTime ?? ''}`;
    if (!deduped.has(key)) {
      deduped.set(key, instance);
    }
  }
  const payloadInstances = Array.from(deduped.values()).map((instance) => ({
    date: instance.date,
    startTime: instance.startTime ?? instance.endTime ?? '00:00',
    endTime: instance.endTime ?? instance.startTime ?? '00:00',
    location,
  }));
  return {
    dates: { kind: 'event', instances: payloadInstances },
    type: 'event',
  };
}

export function parseAndClassifyDates(
  rawData: RawDateTimeInstance[],
  location: SharedLocation,
): { dates: Dates | null; type: 'place' | 'event' | null } {
  const normalized = normalizeInstances(rawData);
  if (!normalized.length) {
    return { dates: null, type: null };
  }
  const uniqueLocations = new Set(normalized.map((entry) => (entry.location ?? '').trim()).filter(Boolean));
  const multipleLocations = uniqueLocations.size > 1;
  if (normalized.length < 20) {
    return buildEventDates(normalized, location);
  }
  if (!multipleLocations) {
    const weekly = buildWeeklySchedule(normalized);
    if (weekly) {
      return {
        dates: {
          kind: 'place',
          location,
          openingHours: weekly.openingHours,
          exceptions: weekly.exceptions.length ? weekly.exceptions : undefined,
        },
        type: 'place',
      };
    }
  }
  return buildEventDates(normalized, location);
}


export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function clampAge(value: unknown): number {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    throw new Error('Age value is not a number.');
  }
  return Math.min(18, Math.max(0, Math.round(value)));
}

export function normalizeCandidateUrl(raw: string): string | null {
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

export function extractUrlsFromString(value: string): string[] {
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

const geocodeCache = new Map<string, { lat: number; lng: number }>();

export async function geocode(query: string): Promise<{ lat: number; lng: number }> {
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

export function buildAddressLine(place?: SourcePlace): string {
  if (!place) return 'London, United Kingdom';
  const parts: string[] = [];
  if (place.name && place.name.trim() && !place.name.match(/^london$/i)) parts.push(place.name.trim());
  if (place.address) parts.push(place.address.trim());
  if (place.town) parts.push(place.town.trim());
  if (place.postal_code) parts.push(place.postal_code.trim().replace(/\s+/g, ' ').toUpperCase());
  return parts.length ? parts.join(', ') : 'London, United Kingdom';
}

export async function resolveCoordinates(place?: SourcePlace): Promise<{ lat: number; lng: number }> {
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

  const geo = await geocode(address);
  geocodeCache.set(address, geo);
  return geo;
}

export function isBookingDomain(url: string): boolean {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    return bookingPatterns.some((pattern) => pattern.test(host) || pattern.test(url));
  } catch {
    return false;
  }
}

export interface OpeningHoursExtraction {
  source: 'none' | 'extracted' | 'jsonld';
  openingHours?: Record<string, { open: string; close: string }>;
  note?: string;
}

const DAY_ORDER = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

function expandDayRange(start: string, end: string): string[] {
  const startIdx = DAY_ORDER.indexOf(start);
  const endIdx = DAY_ORDER.indexOf(end);
  if (startIdx === -1 || endIdx === -1) {
    return [start];
  }
  const days: string[] = [];
  let idx = startIdx;
  do {
    days.push(DAY_ORDER[idx]);
    if (idx === endIdx) {
      break;
    }
    idx = (idx + 1) % DAY_ORDER.length;
  } while (days.length < DAY_ORDER.length);
  return days;
}

export function extractOpeningHours(pages: PageScrapeResult[]): OpeningHoursExtraction {
  for (const page of pages) {
    const jsonLd = page.structured.jsonLdHours;
    if (jsonLd?.hours) {
      const openDays = Object.fromEntries(
        Object.entries(jsonLd.hours)
          .filter(
            ([, value]) =>
              typeof value === 'object' &&
              value !== null &&
              'open' in (value as Record<string, unknown>) &&
              'close' in (value as Record<string, unknown>),
          )
          .map(([day, value]) => [
            day,
            {
              open: (value as { open: string }).open,
              close: (value as { close: string }).close,
            },
          ]),
      );
      if (Object.keys(openDays).length >= 4) {
        return {
          source: 'jsonld',
          openingHours: openDays as Record<string, { open: string; close: string }>,
          note: `from JSON-LD ${page.url}`,
        };
      }
    }
    const parsed = extractDatesAndTimesFromPage(page.text, page.url, { jsonLdHours: jsonLd });
    if (parsed.source === 'chrono' && parsed.openingHours && Object.keys(parsed.openingHours).length >= 4) {
      return {
        source: 'extracted',
        openingHours: parsed.openingHours,
        note: parsed.note ?? `from ${page.url}`,
      };
    }
  }
  return { source: 'none' };
}

function normalizeJsonLdTime(value?: string): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  const explicit = trimmed.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/i);
  if (explicit) {
    let hour = Number(explicit[1]);
    const minute = Number(explicit[2] ?? '0');
    const period = explicit[3]?.toLowerCase();
    if (period === 'pm' && hour < 12) {
      hour += 12;
    }
    if (period === 'am' && hour === 12) {
      hour = 0;
    }
    if (hour < 0) hour = 0;
    if (hour > 23) hour = hour % 24;
    const paddedHour = hour.toString().padStart(2, '0');
    const paddedMinute = minute.toString().padStart(2, '0');
    return `${paddedHour}:${paddedMinute}`;
  }
  const parsed = new Date(trimmed);
  if (!Number.isNaN(parsed.getTime())) {
    return formatTimeFromDate(parsed);
  }
  return null;
}

function buildPlaceDatesFromJsonLd(
  jsonLdHours: JsonLdHoursResult | undefined,
  location: SharedLocation,
): PlaceDates | null {
  if (!jsonLdHours?.hours) return null;
  const openingDays: Partial<Record<DayLabel, { open: string; close: string }>> = {};
  for (const [dayKey, value] of Object.entries(jsonLdHours.hours)) {
    if (!value || typeof value !== 'object') continue;
    const open = 'open' in value ? (value as { open?: string }).open : undefined;
    const close = 'close' in value ? (value as { close?: string }).close : undefined;
    if (typeof open === 'string' && typeof close === 'string') {
      const dayLabel = dayKey as DayLabel;
      openingDays[dayLabel] = { open, close };
    }
  }
  if (Object.keys(openingDays).length < 4) {
    return null;
  }
  return {
    kind: 'place',
    location,
    openingHours: openingDays as Record<DayLabel, { open: string; close: string }>,
    exceptions: jsonLdHours.exceptions?.length ? jsonLdHours.exceptions : undefined,
  };
}

function buildEventDatesFromJsonLdEvents(
  events: JsonLdEvent[] | undefined,
  location?: SharedLocation,
): { dates: Dates; type: 'event' } | null {
  if (!events?.length) return null;
  const uniqueInstances = new Map<string, RawDateTimeInstance>();
  for (const entry of events) {
    const status = entry.eventStatus?.toLowerCase();
    if (status && status.includes('cancel')) continue;
    const startStamp = entry.startDate;
    if (!startStamp) continue;
    const startDate = new Date(startStamp);
    if (Number.isNaN(startDate.getTime())) continue;
    const isoDate = ensureIsoDate(startDate);
    if (!isoDate) continue;
    const startFromDate = normalizeJsonLdTime(entry.startTime ?? entry.doorTime ?? startStamp);
    if (!startFromDate || startFromDate === '00:00') continue;
    const endStamp = entry.endDate;
    const normalizedEnd =
      normalizeJsonLdTime(entry.endTime ?? entry.doorTime ?? (endStamp ?? startStamp)) ??
      startFromDate;
    const finalEnd = normalizedEnd === '00:00' ? startFromDate : normalizedEnd;
    const key = `${isoDate}|${startFromDate}|${finalEnd}`;
    if (uniqueInstances.has(key)) continue;
    uniqueInstances.set(key, {
      date: isoDate,
      startTime: startFromDate,
      endTime: finalEnd,
      note: entry.name ? `jsonld event: ${entry.name}` : 'jsonld event',
    });
  }
  if (!uniqueInstances.size) {
    return null;
  }
  const instances = Array.from(uniqueInstances.values()).map((instance) => ({
    date: instance.date,
    startTime: instance.startTime ?? instance.endTime ?? '00:00',
    endTime: instance.endTime ?? instance.startTime ?? '00:00',
    location: location ?? undefined,
  }));
  return {
    dates: { kind: 'event', instances },
    type: 'event',
  };
}

export function extractStructuredDatesFromPage(
  page: PageScrapeResult,
  location: SharedLocation,
): { dates: Dates; type: 'place' | 'event' } | null {
  const placeCandidate = buildPlaceDatesFromJsonLd(page.structured.jsonLdHours, location);
  if (placeCandidate) {
    return { dates: placeCandidate as Dates, type: 'place' };
  }
  const eventCandidate = buildEventDatesFromJsonLdEvents(page.structured.jsonLdEvents, location);
  if (eventCandidate) {
    return eventCandidate;
  }
  return null;
}

export function addMinutesToTime(time: string, minutesToAdd: number): string {
  const [hourPart, minutePart] = time.split(':');
  const hours = Number(hourPart);
  const minutes = Number(minutePart);
  if (Number.isNaN(hours) || Number.isNaN(minutes)) {
    return time;
  }
  const total = hours * 60 + minutes + minutesToAdd;
  const normalized = ((total % (24 * 60)) + 24 * 60) % (24 * 60);
  const resultHours = Math.floor(normalized / 60);
  const resultMinutes = normalized % 60;
  const pad = (value: number) => value.toString().padStart(2, '0');
  return `${pad(resultHours)}:${pad(resultMinutes)}`;
}

export function formatExportTimestamp(date = new Date()): string {
  const pad = (value: number) => value.toString().padStart(2, '0');
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}-${pad(
    date.getHours(),
  )}${pad(date.getMinutes())}`;
}

export function sanitizeNameForFilename(name: string): string {
  const firstWord = (name.trim().split(/\s+/)[0] ?? 'activity').toLowerCase();
  const cleaned = firstWord.replace(/[^a-z0-9-]/g, '');
  return cleaned || 'activity';
}
