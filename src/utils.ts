import * as chrono from 'chrono-node';
import type { PageScrapeResult, SourcePlace } from './types.js';

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

export function cleanText(text: string): string {
  const collapsed = text
    .replace(/[\t\r]/g, ' ')
    .replace(/\s*\n\s*/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/ {2,}/g, ' ')
    .trim();
  return collapsed;
}

const DAY_LABELS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] as const;

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
  referenceDate = new Date(),
): {
  source: 'chrono';
  openingHours?: Record<'Mon' | 'Tue' | 'Wed' | 'Thu' | 'Fri' | 'Sat' | 'Sun', { open: string; close: string }>;
  eventInstances?: Array<{ date: string; startTime: string; endTime?: string }>;
  note?: string;
} | { source: 'none' } {
  const segments = text
    .split(/\n{1,2}/)
    .map((segment) => segment.trim())
    .filter((segment) => segment.length > 0);

  const groupedResults = new Map<string, chrono.ParsedResult[]>();

  const addResults = (segment: string, results: chrono.ParsedResult[]) => {
    if (!results.length) return;
    const existing = groupedResults.get(segment) ?? [];
    groupedResults.set(segment, existing.concat(results));
  };

  for (const segment of segments) {
    const results = chrono.parse(segment, referenceDate, { forwardDate: true });
    addResults(segment, results);
  }

  const fallbackResults = chrono.parse(text, referenceDate, { forwardDate: true });
  for (const result of fallbackResults) {
    addResults(result.text, [result]);
  }

  if (!groupedResults.size) {
    return { source: 'none' };
  }

  const openingHours: Record<string, { open: string; close: string }> = {};
  const eventInstances: Array<{ date: string; startTime: string; endTime?: string }> = [];

  for (const [segment, results] of groupedResults.entries()) {
    const snippet = segment.toLowerCase();
    const timeResult = results.find((result) => result.end && (result.start.isCertain('hour') || result.start.get('hour') !== null));
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

      const instance = { date: startDate.toISOString().split('T')[0], startTime } as {
        date: string;
        startTime: string;
        endTime?: string;
      };
      if (endTime) {
        instance.endTime = endTime;
      }
      eventInstances.push(instance);
    }
  }

  const meaningfulCount = Object.keys(openingHours).length + eventInstances.length;
  if (meaningfulCount < 2) {
    return { source: 'none' };
  }

  const payload: {
    source: 'chrono';
    openingHours?: Record<'Mon' | 'Tue' | 'Wed' | 'Thu' | 'Fri' | 'Sat' | 'Sun', { open: string; close: string }>;
    eventInstances?: Array<{ date: string; startTime: string; endTime?: string }>;
    note?: string;
  } = { source: 'chrono', note: `from ${url}` };

  if (Object.keys(openingHours).length) {
    payload.openingHours = openingHours as Record<'Mon' | 'Tue' | 'Wed' | 'Thu' | 'Fri' | 'Sat' | 'Sun', { open: string; close: string }>;
  }
  if (eventInstances.length) {
    payload.eventInstances = eventInstances;
  }
  return payload;
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
  source: 'none' | 'extracted';
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
    const parsed = extractDatesAndTimesFromPage(page.text, page.url);
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
