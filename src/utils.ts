import type { PageScrapeResult, SourcePerformance, SourcePlace } from './types.js';

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

const DAY_CANONICAL: Record<string, string> = {
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
  sun: 'Sun',
  sunday: 'Sun',
};

const DAY_ORDER = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

function to24h(hourPart: string, minutePart: string | undefined, ampm?: string): string {
  let hour = parseInt(hourPart, 10);
  let minute = minutePart ? parseInt(minutePart, 10) : 0;
  if (Number.isNaN(hour)) {
    hour = 0;
  }
  if (Number.isNaN(minute)) {
    minute = 0;
  }
  const suffix = ampm?.trim().toLowerCase();
  if (suffix === 'pm' && hour < 12) {
    hour += 12;
  }
  if (suffix === 'am' && hour === 12) {
    hour = 0;
  }
  hour = ((hour % 24) + 24) % 24;
  minute = Math.min(59, Math.max(0, minute));
  const pad = (value: number) => value.toString().padStart(2, '0');
  return `${pad(hour)}:${pad(minute)}`;
}

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

function parseHoursText(text: string): Record<string, { open: string; close: string }> {
  const result: Record<string, { open: string; close: string }> = {};
  const normalized = text
    .toLowerCase()
    .replace(/a\.m\./g, 'am')
    .replace(/p\.m\./g, 'pm')
    .replace(/midnight/g, '12am')
    .replace(/noon/g, '12pm');

  // Regex 1: Day before optional date (original, e.g., "Wednesday 10th: 10am - 3pm")
  const dayRegexAfter =
    /(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)(?:\s*\d{1,2}(?:st|nd|rd|th)?)?(?:\s*(?:-|to|–|—)\s*(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)(?:\s*\d{1,2}(?:st|nd|rd|th)?)?)?\s*(?:[:–—-])?\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*[-–—]\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/gi;

  // Regex 2: Optional date before day (new, e.g., "10 Wednesday: 10am - 3pm" or "11th Thu - 14th Sun: 10am - 4pm")
  const dayRegexBefore =
    /(?:\d{1,2}(?:st|nd|rd|th)?\s*)?(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)(?:\s*(?:-|to|–|—)\s*(?:\d{1,2}(?:st|nd|rd|th)?\s*)?(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?))?\s*(?:[:–—-])?\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*[-–—]\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/gi;

  const processMatches = (regex: RegExp) => {
    let match: RegExpExecArray | null;
    while ((match = regex.exec(normalized)) !== null) {
      const startToken = match[1];
      const endToken = match[2];
      const startDay = startToken ? DAY_CANONICAL[startToken.slice(0, 3).toLowerCase()] : null;
      const endDay = endToken ? DAY_CANONICAL[endToken.slice(0, 3).toLowerCase()] : startDay;
      if (!startDay || !endDay) continue;

      const open = to24h(match[3], match[4], match[5] ?? undefined);
      const close = to24h(match[6], match[7], match[8] ?? undefined);

      const days = expandDayRange(startDay, endDay);
      for (const day of days) {
        if (!result[day]) {
          result[day] = { open, close };
        }
      }
    }
  };

  processMatches(dayRegexAfter);
  processMatches(dayRegexBefore);

  // Daily fallback (unchanged)
  const dailyRegex =
    /(daily|every day|open daily|open every day|open daily|open every day)(?:\s*[:–—-])?\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*[-–—]\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/i;
  const dailyMatch = dailyRegex.exec(normalized);
  if (dailyMatch) {
    const open = to24h(dailyMatch[2], dailyMatch[3], dailyMatch[4] ?? undefined);
    const close = to24h(dailyMatch[5], dailyMatch[6], dailyMatch[7] ?? undefined);
    for (const day of DAY_ORDER) {
      if (!result[day]) {
        result[day] = { open, close };
      }
    }
  }

  return result;
}

function trimmedText(value?: string): string {
  return value?.trim() ?? '';
}

export function extractOpeningHours(
  pages: PageScrapeResult[],
  _performances: SourcePerformance[] = [],
): OpeningHoursExtraction {
  for (const page of pages) {
    const candidates = [
      page.structured.extractedHours,
      page.structured.openingHoursText,
      page.structured.extractedDescription,
    ].map(trimmedText);
    for (const candidate of candidates) {
      if (!candidate) continue;
      const parsed = parseHoursText(candidate);
      if (Object.keys(parsed).length >= 4) {
        return {
          source: 'extracted',
          openingHours: parsed,
          note: `from ${page.url}`,
        };
      }
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
