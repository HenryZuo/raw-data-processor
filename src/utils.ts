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
  if (!place) {
    return 'London, United Kingdom';
  }
  const line = [place.address, place.town, place.postal_code].filter(Boolean).join(', ');
  return line || 'London, United Kingdom';
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

function parseHoursText(text: string): Record<string, { open: string; close: string }> {
  const result: Record<string, { open: string; close: string }> = {};
  const dayPattern = '(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)';
  const timePattern = '(\\d{1,2}:\\d{2})';
  const regex = new RegExp(`${dayPattern}[\\s\\S]{0,40}?${timePattern}\\s*[–-]\\s*${timePattern}`, 'gi');
  let match: RegExpExecArray | null;
  while ((match = regex.exec(text)) !== null) {
    const dayKey = match[1]?.toLowerCase();
    if (!dayKey) continue;
    const canonical = DAY_CANONICAL[dayKey];
    if (!canonical || result[canonical]) continue;
    const open = match[2];
    const close = match[3];
    if (open && close) {
      result[canonical] = { open, close };
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
