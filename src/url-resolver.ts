import { normalizeCandidateUrl, isBookingDomain } from './utils.js';
import type { SourceEvent } from './types.js';

const OFFICIAL_SIGNAL_REGEX = /official|book tickets|opening times|visit us|family|kids|age \d|merlin|©/i;
const AGGREGATOR_REGEX = /ticketmaster|eventbrite|timeout\.com|visitlondoncom|datathistle|seetickets|axs|ticketweb|skiddle/i;

const SERP_API_URL = 'https://serpapi.com/search.json';
const SERP_API_BLACKLIST = [
  'ticketmaster',
  'eventbrite',
  'seetickets',
  'timeout',
  'datathistle',
  'axs',
  'ticketweb',
  'dayoutwiththekids.co.uk',
  'visitlondon.com',
];
const OFFICIAL_URL_CACHE = new Map<string, string | null>();

function normalizeActivityForHostname(name: string): string | null {
  const cleaned = name
    .toLowerCase()
    .replace(/[’'‘`]/g, '')
    .replace(/\blondon\b/g, '')
    .replace(/[^a-z0-9]+/g, '');
  return cleaned.length >= 3 ? cleaned : null;
}

async function verifyCandidateUrl(url: string, activityName: string, eventId: string): Promise<boolean> {
  try {
    const head = await fetch(url, { method: 'HEAD', redirect: 'follow', signal: AbortSignal.timeout(8_000) });
    if (!head.ok || !head.headers.get('content-type')?.includes('text/html')) {
      console.log(`[${eventId}] ${url} failed HEAD/content-type check`);
      return false;
    }

    const res = await fetch(url, { signal: AbortSignal.timeout(12_000) });
    if (!res.ok) {
      console.log(`[${eventId}] ${url} failed fetch status ${res.status}`);
      return false;
    }
    const buffer = await res.arrayBuffer();
    if (buffer.byteLength < 5000) {
      console.log(`[${eventId}] ${url} rejected because the HTML was too small (${buffer.byteLength} bytes)`);
      return false;
    }

    const html = new TextDecoder().decode(buffer.slice(0, 15_000)).toLowerCase();
    const normalizedName = activityName.toLowerCase().replace(/[’'‘`]/g, '');
    const tokens = normalizedName
      .replace(/\blondon\b/g, '')
      .replace(/[^a-z0-9]+/g, ' ')
      .split(' ')
      .filter((token) => token.length >= 3);
    const hasName =
      (normalizedName.length >= 3 && html.includes(normalizedName)) ||
      tokens.some((token) => html.includes(token));
    const hasOfficialSignal = OFFICIAL_SIGNAL_REGEX.test(html);
    const noAggregator = !AGGREGATOR_REGEX.test(html);

    const hostname = new URL(url).hostname.toLowerCase();
    const normalizedHostname = normalizeActivityForHostname(activityName);
    const hostnameSlim = hostname.replace(/[^a-z0-9]+/g, '');
    if (normalizedHostname && !hostnameSlim.includes(normalizedHostname)) {
      console.log(
        `[${eventId}] rejected ${url} because hostname "${hostname}" lacks normalized activity "${normalizedHostname}"`,
      );
      return false;
    }

    const passedCount = [hasName, hasOfficialSignal, noAggregator].filter(Boolean).length;
    const result = passedCount >= 2;
    console.log(
      `[verify] ${url} → name: ${hasName ? 'yes' : 'no'}, officialSignal: ${hasOfficialSignal ? 'yes' : 'no'}, noAggregator: ${
        noAggregator ? 'yes' : 'no'
      } → ${result ? 'ACCEPTED' : 'REJECTED'}`,
    );
    return result;
  } catch (error) {
    console.log(`[${eventId}] verification for ${url} failed: ${(error as Error).message}`);
    return false;
  }
}

function buildSearchQuery(event: SourceEvent): string {
  const tagsSegment = event.tags?.slice(0, 3).filter(Boolean).join(' ');
  const nameSegment = event.name.trim();
  const parts = [`"${nameSegment}"`];
  if (tagsSegment) {
    parts.push(tagsSegment);
  }
  parts.push('official website London', '-ticketmaster -eventbrite -seetickets -timeout');
  return parts.join(' ');
}

async function fetchSerpLinks(query: string): Promise<string[]> {
  const apiKey = process.env.SERPAPI_KEY;
  if (!apiKey) {
    console.warn('SERPAPI_KEY missing; skipping SerpAPI search for official URL.');
    return [];
  }

  try {
    const params = new URLSearchParams({
      engine: 'google',
      q: query,
      num: '10',
      api_key: apiKey,
    });
    const response = await fetch(`${SERP_API_URL}?${params.toString()}`);
    if (!response.ok) {
      console.warn('SerpAPI request failed:', response.statusText);
      return [];
    }

    const payload = (await response.json()) as { organic_results?: { link?: string }[] };
    const results = payload.organic_results ?? [];
    return results
      .map((item) => item.link)
      .filter((link): link is string => Boolean(link));
  } catch (error) {
    console.warn('SerpAPI fetch error:', (error as Error).message);
    return [];
  }
}

function filterAndLimitSerpLinks(links: string[]): string[] {
  const filtered: string[] = [];
  for (const link of links) {
    const lowered = link.toLowerCase();
    if (SERP_API_BLACKLIST.some((term) => lowered.includes(term))) continue;
    filtered.push(link);
    if (filtered.length === 5) break;
  }
  return filtered;
}

export async function resolveOfficialUrl(event: SourceEvent): Promise<string | null> {
  if (OFFICIAL_URL_CACHE.has(event.event_id)) {
    return OFFICIAL_URL_CACHE.get(event.event_id) ?? null;
  }

  const activityName = event.name.trim();
  let officialUrl: string | null = null;

  const website = event.website?.trim();
  if (website) {
    const normalized = normalizeCandidateUrl(website);
    if (normalized) {
      if (isBookingDomain(normalized)) {
        console.log(`[${event.event_id}] rejected DataThistle website ${normalized} because it points to a booking domain.`);
      } else {
        officialUrl = normalized;
        console.log(`[${event.event_id}] using trusted DataThistle website ${normalized}`);
        OFFICIAL_URL_CACHE.set(event.event_id, officialUrl);
        return officialUrl;
      }
    }
  }

  const query = buildSearchQuery(event);
  const serpLinks = await fetchSerpLinks(query);
  const candidates = filterAndLimitSerpLinks(serpLinks);

  for (const candidate of candidates) {
    console.log(`[${event.event_id}] SerpAPI candidate ${candidate}`);
    const normalized = normalizeCandidateUrl(candidate);
    if (!normalized) {
      console.log(`[${event.event_id}] skipped invalid candidate ${candidate}`);
      continue;
    }
    if (await verifyCandidateUrl(normalized, activityName, event.event_id)) {
      officialUrl = normalized;
      break;
    }
  }

  OFFICIAL_URL_CACHE.set(event.event_id, officialUrl);
  return officialUrl;
}
