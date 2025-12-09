import { cleanText, normalizeCandidateUrl, isBookingDomain } from './utils.js';
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
  'tripadvisor',
  'wikipedia',
  'youtube',
  'facebook',
  'yelp',
];
const OFFICIAL_URL_CACHE = new Map<string, string | null>();

export function normalizeActivityForHostname(name: string): string | null {
  const cleaned = name
    .toLowerCase()
    .replace(/[’'‘`]/g, '')
    .replace(/\blondon\b/g, '')
    .replace(/[^a-z0-9]+/g, '');
  return cleaned.length >= 3 ? cleaned : null;
}

function normalizeToRootUrl(url: string): string {
  try {
    return `${new URL(url).origin}/`;
  } catch {
    return url;
  }
}

async function verifyCandidateUrl(url: string, event: SourceEvent): Promise<boolean> {
  try {
    const head = await fetch(url, { method: 'HEAD', redirect: 'follow', signal: AbortSignal.timeout(8_000) });
    if (!head.ok || !head.headers.get('content-type')?.includes('text/html')) {
      console.log(`[${event.event_id}] ${url} failed HEAD/content-type check`);
      return false;
    }

    const res = await fetch(url, { signal: AbortSignal.timeout(12_000) });
    if (!res.ok) {
      console.log(`[${event.event_id}] ${url} failed fetch status ${res.status}`);
      return false;
    }
    const buffer = await res.arrayBuffer();
    if (buffer.byteLength < 5000) {
      console.log(`[${event.event_id}] ${url} rejected because the HTML was too small (${buffer.byteLength} bytes)`);
      return false;
    }

    const html = new TextDecoder().decode(buffer.slice(0, 15_000)).toLowerCase();
    const activityName = event.name.trim();
    const normalizedName = activityName.toLowerCase().replace(/[’'‘`]/g, '');
    const tokens = normalizedName
      .replace(/\blondon\b/g, '')
      .replace(/[^a-z0-9]+/g, ' ')
      .split(' ')
      .filter((token) => token.length >= 3);
    const hasName =
      (normalizedName.length >= 3 && html.includes(normalizedName)) ||
      tokens.some((token) => html.includes(token));
    if (!hasName) {
      console.log(`[${event.event_id}] ${url} rejected because the activity name is missing from HTML`);
      return false;
    }
    const descriptionTokens = new Set<string>();
    for (const desc of event.descriptions ?? []) {
      desc.description
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, ' ')
        .split(' ')
        .filter((token) => token.length >= 4)
        .forEach((token) => descriptionTokens.add(token));
    }
    const hasDescriptionMatch = Array.from(descriptionTokens).some((token) => html.includes(token));
    const hasOfficialSignal = OFFICIAL_SIGNAL_REGEX.test(html);
    const noAggregator = !AGGREGATOR_REGEX.test(html);

    const hostname = new URL(url).hostname.toLowerCase();
    const normalizedHostname = normalizeActivityForHostname(activityName);
    const hostnameSlim = hostname.replace(/[^a-z0-9]+/g, '');
    const hostnameMatches = !normalizedHostname || hostnameSlim.includes(normalizedHostname);
    if (!hostnameMatches) {
      console.log(
        `[${event.event_id}] hostname "${hostname}" lacks normalized activity "${normalizedHostname}", but continuing because signals may suffice.`,
      );
    }
    const tagLookup = new Set((event.tags ?? []).map((tag) => tag.toLowerCase()));
    if (tagLookup.has('film') && (html.includes('musical') || html.includes('theatre'))) {
      console.log(`[${event.event_id}] rejected ${url} because film tags clash with musical/theatre content`);
      return false;
    }

    const signalCount = [hasDescriptionMatch, hasOfficialSignal, noAggregator].filter(Boolean).length;
    const passed = signalCount >= 3;
    console.log(
      `[verify] ${url} → name: ${hasName ? 'yes' : 'no'}, desc: ${hasDescriptionMatch ? 'yes' : 'no'}, official: ${
        hasOfficialSignal ? 'yes' : 'no'
      }, noAggregator: ${noAggregator ? 'yes' : 'no'} → ${passed ? 'ACCEPTED' : 'REJECTED'}`,
    );
    return passed;
  } catch (error) {
    console.log(`[${event.event_id}] verification for ${url} failed: ${(error as Error).message}`);
    return false;
  }
}

function cleanSearchName(name: string): string {
  return name
    .replace(/[’'‘`!]/g, '')
    .replace(/[^a-zA-Z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildTypeHints(tags?: string[]): string {
  const normalizedTags = new Set((tags ?? []).map((tag) => tag.toLowerCase()));
  const hints: string[] = [];
  if (normalizedTags.has('film')) {
    hints.push('film movie screening cinema');
  }
  if (normalizedTags.has('theatre') || normalizedTags.has('musical')) {
    hints.push('theatre musical stage');
  }
  if (normalizedTags.has('concert') || normalizedTags.has('music')) {
    hints.push('concert live performance');
  }
  return hints.join(' ');
}

function buildExclusionHints(tags?: string[]): string[] {
  const exclusions = ['-ticketmaster', '-eventbrite', '-seetickets', '-timeout', '-visitlondon'];
  const normalizedTags = new Set((tags ?? []).map((tag) => tag.toLowerCase()));
  if (normalizedTags.has('film')) {
    exclusions.push('-musical', '-theatre');
  }
  if (normalizedTags.has('theatre') || normalizedTags.has('musical')) {
    exclusions.push('-film', '-movie', '-screening', '-cinema');
  }
  return exclusions;
}

function buildDescriptionSnippet(event: SourceEvent): string {
  const text = event.descriptions?.[0]?.description;
  if (!text) return '';
  return cleanText(text).slice(0, 50);
}

function buildSearchQuery(event: SourceEvent): string {
  const cleanedName = cleanSearchName(event.name);
  const typeHints = buildTypeHints(event.tags);
  const snippet = buildDescriptionSnippet(event);
  const exclusions = buildExclusionHints(event.tags).join(' ');
  return [
    cleanedName,
    'London',
    'official',
    'website',
    'tickets',
    typeHints,
    snippet,
    exclusions,
  ]
    .filter(Boolean)
    .join(' ');
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

function collectNonBookingCandidateSources(event: SourceEvent): { urls: string[]; hosts: Set<string> } {
  const urls = new Set<string>();
  const hosts = new Set<string>();
  const addSource = (rawUrl: string | undefined) => {
    if (!rawUrl) return;
    const normalized = normalizeCandidateUrl(rawUrl);
    if (!normalized || isBookingDomain(normalized)) return;
    urls.add(normalized);
    try {
      hosts.add(new URL(normalized).hostname.toLowerCase());
    } catch {
      // ignore hostname extraction errors
    }
  };

  event.links?.forEach((link) => addSource(link.url));
  for (const schedule of event.schedules ?? []) {
    schedule.links?.forEach((link) => addSource(link.url));
    for (const performance of schedule.performances ?? []) {
      performance.links?.forEach((link) => addSource(link.url));
    }
  }

  return { urls: Array.from(urls), hosts };
}

async function evaluateCandidateList(
  candidates: string[],
  event: SourceEvent,
  preferredHosts: Set<string>,
): Promise<{ url: string; pathLength: number; hostMatch: boolean }[]> {
  const verified: { url: string; pathLength: number; hostMatch: boolean }[] = [];
  const seen = new Set<string>();
  const normalizedActivityHostname = normalizeActivityForHostname(event.name);

  for (const candidate of candidates) {
    console.log(`[${event.event_id}] evaluating candidate ${candidate}`);
    const normalized = normalizeCandidateUrl(candidate);
    if (!normalized || seen.has(normalized) || isBookingDomain(normalized)) {
      continue;
    }
    seen.add(normalized);

    if (await verifyCandidateUrl(normalized, event)) {
      let pathLength = normalized.length;
      try {
        pathLength = new URL(normalized).pathname.length;
      } catch {
        pathLength = normalized.length;
      }
      const hostname = (() => {
        try {
          return new URL(normalized).hostname.toLowerCase();
        } catch {
          return '';
        }
      })();
      const hostnameSlim = hostname.replace(/[^a-z0-9]+/g, '');
      const hostnameMatch = normalizedActivityHostname
        ? hostnameSlim.includes(normalizedActivityHostname)
        : false;
      const hostMatch = hostname ? preferredHosts.has(hostname) || hostnameMatch : false;
      verified.push({ url: normalized, pathLength, hostMatch });
    }
  }

  verified.sort((a, b) => {
    if (a.hostMatch !== b.hostMatch) {
      return b.hostMatch ? 1 : -1;
    }
    return a.pathLength - b.pathLength;
  });

  return verified;
}

export async function resolveOfficialUrl(event: SourceEvent): Promise<string | null> {
  if (OFFICIAL_URL_CACHE.has(event.event_id)) {
    return OFFICIAL_URL_CACHE.get(event.event_id) ?? null;
  }

  const eventId = event.event_id;
  let officialUrl: string | null = null;

  const website = event.website?.trim();
  if (website) {
    const normalized = normalizeCandidateUrl(website);
    if (normalized) {
      if (isBookingDomain(normalized)) {
        console.log(`[${eventId}] rejected DataThistle website ${normalized} because it points to a booking domain.`);
      } else {
        officialUrl = normalizeToRootUrl(normalized);
        console.log(`[${eventId}] using trusted DataThistle website ${officialUrl}`);
        OFFICIAL_URL_CACHE.set(eventId, officialUrl);
        return officialUrl;
      }
    }
  }

  const { urls: rawUrls, hosts: preferredHosts } = collectNonBookingCandidateSources(event);
  if (rawUrls.length > 0) {
    const verifiedRaw = await evaluateCandidateList(rawUrls, event, preferredHosts);
    if (verifiedRaw.length > 0) {
      officialUrl = normalizeToRootUrl(verifiedRaw[0].url);
      console.log(`[${eventId}] selected best candidate from event links: ${officialUrl}`);
      OFFICIAL_URL_CACHE.set(eventId, officialUrl);
      return officialUrl;
    }
  }

  const query = buildSearchQuery(event);
  let candidates = filterAndLimitSerpLinks(await fetchSerpLinks(query));
  if (candidates.length < 3) {
    console.log(`[${eventId}] only ${candidates.length} SERP candidates; retrying with simplified query`);
    const fallbackQuery = `${cleanSearchName(event.name)} London official site`;
    const fallbackLinks = filterAndLimitSerpLinks(await fetchSerpLinks(fallbackQuery));
    const merged = Array.from(new Set([...candidates, ...fallbackLinks]));
    candidates = filterAndLimitSerpLinks(merged);
  }

  const verifiedCandidates = await evaluateCandidateList(candidates, event, preferredHosts);

  // ---- NEW: Skip dispersed film events (e.g. Elf shown at many cinemas) ----
  const hasFilmTag = (event.tags ?? []).some((tag) => /film|movie|cinema|screening/i.test(tag));
  if (hasFilmTag && verifiedCandidates.length > 0) {
    const uniqueHosts = new Set(
      verifiedCandidates
        .map((c) => {
          try {
            return new URL(c.url).hostname.toLowerCase();
          } catch {
            return '';
          }
        })
        .filter(Boolean),
    );
    if (uniqueHosts.size > 1) {
      const CINEMA_HOSTNAMES = [
        'odeon.co.uk',
        'vue.com',
        'cineworld.co.uk',
        'picturehouses.com',
        'everymancinema.com',
        'curzon.com',
        'curzoncinemas.com',
        'showcasecinemas.co.uk',
        'reelcinemas.co.uk',
        'phoenixcinema.co.uk',
        'riocinema.org.uk',
        'lexicinema.co.uk',
        'bfi.org.uk',
        'barbican.org.uk',
        'genesis-cinema.co.uk',
        'riversidestudios.co.uk',
        'cinema',
        'filmhouse',
        'picturehouse',
      ];
      const cinemaHosts = Array.from(uniqueHosts).filter(
        (host) => CINEMA_HOSTNAMES.includes(host) || CINEMA_HOSTNAMES.some((p) => host.includes(p)),
      );
      if (cinemaHosts.length >= 2) {
        console.log(
          `[${event.event_id}] SKIPPING dispersed film – multiple cinema venues: ${cinemaHosts.join(', ')}`,
        );
        OFFICIAL_URL_CACHE.set(eventId, null);
        return null;
      }
    }
  }
  // ---- END NEW BLOCK ----

  if (verifiedCandidates.length > 0) {
    officialUrl = normalizeToRootUrl(verifiedCandidates[0].url);
    console.log(`[${eventId}] Selected best URL: ${officialUrl}`);
  }

  OFFICIAL_URL_CACHE.set(eventId, officialUrl);
  return officialUrl;
}
