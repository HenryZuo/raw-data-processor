import { cleanText, normalizeCandidateUrl, isBookingDomain, logDebug, normalizeActivityForHostname, USER_AGENT_STRINGS } from './utils.js';
import { lightScrapeUrl, scoreForOfficialUrl } from './scraper.js';
import { OFFICIAL_SIGNAL_REGEX, AGGREGATOR_REGEX, PREFERRED_VENUE_REGEX, SERP_API_BLACKLIST } from './url-constants.js';
import type { PageScrapeResult, SourceEvent, OfficialUrlResolutionResult } from './types.js';
import type { Location as SharedLocation } from '../../london-kids-p1/packages/shared/src/activity.js';

const SERP_API_URL = 'https://serpapi.com/search.json';
const OFFICIAL_URL_CACHE = new Map<string, OfficialUrlResolutionResult>();
const SERP_CANDIDATE_LIMIT = 20;
export const OFFICIAL_URL_SCORE_THRESHOLD = 750;
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
      logDebug(`${url} failed HEAD/content-type check`, event.event_id);
      return false;
    }

    const res = await fetch(url, { signal: AbortSignal.timeout(12_000) });
    if (!res.ok) {
      logDebug(`${url} failed fetch status ${res.status}`, event.event_id);
      return false;
    }
    const buffer = await res.arrayBuffer();
    if (buffer.byteLength < 5000) {
      logDebug(`${url} rejected because the HTML was too small (${buffer.byteLength} bytes)`, event.event_id);
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
      logDebug(`${url} rejected because the activity name is missing from HTML`, event.event_id);
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
      logDebug(
        `hostname "${hostname}" lacks normalized activity "${normalizedHostname}", but continuing because signals may suffice.`,
        event.event_id,
      );
    }
    const tagLookup = new Set((event.tags ?? []).map((tag) => tag.toLowerCase()));
    if (tagLookup.has('film') && (html.includes('musical') || html.includes('theatre'))) {
      logDebug(`rejected ${url} because film tags clash with musical/theatre content`, event.event_id);
      return false;
    }

    const signalCount = [hasDescriptionMatch, hasOfficialSignal, noAggregator].filter(Boolean).length;
    const passed = signalCount >= 3;
    logDebug(
      `verify ${url} → name: ${hasName ? 'yes' : 'no'}, desc: ${hasDescriptionMatch ? 'yes' : 'no'}, official: ${
        hasOfficialSignal ? 'yes' : 'no'
      }, noAggregator: ${noAggregator ? 'yes' : 'no'} → ${passed ? 'ACCEPTED' : 'REJECTED'}`,
      event.event_id,
    );
    return passed;
  } catch (error) {
    logDebug(`verification for ${url} failed: ${(error as Error).message}`, event.event_id);
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

async function fetchSerpLinks(query: string, numResults = SERP_CANDIDATE_LIMIT): Promise<string[]> {
  const apiKey = process.env.SERPAPI_KEY;
  if (!apiKey) {
    logDebug('SERPAPI_KEY missing; skipping SerpAPI search for official URL.', 'url-resolver');
    return [];
  }

  try {
    const params = new URLSearchParams({
      engine: 'google',
      q: query,
      num: String(numResults),
      api_key: apiKey,
    });
    const response = await fetch(`${SERP_API_URL}?${params.toString()}`);
    if (!response.ok) {
      logDebug(`SerpAPI request failed: ${response.statusText}`, 'url-resolver');
      return [];
    }

    const payload = (await response.json()) as { organic_results?: { link?: string }[] };
    const results = payload.organic_results ?? [];
    return results
      .map((item) => item.link)
      .filter((link): link is string => Boolean(link));
  } catch (error) {
    logDebug(`SerpAPI fetch error: ${(error as Error).message}`, 'url-resolver');
    return [];
  }
}

function filterAndLimitSerpLinks(links: string[], limit = SERP_CANDIDATE_LIMIT): string[] {
  const filtered: string[] = [];
  for (const link of links) {
    const lowered = link.toLowerCase();
    if (SERP_API_BLACKLIST.some((term) => lowered.includes(term))) continue;
    filtered.push(link);
    if (filtered.length === limit) break;
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
  }

  return { urls: Array.from(urls), hosts };
}

export async function resolveOfficialUrl(
  event: SourceEvent,
  location: SharedLocation,
): Promise<OfficialUrlResolutionResult> {
  const cached = OFFICIAL_URL_CACHE.get(event.event_id);
  if (cached) {
    return cached;
  }

  const eventId = event.event_id;
  const lightScrapeCandidates: { url: string; score: number }[] = [];

  const finalize = (finalUrl: string | null): OfficialUrlResolutionResult => {
    const result: OfficialUrlResolutionResult = {
      officialUrl: finalUrl,
      lightScrapeCandidates,
    };
    OFFICIAL_URL_CACHE.set(eventId, result);
    return result;
  };

  const website = event.website?.trim();
  if (website) {
    const normalized = normalizeCandidateUrl(website);
    if (normalized) {
      if (isBookingDomain(normalized)) {
        logDebug(`rejected DataThistle website ${normalized} because it points to a booking domain.`, eventId);
      } else {
        const root = normalizeToRootUrl(normalized);
        logDebug(`using trusted DataThistle website ${root}`, eventId);
        return finalize(root);
      }
    }
  }

  const { urls: rawUrls, hosts: candidateHosts } = collectNonBookingCandidateSources(event);
  for (const candidate of rawUrls) {
    const normalized = normalizeCandidateUrl(candidate);
    if (!normalized) continue;
    if (await verifyCandidateUrl(normalized, event)) {
      const root = normalizeToRootUrl(normalized);
      logDebug(`selected verified candidate from raw links: ${root}`, eventId);
      return finalize(root);
    }
  }

  const hasFilmTag = (event.tags ?? []).some((tag) => /film|movie|cinema|screening/i.test(tag));
  if (hasFilmTag) {
    const cinemaHosts = Array.from(candidateHosts).filter(
      (host) => CINEMA_HOSTNAMES.includes(host) || CINEMA_HOSTNAMES.some((pattern) => host.includes(pattern)),
    );
    if (cinemaHosts.length >= 2) {
      logDebug(
        `SKIPPING dispersed film – multiple cinema venues: ${cinemaHosts.join(', ')}`,
        event.event_id,
      );
      return finalize(null);
    }
  }

  const tagLookup = new Set((event.tags ?? []).map((tag) => tag.toLowerCase()));
  let serpQuery = `${buildSearchQuery(event)} official website`;
  if (tagLookup.has('theatre') || tagLookup.has('show')) {
    serpQuery += ' venue official site';
  }

  let candidates = filterAndLimitSerpLinks(
    await fetchSerpLinks(serpQuery, SERP_CANDIDATE_LIMIT),
    SERP_CANDIDATE_LIMIT,
  );
  if (candidates.length < 3) {
    logDebug(`only ${candidates.length} SERP candidates; retrying with simplified query`, eventId);
    const fallbackQuery = `${cleanSearchName(event.name)} London official site`;
    const fallbackLinks = filterAndLimitSerpLinks(
      await fetchSerpLinks(fallbackQuery, SERP_CANDIDATE_LIMIT),
      SERP_CANDIDATE_LIMIT,
    );
    const merged = Array.from(new Set([...candidates, ...fallbackLinks]));
    candidates = filterAndLimitSerpLinks(merged, SERP_CANDIDATE_LIMIT);
  }

  if (!candidates.length) {
    logDebug('no SERP candidates to evaluate', eventId);
    return finalize(null);
  }

  const scrubbed = await Promise.allSettled(candidates.map((url) => lightScrapeUrl(url)));
  const seen = new Set<string>();
  const lightPages: Array<PageScrapeResult> = [];
  for (const result of scrubbed) {
    if (result.status !== 'fulfilled' || !result.value) continue;
    const normalized = normalizeCandidateUrl(result.value.url);
    if (!normalized || isBookingDomain(normalized)) continue;
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    lightPages.push({ ...result.value, url: normalized });
  }

  if (!lightPages.length) {
    logDebug('light scrape did not yield any candidates', eventId);
    return finalize(null);
  }

  const scoredLight = lightPages
    .map((page) => {
      let score = scoreForOfficialUrl(page, event);
      if (PREFERRED_VENUE_REGEX.test(page.url)) {
        score += 400;
      }
      return { page, score };
    })
    .filter((entry) => Number.isFinite(entry.score))
    .sort((a, b) => b.score - a.score);
  lightScrapeCandidates.push(
    ...scoredLight.map((entry) => ({ url: entry.page.url, score: entry.score })),
  );
  if (!scoredLight.length) {
    logDebug('no light-scraped pages scored for official signals', eventId);
    return finalize(null);
  }

  const topCandidate = scoredLight[0];
  if (!topCandidate) {
    logDebug('no viable light-scraped candidates after scoring', eventId);
    return finalize(null);
  }

  if (topCandidate.score > OFFICIAL_URL_SCORE_THRESHOLD) {
    const finalUrl = topCandidate.page.url;
    logDebug(`Selected ${finalUrl} with light score ${topCandidate.score}`, eventId);
    return finalize(finalUrl);
  }

  logDebug(
    `Highest light scrape score ${topCandidate.score} was below threshold ${OFFICIAL_URL_SCORE_THRESHOLD}`,
    eventId,
  );
  return finalize(null);
}
