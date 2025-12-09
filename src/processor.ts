import fs from 'fs/promises';
import path from 'path';
import { mapAddressToAreas, activitySchema } from './schema-loader.js';
import { resolveOfficialUrl } from './url-resolver.js';
import { getOfficialUrlAndContent } from './scraper.js';
import { fetchLLMDataWithRetries, ActivityLLMOutput } from './llm.js';
import {
  extractOpeningHours,
  addMinutesToTime,
  resolveCoordinates,
  buildAddressLine,
  isBookingDomain,
  USER_AGENT_STRINGS,
  formatExportTimestamp,
  sanitizeNameForFilename,
} from './utils.js';
import type { PageScrapeResult, SourceEvent, SourceSchedule, SourcePerformance } from './types.js';
import type {
  Activity,
  Dates,
  Location as SharedLocation,
} from '../../london-kids-p1/packages/shared/src/activity.js';

const SCRAPE_LOG_DIR = path.resolve(process.cwd(), 'scrape-logs');

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
    console.warn(`Failed to export scraped content for ${eventId}:`, (error as Error).message);
  }
}

const TASK_NAMES = [
  '1. Identify the official webpage URL',
  '2. Scrape the official webpage',
  '3. Send scraped content + raw info to LLM',
  '4. Receive LLM response',
  '5. Combine LLM response and raw data into final activity',
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
  console.log(`[${eventId}] Status overview → ${summary}`);
}

function dedupeEvents(events: SourceEvent[]): SourceEvent[] {
  const eventsById = new Map<string, SourceEvent>();
  for (const event of events) {
    const existing = eventsById.get(event.event_id);
    if (!existing) {
      eventsById.set(event.event_id, event);
      continue;
    }
    if (countPerformances(event) > countPerformances(existing)) {
      eventsById.set(event.event_id, event);
    }
  }
  return Array.from(eventsById.values());
}

function countPerformances(event: SourceEvent): number {
  return (
    event.schedules?.reduce(
      (total: number, schedule: SourceSchedule) => total + (schedule.performances?.length ?? 0),
      0,
    ) ?? 0
  );
}

function describeRawSchedules(event: SourceEvent): string {
  const lines: string[] = [];
  const performances = event.schedules?.flatMap((schedule) => schedule.performances ?? []) ?? [];
  if (!performances.length) {
    return 'No raw schedule details provided.';
  }
  performances.forEach((performance: SourcePerformance, index: number) => {
    const timestamp = performance.ts;
    let dateLabel = 'unknown date';
    let timeLabel = 'unknown time';
    if (timestamp) {
      const [datePart, timePart] = timestamp.split('T');
      dateLabel = datePart ?? 'unknown date';
      timeLabel = timePart?.slice(0, 5) ?? 'unknown time';
    }
    const duration = performance.duration ?? 'unknown duration';
    const timeNote = performance.time_unknown ? ' (time unknown)' : '';
    lines.push(
      `Performance ${index + 1}: ${dateLabel} at ${timeLabel}${timeNote} – duration ${duration} mins`,
    );
  });
  return lines.join('\n');
}

export async function processEvents(events: SourceEvent[], groqApiKey: string) {
  const activities: Activity[] = [];
  const skipped: { id: string; reason: string }[] = [];
  const uniqueEvents = dedupeEvents(events);

  for (const event of uniqueEvents) {
    console.log(`Processing ${event.event_id}: ${event.name}`);
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

  const officialUrl = await resolveOfficialUrl(event);
  if (!officialUrl) {
      setTaskStatus(statuses, 0, 'fail', 'no official descriptive URL found');
      logAndContinue();
      skipped.push({ id: event.event_id, reason: 'no official descriptive URL found' });
      continue;
    }
    setTaskStatus(statuses, 0, 'success', 'URL identified');
    console.log(`[${event.event_id}] Step 1: identified official URL ${officialUrl}`);
    scrapedOfficialUrl = officialUrl;

    let startUrl = officialUrl;
    try {
      const u = new URL(officialUrl);
      startUrl = `${u.origin}/`;
    } catch {
      // ignore invalid URL
    }
    const scrapedPages = await getOfficialUrlAndContent(startUrl, event.name, USER_AGENT_STRINGS);
    if (!scrapedPages.length) {
      setTaskStatus(statuses, 1, 'fail', 'official page scrape failed');
      console.log(`[${event.event_id}] Step 2: scraping failed for ${officialUrl}`);
      console.warn(`Scrape failed for ${event.event_id}: ${officialUrl}`);
      logAndContinue();
      skipped.push({ id: event.event_id, reason: 'official page scrape failed' });
      continue;
    }
    setTaskStatus(statuses, 1, 'success', 'official page scraped');
    console.log(
      `[${event.event_id}] Step 2: scraped official content (${scrapedPages.length} page(s), ${scrapedPages[0].text.length} chars first page)`,
    );
    for (const [index, page] of scrapedPages.entries()) {
      await exportScrapedContent(event.event_id, event.name, page, index);
    }

    const rawScheduleSummary = describeRawSchedules(event);
    console.log(`[${event.event_id}] Step 3: sending scraped content + raw data to Groq`);
    const llmData = await fetchLLMDataWithRetries(
      event,
      scrapedPages,
      rawScheduleSummary,
      groqApiKey,
    );
    if (!llmData) {
      setTaskStatus(statuses, 3, 'fail', 'LLM returned null (all attempts failed)');
      logAndContinue();
      skipped.push({ id: event.event_id, reason: 'LLM failed after retries' });
      continue;
    }
    setTaskStatus(statuses, 2, 'success', 'Groq request finished');

    console.log(
      `[${event.event_id}] Step 4: received LLM response (type=${llmData.type}, contentMatchesDescription=${llmData.contentMatchesDescription})`,
    );
    if (!llmData.contentMatchesDescription) {
      setTaskStatus(statuses, 3, 'fail', 'LLM mismatch in content');
      logAndContinue();
      skipped.push({
        id: event.event_id,
        reason: 'scraped content does not match raw description',
      });
      continue;
    }
    setTaskStatus(statuses, 3, 'success', 'LLM response valid');

    if (isBookingDomain(officialUrl)) {
      setTaskStatus(statuses, 3, 'fail', 'official URL is a booking domain');
      logAndContinue();
      skipped.push({ id: event.event_id, reason: 'official URL points to booking domain' });
      continue;
    }

    try {
    const activity = await buildActivity(event, llmData, officialUrl, scrapedPages, scrapedOfficialUrl);
      if (!activity) {
        setTaskStatus(statuses, 4, 'fail', 'no valid dates');
        logAndContinue();
        skipped.push({ id: event.event_id, reason: 'no valid dates extracted' });
        continue;
      }
      activitySchema.parse(activity);
      activities.push(activity);
      setTaskStatus(statuses, 4, 'success', 'final activity constructed');
      console.log(`[${event.event_id}] Step 5: combined LLM response and raw data into final activity`);
      logAndContinue();
    } catch (error) {
      setTaskStatus(statuses, 4, 'fail', 'validation failed');
      logAndContinue();
      skipped.push({
        id: event.event_id,
        reason: `validation failed: ${(error as Error).message}`,
      });
      console.warn(
        `Validation for ${event.event_id} failed:`,
        (error as Error).message,
      );
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
): Promise<Activity | null> {
  const place = event.schedules?.[0]?.place;
  const coords = await resolveCoordinates(place);
  const addressLine = buildAddressLine(place);
  const areaCandidates = mapAddressToAreas(addressLine);
  const area = areaCandidates[areaCandidates.length - 1] ?? 'Greater London';
  const imageUrl = event.images?.[0]?.url;
  const location: SharedLocation = {
    addressLine,
    postcode: place?.postal_code ?? '',
    city: place?.town ?? 'London',
    country: 'United Kingdom',
    lat: coords.lat,
    lng: coords.lng,
    area,
  };

  const dates = computeDatesFromRules(event, llmData, scrapedPages, location);
  if (!dates) {
    return null;
  }

  let bestUrl = scrapedOfficialUrl || officialUrl;
  if (scrapedPages.length > 0) {
    const bestPage = scrapedPages.reduce((prev, curr) => (curr.url.length > prev.url.length ? curr : prev));
    if (bestPage.url.length > bestUrl.length + 10) {
      bestUrl = bestPage.url;
    }
  }

  const baseActivity = {
    id: `${event.event_id}--${llmData.type}`,
    name: event.name,
    summary: llmData.summary,
    priceLevel: '££' as const,
    age: {
      officialAgeAvailable: llmData.officialAgeAvailable,
      minAge: llmData.minAge,
      maxAge: llmData.maxAge,
    },
    url: bestUrl,
    source: 'datathistle',
    lastUpdate: new Date().toISOString(),
    keywords: llmData.keywords,
    labels: llmData.labels,
    imageUrl,
  };

  if (llmData.type === 'event') {
    if (dates.kind !== 'event') {
      return null;
    }
    return { ...baseActivity, dates } as Activity;
  }

  if (dates.kind === 'event') {
    return null;
  }

  return { ...baseActivity, dates } as Activity;
}

function computeDatesFromRules(
  event: SourceEvent,
  llmData: ActivityLLMOutput,
  scrapedPages: PageScrapeResult[],
  location: SharedLocation,
): Dates | null {
  const performances = event.schedules?.flatMap((schedule) => schedule.performances ?? []) ?? [];
  const instances = performances
    .filter((p): p is SourcePerformance & { ts: string } => Boolean(p.ts))
    .map((p) => {
      const [date, time] = p.ts.split('T');
      const startTime = time.slice(0, 5);
      const duration = p.duration ?? 120;
      const endTime = addMinutesToTime(startTime, duration);
      return { date, startTime, endTime };
    })
    .filter(Boolean);

  if (llmData.type === 'event') {
    if (instances.length === 0) return null;
    return { kind: 'event', instances: instances.map((i) => ({ ...i, location })) };
  }

  if (llmData.type === 'place') {
    const extracted = extractOpeningHours(scrapedPages, []);
    if (extracted.source !== 'none' && extracted.openingHours && Object.keys(extracted.openingHours).length >= 4) {
      return { kind: 'ongoing', location, openingHours: extracted.openingHours, note: extracted.note };
    }
    return null;
  }

  return null;
}
