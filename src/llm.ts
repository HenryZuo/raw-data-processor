import fs from 'fs/promises';
import path from 'path';
import {
  TIMEOUT_MS,
  clampAge,
  RETRY_DELAY_MS,
  formatExportTimestamp,
  sanitizeNameForFilename,
  cleanText,
} from './utils.js';
import type { PageScrapeResult, SourceEvent } from './types.js';
import type { Dates } from '../../london-kids-p1/packages/shared/src/activity.js';

export interface ActivityLLMOutput {
  contentMatchesDescription: boolean;
  type: 'event' | 'place';
  summary: string;
  officialAgeAvailable: boolean;
  minAge: number;
  maxAge: number;
  keywords: string[];
  labels: string[];
}

const MAX_GROQ_ATTEMPTS = 2;

const RULES_TEXT = (eventName: string) => `You are an expert London family-activities curator.

Return ONLY a valid JSON object. No markdown, no explanations.

CRITICAL MATCH CHECK:
- Strictly compare scraped pages to raw name, tags, schedules, AND descriptions.
- Look for mismatches in activity type (e.g., film vs. live performance), format (e.g., screening vs. workshop), details (e.g., key participants or themes), or venue.
- If ANY core mismatch exists, set "contentMatchesDescription": false and return ONLY { "contentMatchesDescription": false }.
- Mismatch examples:
  • Raw: "comedy film screening" vs. Pages: "live stage musical" → false
- If NONE of the pages describe "${eventName}", set "contentMatchesDescription": false.

CRITICAL CLASSIFICATION (only if contentMatchesDescription === true):
- "type": "place" → ONLY if scraped pages clearly show reliable ongoing opening hours (e.g. "Open daily 10am–6pm", "Mon–Sun 9:30–17:30", visible table, JSON-LD openingHoursSpecification, etc.).
- If opening hours are missing, hidden in JS widgets, or not visible → classify as "event" (even if it's a permanent attraction like a zoo or museum).
- NEVER classify as "place" just because raw DataThistle performances exist.

"type": "event" | "place"
  → "event" = one-off shows, workshops, timed tickets, OR permanent venues without visible ongoing hours
  → "place" = permanent attractions with clear ongoing opening hours only

"summary": string, 25–45 words — exciting, family-focused, enticing!

"officialAgeAvailable": boolean (true only if site explicitly states age range like "suitable for ages 5–12")

"minAge" / "maxAge": integers 0–18
  → If officialAgeAvailable === true → use the exact numbers stated.
  → If officialAgeAvailable === false → give your best RECOMMENDED realistic age range for this activity (do NOT use 0–18).
  
"keywords": array of 32–50 lowercase single words (give plenty, we’ll trim)

"labels": exactly 3 short labels, e.g. ["indoor", "theatre", "christmas"]`;

const GROQ_MODEL = process.env.GROQ_MODEL ?? 'llama-3.3-70b-versatile';
const DEFAULT_GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions';

export async function callGroqAPI(
  prompt: string,
  apiKey: string,
  eventId: string,
  attempt: number,
  eventName: string,
): Promise<ActivityLLMOutput> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);

  const requestPayload = {
    model: GROQ_MODEL,
    messages: [
      {
        role: 'system',
        content: RULES_TEXT(eventName),
      },
      {
        role: 'user',
        content: prompt,
      },
    ],
    temperature: 0.1,
    max_tokens: 1500,
    response_format: { type: 'json_object' },
  };

  try {
    const response = await fetch(process.env.GROQ_API_URL ?? DEFAULT_GROQ_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(requestPayload),
      signal: controller.signal,
    });

    const responseText = await response.text();
    const responseLog = { status: response.status, body: responseText };
    await exportGroqInteraction(
      eventId,
      eventName,
      requestPayload,
      responseLog,
      attempt > 1,
      attempt,
    );

    if (!response.ok) {
      console.error('Groq error:', responseText);
      throw new Error(`Groq ${response.status}: ${responseText}`);
    }

    let payload: unknown;
    try {
      payload = JSON.parse(responseText);
    } catch (error) {
      throw new Error(`Failed to parse Groq response: ${(error as Error).message}`);
    }

    const content = (payload as any).choices?.[0]?.message?.content;
    if (!content) {
      throw new Error('Groq response contained no content');
    }

    const parsed = typeof content === 'string' ? JSON.parse(content) : content;
    return normalizeLLMOutput(parsed);
  } finally {
    clearTimeout(timeout);
  }
}

export async function exportGroqInteraction(
  eventId: string,
  eventName: string,
  payload: unknown,
  response: unknown,
  isRetry = false,
  attempt = 1,
): Promise<void> {
  const dir = path.resolve(process.cwd(), 'groq-logs');
  try {
    await fs.mkdir(dir, { recursive: true });
    const timestamp = formatExportTimestamp();
    const nameSegment = sanitizeNameForFilename(eventName);
    const filename = `groq-${timestamp}-${nameSegment}-attempt${attempt}.json`;
    const filePath = path.join(dir, filename);
    const entry = {
      eventId,
      eventName,
      timestamp: new Date().toISOString(),
      attempt,
      isRetry,
      success: response != null,
      request: payload,
      response: response ?? null,
    };
    await fs.writeFile(filePath, `${JSON.stringify(entry, null, 2)}\n`, 'utf-8');
  } catch (error) {
    console.warn('Failed to export Groq interaction:', (error as Error).message);
  }
}

export function normalizeLLMOutput(raw: unknown): ActivityLLMOutput {
  if (!raw || typeof raw !== 'object') {
    throw new Error('LLM output is not an object.');
  }

  const candidate = raw as Record<string, unknown>;
  if (candidate.contentMatchesDescription !== true) {
    return {
      contentMatchesDescription: false,
      type: 'event',
      summary: '',
      officialAgeAvailable: false,
      minAge: 0,
      maxAge: 0,
      keywords: [],
      labels: [],
    };
  }

  const type = candidate.type;
  if (type !== 'event' && type !== 'place') {
    throw new Error('Invalid type from LLM');
  }

  const summaryRaw = String(candidate.summary ?? '').trim();
  const wordCount = summaryRaw
    .split(/\s+/)
    .filter((word) => word.length > 0).length;
  if (wordCount < 10) {
    throw new Error(`Summary too short: ${wordCount} words`);
  }
  const summary = summaryRaw;

  const officialAgeAvailable = Boolean(candidate.officialAgeAvailable);
  const minAge = clampAge(officialAgeAvailable ? candidate.minAge ?? 0 : 0);
  const maxAge = clampAge(officialAgeAvailable ? candidate.maxAge ?? 18 : 18);

  const rawKeywords = Array.isArray(candidate.keywords) ? candidate.keywords : [];
  let keywords = rawKeywords
    .map((keyword) => String(keyword ?? '').toLowerCase().trim())
    .filter((value) => value.length > 1);

  if (keywords.length < 20) {
    console.warn(`LLM gave only ${keywords.length} keywords → padding to 20`);
    const fillers = [
      'london',
      'family',
      'kids',
      'children',
      'fun',
      'activity',
      'england',
      'uk',
      'attraction',
      'dayout',
      'experience',
      'adventure',
      'interactive',
      'educational',
      'outdoor',
      'indoor',
      'play',
      'learn',
      'explore',
      'discover',
    ];
    while (keywords.length < 20) {
      const filler =
        fillers[keywords.length % fillers.length];
      if (!keywords.includes(filler)) {
        keywords.push(filler);
      } else {
        keywords.push(`${filler}-${keywords.length}`);
      }
    }
  }
  const trimmedKeywords = keywords.slice(0, 30);

  const rawLabels = Array.isArray(candidate.labels) ? candidate.labels : [];
  let labels = rawLabels
    .map((label) => String(label ?? '').trim())
    .filter((value) => value.length > 0)
    .slice(0, 3);

  const labelFallbacks = ['indoor', 'outdoor', 'interactive'];
  while (labels.length < 3) {
    labels.push(labelFallbacks[labels.length]);
  }

  return {
    contentMatchesDescription: true,
    type,
    summary,
    officialAgeAvailable,
    minAge,
    maxAge,
    keywords: trimmedKeywords,
    labels,
  };
}

function buildPrompt(
  event: SourceEvent,
  scrapedPages: PageScrapeResult[],
  rawScheduleSummary: string,
): string {
  const tags = event.tags?.join(', ') ?? 'None';
  const rawDescriptions = formatRawDescriptions(event);
  const pageBlocks = scrapedPages
    .map((page, index) => {
      const extracts = [
        page.structured.extractedHours && `HOURS → ${page.structured.extractedHours}`,
        page.structured.extractedAge && `AGE → ${page.structured.extractedAge}`,
        page.structured.extractedPrice && `PRICE → ${page.structured.extractedPrice}`,
        page.structured.extractedDescription && `DESCRIPTION → ${page.structured.extractedDescription}`,
      ]
        .filter(Boolean)
        .join('\n\n');
      return `=== Page ${index + 1} – ${page.title} (${page.url}) ===
Extracted key sections (use these first):
${extracts || 'No strong extracts'}

Full page text (first 4000 chars):
${page.text.slice(0, 4000)}`;
    })
    .join('\n\n');
  const overview = [
    `Raw event descriptions (MUST compare to scraped pages for match):\n${rawDescriptions}`,
    `Activity name: ${event.name}`,
    `Tags: ${tags}`,
    `Raw schedule data from DataThistle:\n${rawScheduleSummary || 'None'}`,
    `You are receiving the TOP ${scrapedPages.length} MOST RELEVANT pages from the official website (already ranked by relevance).`,
    `Each page contains both the full text (first 4000 chars) AND focused extracted sections (hours, age, price, description).`,
    `Synthesise information ACROSS ALL pages. Prioritise the extracted sections when they exist.`,
    `If NONE of the pages describe the activity "${event.name}", set "contentMatchesDescription": false. Otherwise it MUST be true.`,
  ];
  return [...overview, pageBlocks].join('\n\n');
}

function formatRawDescriptions(event: SourceEvent): string {
  if (!event.descriptions || event.descriptions.length === 0) {
    return 'None';
  }
  return event.descriptions
    .map((desc) => {
      const cleaned = cleanText(desc.description);
      const truncated =
        cleaned.length > 2000 ? `${cleaned.slice(0, 2000)} [truncated]` : cleaned;
      return `Type: ${desc.type}\nDescription: ${truncated}`;
    })
    .join('\n\n');
}

const POST_LLM_CONFLICTS = [
  { raw: ['film', 'movie', 'screening', 'cinema'], llm: ['theatre', 'musical', 'stage', 'drama'] },
  { raw: ['workshop', 'class', 'hands-on'], llm: ['exhibition', 'gallery', 'installation', 'art'] },
  { raw: ['outdoor', 'park', 'walk', 'nature'], llm: ['indoor', 'gallery', 'museum', 'show'] },
];

function hasPostLLMConflict(event: SourceEvent, output: ActivityLLMOutput): boolean {
  const rawTags = new Set((event.tags ?? []).map((tag) => tag.toLowerCase()));
  const llmTerms = new Set(
    [...output.keywords, ...output.labels]
      .map((term) => String(term ?? '').toLowerCase().trim())
      .filter((term) => term.length > 0),
  );
  for (const conflict of POST_LLM_CONFLICTS) {
    if (
      conflict.raw.some((tag) => rawTags.has(tag)) &&
      conflict.llm.some((term) => llmTerms.has(term))
    ) {
      return true;
    }
  }
  return false;
}

export async function fetchLLMDataWithRetries(
  event: SourceEvent,
  scrapedPages: PageScrapeResult[],
  rawScheduleSummary: string,
  apiKey: string,
): Promise<ActivityLLMOutput | null> {
  const prompt = buildPrompt(event, scrapedPages, rawScheduleSummary);
  for (let attempt = 1; attempt <= MAX_GROQ_ATTEMPTS; attempt += 1) {
    try {
      const llmOutput = await callGroqAPI(prompt, apiKey, event.event_id, attempt, event.name);
      if (llmOutput.contentMatchesDescription && hasPostLLMConflict(event, llmOutput)) {
        console.warn(`[${event.event_id}] Post-LLM mismatch override: Raw tags conflict with LLM output.`);
        return null;
      }
      return llmOutput;
    } catch (error) {
      console.warn(`Groq attempt ${attempt} for ${event.event_id} failed:`, (error as Error).message);
      if (attempt < MAX_GROQ_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
      }
    }
  }
  console.warn(`Groq failed after ${MAX_GROQ_ATTEMPTS} attempts for ${event.event_id} — SKIPPING EVENT`);
  return null;
}
