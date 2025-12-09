import fs from 'fs/promises';
import path from 'path';
import { TIMEOUT_MS, clampAge, RETRY_DELAY_MS } from './utils.js';
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

Return ONLY a valid JSON object with exactly these fields. No markdown, no explanations.

CRITICAL:
- If NONE of the pages describe "${eventName}", set "contentMatchesDescription": false and return only that field.
- Otherwise it MUST be true.

FIELD DEFINITIONS:

"type": "event" | "place"
  → "event" = one-off shows, film screenings, workshops, Christmas grottos, timed tickets with specific dates
  → "place" = permanent attractions, museums, zoos, soft play, aquariums, adventure lands open daily/weekly

"summary": string, 20–40 words, exciting and family-focused

"officialAgeAvailable": boolean
  → true only if site explicitly states minimum/recommended age

"minAge" and "maxAge": integers 0–18
  → use official range; if none → officialAgeAvailable: false, minAge: 0, maxAge: 18

"keywords": array of 27–35 lowercase single words (we trim to 20–30)

"labels": exactly 3 short labels, e.g. ["indoor", "theatre", "christmas"]

Example for Shrek's Adventure:
{
  "contentMatchesDescription": true,
  "type": "place",
  "summary": "Step into Shrek’s swamp for a hilarious 4D bus ride, meet Donkey and Puss in Boots, and navigate the Mirror Maze of Insanity in this fully immersive Far Far Away adventure perfect for ogre-sized family fun.",
  "officialAgeAvailable": true,
  "minAge": 3,
  "maxAge": 18,
  "keywords": ["shrek", "donkey", "fiona", "4d", "bus", "mirror", "maze", "interactive", "dreamworks", "farfaraway", ...],
  "labels": ["indoor", "interactive", "character"]
}`;

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
    temperature: 0.2,
    max_tokens: 1500,
    response_format: { type: 'json_object' },
  };

  await exportGroqInteraction(eventId, requestPayload, undefined, attempt > 1, attempt);
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
    await exportGroqInteraction(eventId, requestPayload, responseLog, attempt > 1, attempt);

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
  payload: unknown,
  response: unknown,
  isRetry = false,
  attempt = 1,
): Promise<void> {
  const dir = path.resolve(process.cwd(), 'groq-logs');
  try {
    await fs.mkdir(dir, { recursive: true });
    const filename = `groq-${eventId}--attempt${attempt}--${Date.now()}.json`;
    const filePath = path.join(dir, filename);
    const entry = {
      eventId,
      timestamp: new Date().toISOString(),
      attempt,
      isRetry,
      request: payload,
      response: response ?? null,
      success: response != null,
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
  const contentMatchesDescription = candidate.contentMatchesDescription;
  if (typeof contentMatchesDescription !== 'boolean') {
    throw new Error('LLM output missing contentMatchesDescription boolean.');
  }

  if (!contentMatchesDescription) {
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
    throw new Error('LLM output missing type or invalid type.');
  }
  const summary = candidate.summary;
  if (typeof summary !== 'string') {
    throw new Error('LLM output missing summary.');
  }
  const summaryText = summary.trim();
  const wordCount = summaryText
    .split(/\s+/)
    .filter((word) => word.length > 0).length;
  if (wordCount < 20 || wordCount > 40) {
    throw new Error(`Summary must be 20–40 words but got ${wordCount}.`);
  }

  const officialAgeAvailable = candidate.officialAgeAvailable;
  if (typeof officialAgeAvailable !== 'boolean') {
    throw new Error('LLM output missing officialAgeAvailable boolean.');
  }
  const minAge = clampAge(candidate.minAge);
  const maxAge = clampAge(candidate.maxAge);
  if (minAge > maxAge) {
    throw new Error(`minAge (${minAge}) cannot exceed maxAge (${maxAge}).`);
  }

  const keywordsRaw = Array.isArray(candidate.keywords) ? candidate.keywords : [];
  const normalizedKeywords = Array.from(
    new Set(
      keywordsRaw
        .map((keyword) => String(keyword ?? '').toLowerCase().trim())
        .filter((value) => value.length > 0),
    ),
  );
  if (normalizedKeywords.length < 27 || normalizedKeywords.length > 35) {
    throw new Error(`Expected 27–35 keywords but got ${normalizedKeywords.length}.`);
  }
  const trimmedKeywords = normalizedKeywords.slice(0, 30);

  const labelsRaw = Array.isArray(candidate.labels) ? candidate.labels : [];
  const normalizedLabels = labelsRaw
    .map((label) => String(label ?? '').trim())
    .filter((value) => value.length > 0);
  if (normalizedLabels.length !== 3) {
    throw new Error(`Expected 3 labels but got ${normalizedLabels.length}.`);
  }

  return {
    contentMatchesDescription: true,
    type,
    summary: summaryText,
    officialAgeAvailable,
    minAge,
    maxAge,
    keywords: trimmedKeywords,
    labels: normalizedLabels,
  };
}

function buildPrompt(
  event: SourceEvent,
  scrapedPages: PageScrapeResult[],
  rawScheduleSummary: string,
): string {
  const tags = event.tags?.join(', ') ?? 'None';
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
      return llmOutput;
    } catch (error) {
      console.warn(`Groq attempt ${attempt} for ${event.event_id} failed:`, (error as Error).message);
      if (attempt < MAX_GROQ_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
      }
    }
  }
  console.warn(`Groq failed after ${MAX_GROQ_ATTEMPTS} attempts for ${event.event_id} — SKIPPING EVENT`);
  await exportGroqInteraction(
    event.event_id,
    { fallback: true, officialUrl: scrapedPages[0]?.url ?? '' },
    null,
    true,
    MAX_GROQ_ATTEMPTS + 1,
  );
  return null;
}
