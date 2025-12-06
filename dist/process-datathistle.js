"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const promises_1 = __importDefault(require("fs/promises"));
const path_1 = __importDefault(require("path"));
const date_fns_1 = require("date-fns");
const activity_1 = require("../../london-kids-p1/packages/shared/src/activity");
const areas_1 = require("../../london-kids-p1/packages/shared/src/areas");
const SOURCE_FILE = 'datathistle json sample .json';
const MAX_RETRIES = 3;
const TIMEOUT_MS = 30000;
const RETRY_DELAY_MS = 2000;
const GROQ_MODEL = 'llama-3.3-70b-versatile-128k';
const DEFAULT_GROQ_URL = 'https://api.groq.com/v1/completions';
const GENERIC_KEYWORDS = [
    'London family activities',
    'kids days out',
    'family bonding London',
    'child-friendly London',
    'children adventures',
    'London heritage walks',
    'museum experiences for kids',
    'outdoor discovery London',
    'creative workshops London',
    'indoor play London',
    'London waterfront fun',
    'science and discovery',
    'theatre for families',
    'park picnics London',
    'interactive learning London',
    'hidden gems London',
    'new things to try London',
    'family-friendly shows',
    'mini explorers London',
    'London neighbourhood walks',
];
const FALLBACK_LABELS = ['family', 'kids', 'London'];
const RULES_TEXT = `You are an expert London family-activity curator. Return ONLY the JSON object above.

{
  "officialAgeAvailable": boolean,
  "minAge": number (0–18 only),
  "maxAge": number (0–18 only),
  "summary": string (maximum 30 words, first-person parent voice, upbeat and polished, must describe the activity + highlight one unique/exciting hook),
  "priceLevel": "free" | "£" | "££" | "£££",
  "keywords": array of exactly 20 search keywords/phrases,
  "labels": array of exactly the 3 most distinctive and powerful keywords from the 20 above
}

Age rules:
- If an official age range is explicitly stated anywhere, set officialAgeAvailable = true and use those exact numbers.
- Otherwise set officialAgeAvailable = false and recommend a realistic range for this type of activity (typical 0–16, never exceed 18).

Summary rules:
- Max 30 words (count them)
- First-person parent voice ("We're going to love...", "My kids will be thrilled...")
- Upbeat, warm, and polished — suitable for a public website
- Must (1) say what the activity is and (2) highlight one unique, rare, or exciting feature

PriceLevel rules (follow exactly):
- If explicit price in GBP is available:
    • £0 → "free"
    • £0.01 – £24.99 → "£"
    • £25.00 – £69.99 → "££"
    • £70.00 or higher → "£££"
- If price is "tbc", "from", missing, or unclear → make your best educated guess for this type of London attraction and apply the same buckets

Keywords & labels:
- Return exactly 20 relevant search terms (words or short phrases)
- From those 20, separately choose the 3 most specific and powerful as "labels"`;
const fallbackLLMOutput = (event) => {
    const safeNameSegment = event.name
        .trim()
        .replace(/\s+/g, '-')
        .replace(/[^A-Za-z0-9-]/g, '');
    const safeNameToken = safeNameSegment || 'activity';
    const summary = `My kids will delight in ${safeNameToken} and soak up its playful London energy with joyful, laughter-filled family time today together.`;
    return {
        officialAgeAvailable: false,
        minAge: 3,
        maxAge: 12,
        summary,
        priceLevel: '££',
        keywords: GENERIC_KEYWORDS,
        labels: FALLBACK_LABELS,
    };
};
const geocodeCache = new Map();
async function main() {
    const sourcePath = path_1.default.resolve(process.cwd(), SOURCE_FILE);
    const raw = await promises_1.default.readFile(sourcePath, 'utf-8');
    const events = JSON.parse(raw);
    const activities = [];
    const skipped = [];
    const groqApiKey = process.env.GROQ_API_KEY;
    if (!groqApiKey) {
        throw new Error('Set GROQ_API_KEY in the environment before running this processor.');
    }
    for (const event of events) {
        console.log(`Processing event ${event.event_id} (${event.name})…`);
        const llmData = await fetchLLMDataWithRetries(event, groqApiKey);
        const schedules = event.schedules ?? [];
        if (!schedules.length) {
            skipped.push({ id: event.event_id, reason: 'missing schedules' });
            continue;
        }
        for (const schedule of schedules) {
            const performances = schedule.performances ?? [];
            if (!performances.length) {
                skipped.push({ id: `${event.event_id}::${schedule.place_id}`, reason: 'schedule lacks performances' });
                continue;
            }
            for (const performance of performances) {
                try {
                    const activity = await buildActivity(event, schedule, performance, llmData);
                    activity_1.activitySchema.parse(activity);
                    activities.push(activity);
                }
                catch (error) {
                    skipped.push({
                        id: `${event.event_id}::${performance.ts}`,
                        reason: `validation failed: ${error.message}`,
                    });
                    console.warn(`Validation for ${event.event_id} at ${performance.ts} failed:`, error.message);
                }
            }
        }
    }
    const exportName = `processed-datathistle-${new Date().toISOString().slice(0, 10)}.json`;
    const exportPath = path_1.default.resolve(process.cwd(), exportName);
    await promises_1.default.writeFile(exportPath, `${JSON.stringify(activities, null, 2)}\n`, 'utf-8');
    console.log(`Wrote ${activities.length} valid activities to ${exportName}`);
    if (skipped.length) {
        console.log(`Skipped ${skipped.length} records (${skipped.map((item) => item.reason).join('; ')})`);
    }
    else {
        console.log('No records were skipped.');
    }
}
async function fetchLLMDataWithRetries(event, apiKey) {
    const prompt = buildPrompt(event);
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
        try {
            return await callGroqAPI(prompt, apiKey);
        }
        catch (error) {
            console.warn(`Groq attempt ${attempt} for ${event.event_id} failed:`, error.message);
            if (attempt < MAX_RETRIES) {
                await sleep(RETRY_DELAY_MS);
            }
        }
    }
    console.warn(`Falling back for ${event.event_id} after ${MAX_RETRIES} Groq attempts.`);
    return fallbackLLMOutput(event);
}
function buildPrompt(event) {
    const description = event.descriptions?.find((entry) => entry.type === 'default')?.description ?? '';
    const trimmedDescription = description.replace(/\s+/g, ' ').trim();
    const descriptionSnippet = trimmedDescription.length <= 400 ? trimmedDescription : `${trimmedDescription.slice(0, 397)}...`;
    const locationHints = event.schedules?.[0]?.place;
    const addressParts = [
        locationHints?.name,
        locationHints?.address,
        locationHints?.town,
        locationHints?.postal_code,
    ].filter(Boolean);
    const priceHint = gatherPriceHint(event);
    const ageHint = extractAgeHint(event);
    const performances = event.schedules?.flatMap((schedule) => schedule.performances ?? []) ?? [];
    const importantDates = performances
        .slice(0, 3)
        .map((performance) => new Date(performance.ts).toISOString().replace('T', ' ').replace(/\.\d+Z$/, ''))
        .join('; ');
    const bookingLink = findBookingLink(event);
    const tags = event.tags?.join(', ') ?? 'N/A';
    const eventDetails = [
        `Name: ${event.name}`,
        `Description: ${descriptionSnippet || 'N/A'}`,
        `Location notes: ${addressParts.join(', ') || 'London'}`,
        `Primary tags: ${tags}`,
        `Upcoming performances: ${importantDates || 'dates TBD'}`,
        `Price info: ${priceHint}`,
        `Booking link: ${bookingLink || 'N/A'}`,
        `Official age notes: ${ageHint || 'None provided'}`,
    ].join('\n');
    return `${RULES_TEXT}

Event data:
${eventDetails}`;
}
function gatherPriceHint(event) {
    const performances = event.schedules?.flatMap((schedule) => schedule.performances ?? []) ?? [];
    const firstPerformance = performances.find((performance) => performance.tickets && performance.tickets.length);
    if (!firstPerformance) {
        return 'Price details unavailable; assume typical London family attraction prices.';
    }
    const ticket = firstPerformance.tickets[0];
    const prices = [];
    if (ticket.min_price != null) {
        prices.push(`from £${ticket.min_price.toFixed(2)}`);
    }
    if (ticket.max_price != null && ticket.max_price !== ticket.min_price) {
        prices.push(`up to £${ticket.max_price.toFixed(2)}`);
    }
    if (!prices.length && ticket.description) {
        prices.push(ticket.description);
    }
    const typeLabel = ticket.type ? `${ticket.type} ticket` : 'Ticket';
    return `${typeLabel} ${prices.join(' / ') || 'pricing tbc (GBP)'} (${ticket.description ?? 'no extra note'})`;
}
function extractAgeHint(event) {
    const haystack = (event.descriptions ?? [])
        .map((entry) => entry.description)
        .concat(event.tags ?? [])
        .join(' ');
    const matchRange = haystack.match(/(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})/);
    if (matchRange) {
        return `Found explicit ages ${matchRange[1]}-${matchRange[2]} in source text.`;
    }
    const matchMin = haystack.match(/(\d{1,2})\s*\+/);
    if (matchMin) {
        return `Found suggested minimum age ${matchMin[1]}+.`;
    }
    return undefined;
}
function findBookingLink(event) {
    const schedules = event.schedules ?? [];
    const scheduleLinks = schedules.flatMap((schedule) => schedule.links ?? []);
    const performanceLinks = schedules
        .flatMap((schedule) => schedule.performances ?? [])
        .flatMap((performance) => performance.links ?? []);
    const linkSets = [...(event.links ?? []), ...scheduleLinks, ...performanceLinks];
    return linkSets.find((link) => link.type === 'booking')?.url ?? linkSets[0]?.url;
}
async function callGroqAPI(prompt, apiKey) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
    const response = await fetch(process.env.GROQ_API_URL ?? DEFAULT_GROQ_URL, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
            model: GROQ_MODEL,
            input: prompt,
            temperature: 0.0,
            max_tokens: 800,
        }),
        signal: controller.signal,
    });
    clearTimeout(timeout);
    if (!response.ok) {
        const errorBody = await response.text();
        throw new Error(`Groq API responded with ${response.status}: ${errorBody}`);
    }
    const payload = (await response.json());
    const rawOutput = payload.output?.[0]?.content ??
        payload.choices?.[0]?.message?.content ??
        payload.choices?.[0]?.text ??
        payload.text ??
        payload.output ??
        '';
    const text = typeof rawOutput === 'string'
        ? rawOutput.trim()
        : typeof rawOutput === 'object'
            ? JSON.stringify(rawOutput)
            : '';
    if (!text) {
        throw new Error('Groq response contained no usable text.');
    }
    let parsed;
    try {
        parsed = JSON.parse(text);
    }
    catch (error) {
        throw new Error(`Groq output could not be parsed as JSON (${error.message}).`);
    }
    return normalizeLLMOutput(parsed);
}
function normalizeLLMOutput(raw) {
    if (!raw || typeof raw !== 'object') {
        throw new Error('LLM output is not an object.');
    }
    const candidate = raw;
    const officialAgeAvailable = candidate.officialAgeAvailable;
    if (typeof officialAgeAvailable !== 'boolean') {
        throw new Error('LLM output missing officialAgeAvailable boolean.');
    }
    const keywords = Array.isArray(candidate.keywords) ? candidate.keywords : [];
    const labels = Array.isArray(candidate.labels) ? candidate.labels : [];
    if (keywords.length !== 20) {
        throw new Error(`Expected 20 keywords but got ${keywords.length}.`);
    }
    if (labels.length !== 3) {
        throw new Error(`Expected 3 labels but got ${labels.length}.`);
    }
    const minAge = clampAge(candidate.minAge);
    const maxAge = clampAge(candidate.maxAge);
    if (minAge > maxAge) {
        throw new Error(`minAge (${minAge}) cannot exceed maxAge (${maxAge}).`);
    }
    const priceLevel = candidate.priceLevel;
    if (priceLevel !== 'free' && priceLevel !== '£' && priceLevel !== '££' && priceLevel !== '£££') {
        throw new Error(`Invalid priceLevel: ${priceLevel}`);
    }
    const summary = candidate.summary;
    if (typeof summary !== 'string') {
        throw new Error('Summary must be a string.');
    }
    const normalizedPriceLevel = priceLevel;
    return {
        officialAgeAvailable,
        minAge,
        maxAge,
        summary,
        priceLevel: normalizedPriceLevel,
        keywords: keywords.map((keyword) => String(keyword).trim()),
        labels: labels.map((label) => String(label).trim()),
    };
}
function clampAge(value) {
    if (typeof value !== 'number' || Number.isNaN(value)) {
        throw new Error('Age value is not a number.');
    }
    return Math.min(18, Math.max(0, Math.round(value)));
}
async function buildActivity(event, schedule, performance, llmData) {
    const coords = await resolveCoordinates(schedule.place);
    const addressLine = buildAddressLine(schedule.place);
    const areas = (0, areas_1.mapAddressToAreas)(`${addressLine}`);
    const area = areas[areas.length - 1] ?? 'Greater London';
    const startDate = (0, date_fns_1.parseISO)(performance.ts);
    const date = (0, date_fns_1.format)(startDate, 'yyyy-MM-dd');
    const startTime = (0, date_fns_1.format)(startDate, 'HH:mm');
    const endTime = (0, date_fns_1.format)((0, date_fns_1.addHours)(startDate, determineDefaultDuration(schedule)), 'HH:mm');
    const bookingLink = findBookingLink(event) ?? `https://www.datathistle.com/details/${event.event_id}`;
    const imageUrl = event.images?.[0]?.url;
    const activity = {
        id: `${event.event_id}--${performance.ts.replace(/:/g, '-')}`,
        name: event.name,
        date,
        startTime,
        endTime,
        location: {
            locationName: schedule.place?.name ?? event.name,
            addressLine,
            postcode: schedule.place?.postal_code ?? '',
            city: schedule.place?.town ?? 'London',
            country: 'United Kingdom',
            lat: coords.lat,
            lng: coords.lng,
            area,
        },
        priceLevel: llmData.priceLevel,
        age: {
            officialAgeAvailable: llmData.officialAgeAvailable,
            minAge: llmData.minAge,
            maxAge: llmData.maxAge,
        },
        url: bookingLink,
        source: 'datathistle',
        lastUpdate: new Date().toISOString(),
        summary: llmData.summary,
        keywords: llmData.keywords,
        labels: llmData.labels,
        imageUrl,
        areas,
    };
    return activity;
}
function determineDefaultDuration(schedule) {
    const tags = [...(schedule.tags ?? []), ...(schedule.place?.name ? [schedule.place.name] : [])].join(' ');
    const normalizedTags = tags.toLowerCase();
    const isWalk = normalizedTags.includes('walk') || normalizedTags.includes('trail');
    return isWalk ? 2 : 4;
}
function buildAddressLine(place) {
    if (!place) {
        return 'London, United Kingdom';
    }
    const line = [place.address, place.town, place.postal_code].filter(Boolean).join(', ');
    return line || 'London, United Kingdom';
}
async function resolveCoordinates(place) {
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
        return geocodeCache.get(address);
    }
    try {
        const geo = await geocode(address);
        geocodeCache.set(address, geo);
        return geo;
    }
    catch (error) {
        console.warn(`Geocoding failed for "${address}":`, error.message);
        return { lat: 51.5074, lng: -0.1278 };
    }
}
async function geocode(query) {
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
    const payload = (await response.json());
    const first = payload[0];
    if (!first?.lat || !first?.lon) {
        throw new Error('Nominatim returned no coordinates');
    }
    return { lat: Number(first.lat), lng: Number(first.lon) };
}
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
void main().catch((error) => {
    console.error('Processing failed:', error);
    process.exitCode = 1;
});
