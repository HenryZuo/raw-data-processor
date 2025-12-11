import * as chrono from 'chrono-node';

export interface ParsedHours {
  open: string;
  close: string;
}

export interface ClosedDay {
  closed: true;
}

export type DayHours = ParsedHours | ClosedDay;

/**
 * Extracts opening hours from noisy scraped text such as:
 *   "Opening times today: 11am - 3pm"
 *   "Open 10:00–16:00"
 *   "Closed today"
 *   "From 9am until 5pm"
 */
export function extractHoursFromText(text: string): DayHours | null {
  const cleaned = text.toLowerCase().replace(/\s+/g, ' ').trim();

  // Quick closed detection
  if (/\bclosed\b/.test(cleaned)) {
    return { closed: true };
  }

  // Very permissive range pattern – works for all dashes and keywords
  const pattern =
    /(?:opening?.*?times?[:\s]*|open[:\s]*|from\s+|opens?\s+at\s+)?(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s*(?:[—–\-to|until]+)\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)/i;
  const match = cleaned.match(pattern);

  if (!match) return null;

  const [, startStr, endStr] = match;

  const parse = (s: string): string | null => {
    const withMinutes = s.includes(':') ? s : `${s}:00`;
    const date = chrono.parseDate(`2025-01-01 ${withMinutes}`);
    if (!date) return null;
    return `${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;
  };

  const open = parse(startStr);
  const close = parse(endStr);

  if (open && close) {
    return { open, close };
  }
  return null;
}
