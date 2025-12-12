import * as chrono from 'chrono-node';

const EXCLUDE_PATTERN = /(expire|refund|policy|delivery|post|mail|discount|open day|working day)/i;

const customChrono = chrono.strict.clone();
customChrono.refiners.push({
  refine(context, results) {
    return results.filter((result) => {
      if (!result.start || !result.start.isCertain('hour')) return false;
      if (!result.text || result.text.length < 6) return false;
      const normalized = result.text.toLowerCase();
      if (EXCLUDE_PATTERN.test(normalized)) return false;
      return true;
    });
  },
});

export function parseChronoSegments(
  text: string,
  referenceDate: Date,
): chrono.ParsedResult[] {
  return customChrono.parse(text, referenceDate, { forwardDate: true });
}

export function parseChronoDate(
  text: string,
  referenceDate: Date,
): Date | null {
  const parsed = customChrono.parseDate(text, referenceDate, { forwardDate: true });
  return parsed ?? null;
}
