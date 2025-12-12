import type { Exception as SharedException } from '../../london-kids-p1/packages/shared/src/activity.js';

export const DAY_LABELS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] as const;
export type DayLabel = typeof DAY_LABELS[number];

export interface JsonLdHoursResult {
  hours: Partial<Record<DayLabel, { open: string; close: string } | { closed: true }>>;
  exceptions?: SharedException[];
}

export interface JsonLdEvent {
  startDate?: string;
  endDate?: string;
  startTime?: string;
  doorTime?: string;
  eventStatus?: string;
  name?: string;
  endTime?: string;
}

export interface SourceLink {
  url: string;
  type?: string;
}

export interface SourceTicket {
  type?: string;
  currency?: string;
  min_price?: number | null;
  max_price?: number | null;
  description?: string;
}

export interface SourcePerformance {
  ts: string;
  time_unknown?: boolean | null;
  duration?: number | null;
  tickets?: SourceTicket[];
  links?: SourceLink[];
}

export interface SourcePlace {
  name?: string;
  address?: string;
  town?: string;
  postal_code?: string;
  lat?: number | null;
  lon?: number | null;
  lng?: number | null;
}

export interface RawDateTimeInstance {
  date: string;
  startTime?: string;
  endTime?: string;
  location?: string;
  note?: string;
}

export interface SourceSchedule {
  place_id: string;
  start_ts?: string;
  end_ts?: string;
  tags?: string[];
  place?: SourcePlace;
  performances?: SourcePerformance[];
  links?: SourceLink[];
  tickets?: SourceTicket[];
}

export interface SourceEvent {
  event_id: string;
  name: string;
  schedules?: SourceSchedule[];
  tags?: string[];
  descriptions?: { type: string; description: string }[];
  links?: SourceLink[];
  website?: string;
  images?: { url: string }[];
}

export interface PageScrapeResult {
  url: string;
  title: string;
  text: string;
  structured: {
    description?: string;
    priceText?: string;
    ageText?: string;
    openingHoursText?: string;
    addressText?: string;
    extractedHours?: string;
    extractedAge?: string;
    extractedPrice?: string;
    extractedDescription?: string;
    jsonLdHours?: JsonLdHoursResult;
    jsonLdEvents?: JsonLdEvent[];
  };
  html?: string;
  deepDateTimeData?: RawDateTimeInstance[];
}
