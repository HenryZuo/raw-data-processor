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

export interface SourceSchedule {
  place_id: string;
  start_ts?: string;
  end_ts?: string;
  tags?: string[];
  place?: SourcePlace;
  performances?: SourcePerformance[];
  links?: SourceLink[];
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
  };
}
