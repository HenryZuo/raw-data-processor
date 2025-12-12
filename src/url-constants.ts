export const OFFICIAL_SIGNAL_REGEX = /official|book tickets|opening times|visit us|family|kids|age \d|merlin|©/i;
export const AGGREGATOR_REGEX =
  /ticketmaster|eventbrite|timeout\.com|visitlondoncom|datathistle|seetickets|axs|ticketweb|skiddle|londonboxoffice|musicalsontour|lovetheatre|westendtheatre|londontheatredirect|londontheatre|atgtickets|todaytix|london-theater-tickets|seatplan|officiallondontheatre|twickets|ticketstosee|longstandingtickets|secondarymarket/i;
export const PREFERRED_VENUE_REGEX =
  /oldvictheatre|garricktheatre|apollo-theatre|thedinosaurthatpooped|maddiemoate|nationaltheatre|royalcourttheatre|donmarwarehouse|almeida|barbican|youngvic|sadlerswells|royalballettandopera|lyceumtheatre|lwtheatres|nimax|delfontmackintosh|shakespeare'sglobe|southbankcentre|haroldpintertheatre|dominiontheatre|palacetheatre|sondheimtheatre|adelphi|aldwych|cambridge|dukeofyorks|gilliantheatre|noelcoward|novello|phoenix|princeedward|savoy|trafalgar|wyndhams/i;

export const SERP_API_BLACKLIST = [
  'ticketmaster',
  'ticketmaster.co.uk',
  'ticketmaster.com',
  'eventbrite',
  'seetickets',
  'timeout',
  'datathistle',
  'axs',
  'ticketweb',
  'skiddle',
  'todaytix',
  'seatplan',
  'twickets',
  'dayoutwiththekids.co.uk',
  'visitlondon.com',
  'tripadvisor',
  'wikipedia',
  'youtube',
  'facebook',
  'yelp',
  'londonboxoffice.co.uk',
  'musicalsontour.co.uk',
  'lovetheatre.com',
];
