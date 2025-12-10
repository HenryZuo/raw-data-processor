import assert from 'node:assert';
import { extractDatesAndTimesFromPage } from '../src/utils.ts';

function assertChronoResult(result: ReturnType<typeof extractDatesAndTimesFromPage>) {
  if (result.source !== 'chrono') {
    throw new Error('expected chrono result');
  }
  return result;
}

function expectOpeningHours(result: ReturnType<typeof extractDatesAndTimesFromPage>) {
  const chronoResult = assertChronoResult(result);
  assert(chronoResult.openingHours && Object.keys(chronoResult.openingHours).length > 0, 'expected opening hours');
  return chronoResult;
}

function expectEventInstances(result: ReturnType<typeof extractDatesAndTimesFromPage>) {
  const chronoResult = assertChronoResult(result);
  assert(chronoResult.eventInstances && chronoResult.eventInstances.length > 0, 'expected event instances');
  return chronoResult;
}

(() => {
  const shrekText = `
Wednesday 10th: 10am - 3pm
Thursday 11th - Sunday 14th: 10am - 5pm
  `;
  const result = extractDatesAndTimesFromPage(shrekText, 'https://www.shreksadventure.com/plan-your-visit/before-you-visit/opening-hours/');
  const chronoShrek = expectOpeningHours(result);
  assert(chronoShrek.openingHours?.Wed?.open === '10:00');
  assert(chronoShrek.openingHours?.Thu?.close === '17:00');
})();

(() => {
  const showText = `
Dec 12th 2025 from 7:30pm to 9:30pm
Dec 13th 2025 from 6:00pm to 7:15pm
  `;
  const result = extractDatesAndTimesFromPage(showText, 'https://example.com/shows');
  const chronoShow = expectEventInstances(result);
  assert((chronoShow.eventInstances?.length ?? 0) >= 2);
  assert(chronoShow.eventInstances?.[0].startTime === '19:30');
})();

(() => {
  const dailyText = 'Closed Mondays. Open daily 10am - 5pm except 25 Dec.';
  const result = extractDatesAndTimesFromPage(dailyText, 'https://example.com/hours');
  const chronoDaily = expectOpeningHours(result);
  assert(chronoDaily.openingHours?.Tue?.open === '10:00');
  assert(chronoDaily.openingHours?.Sun?.close === '17:00');
})();

(() => {
  const noneText = 'This page does not include any dates or times.';
  const result = extractDatesAndTimesFromPage(noneText, 'https://example.com/empty');
  assert.deepStrictEqual(result, { source: 'none' });
})();

console.log('chrono-node extraction tests passed.');
