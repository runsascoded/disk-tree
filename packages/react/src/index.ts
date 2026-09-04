// The treemap core + primitives now live in `@rdub/treemap`. This package
// re-exports them so existing `@disk-tree/react` consumers keep working
// unchanged; new/non-disk consumers should depend on `@rdub/treemap` directly.
export * from '@rdub/treemap'

// disk-flavored widgets (bytes / mtime / age-year domain) built on the core.
export { StalenessScatter } from './StalenessScatter'
export type { StalenessScatterProps } from './StalenessScatter'
export { AgeHistograms } from './AgeHistograms'
export type { AgeHistogramsProps } from './AgeHistograms'
export { bytesOlderThan, peakBin, timeTicks, totalBytes } from './histogram'
export { formatTbYears, pow10, SEC_PER_YEAR, sumTbYears, TB } from './stats'
export {
  decadesBetween,
  isoScoreDecades,
  isoScoreSegment,
  isoScoresForData,
  logDomain,
  logPos,
  logTicks,
  radiusFor,
} from './scatter'
export { BytesOverTime, TimeSeries } from './TimeSeries'
export type { Series, TimeSeriesProps } from './TimeSeries'
