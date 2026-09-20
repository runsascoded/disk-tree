import { describe, expect, it } from 'vitest'
import { barControls } from './pageBar'

describe('barControls', () => {
  it('cw store (plan-first marks, no attribution, no classes, no read logs): marks / written / tree, marks filter, path filter', () => {
    expect(barControls({ marks: true, owners: false, hasAttr: false, classes: false, readRange: false })).toEqual({
      color: ['marks', 'date', 'tree'],
      shade: false,
      classes: false,
      marksFilter: true,
      ownerFilter: false,
      pathFilter: true,
    })
  })
  it('cw store, anonymous viewer (no ledger): written / tree and the path filter only', () => {
    expect(barControls({ marks: false, owners: false, hasAttr: false, classes: false, readRange: false })).toEqual({
      color: ['date', 'tree'],
      shade: false,
      classes: false,
      marksFilter: false,
      ownerFilter: false,
      pathFilter: true,
    })
  })
  it('gcs store with a rich scan: every control', () => {
    expect(barControls({ marks: true, owners: true, hasAttr: true, classes: true, readRange: true })).toEqual({
      color: ['marks', 'read', 'user', 'date', 'tree'],
      shade: true,
      classes: true,
      marksFilter: true,
      ownerFilter: true,
      pathFilter: true,
    })
  })
  it('gcs store, a scan without attribution or read logs: owner axes fall away, the rest stays', () => {
    expect(barControls({ marks: true, owners: true, hasAttr: false, classes: true, readRange: false })).toEqual({
      color: ['marks', 'date', 'tree'],
      shade: true,
      classes: true,
      marksFilter: true,
      ownerFilter: false,
      pathFilter: true,
    })
  })
})
