import { describe, expect, it } from 'vitest'
import { fmtBytes, realGate, renderParent, runEvent, type RunRow } from './stagedSlack.js'

const run = (o: Partial<RunRow>): RunRow => ({
  run_id: 'cw-sweep-dry-1', mode: 'dry', scan: '2026-09-28T1201', actor: 'ann@openathena.ai', started_ts: 100,
  finished_ts: 200, deleted_bytes: 2 * 1024 ** 4, deleted_objects: 1234, skipped_gone: 0, skipped_overwritten: 0,
  plan_digest: 'D1', undo_deadline: null, ...o,
})

describe('realGate', () => {
  it('needs a finished dry-run of exactly the current set, and nothing in flight', () => {
    const dry = run({})
    expect(realGate([], 'D1', 0)).toEqual({ ok: false, reason: 'the plan is empty' })
    expect(realGate([], 'D1', 3)).toEqual({ ok: false, reason: 'no dry-run of this plan yet' })
    expect(realGate([dry], 'D2', 3)).toEqual({ ok: false, reason: 'the plan changed since the last dry-run; dry-run it again' })
    expect(realGate([run({ finished_ts: null })], 'D1', 3)).toEqual({ ok: false, reason: 'a dry run is in progress (cw-sweep-dry-1)' })
    expect(realGate([dry], 'D1', 3)).toEqual({ ok: true, dry })
    // the newest matching dry-run wins
    const newer = run({ run_id: 'cw-sweep-dry-2', started_ts: 300, finished_ts: 400, deleted_bytes: 5 })
    expect(realGate([dry, newer], 'D1', 3)).toEqual({ ok: true, dry: newer })
  })
})

const ids = (blocks: unknown[]): string[] =>
  ((blocks[2] as { elements: { action_id: string }[] }).elements).map(e => e.action_id)

describe('renderParent', () => {
  const base = { planId: 7, siteUrl: 'https://cw-s3.oa.dev', items: 3, batches: 2, stagers: ['ann@openathena.ai', 'bo@coreweave.com'], digest: 'D1', actions: true, closed: false }

  it('offers Delete for real only once a matching dry-run has finished', () => {
    expect(ids(renderParent({ ...base, runs: [] }).blocks)).toEqual(['staged_open', 'staged_dry'])
    expect(ids(renderParent({ ...base, runs: [run({ plan_digest: 'OLD' })] }).blocks)).toEqual(['staged_open', 'staged_dry'])
    expect(ids(renderParent({ ...base, runs: [run({})] }).blocks)).toEqual(['staged_open', 'staged_dry', 'staged_real'])
  })

  it('shows only the www link when dispatch is not wired, the plan is closed, or empty', () => {
    expect(ids(renderParent({ ...base, runs: [run({})], actions: false }).blocks)).toEqual(['staged_open'])
    expect(ids(renderParent({ ...base, runs: [run({})], closed: true }).blocks)).toEqual(['staged_open'])
    expect(ids(renderParent({ ...base, runs: [], items: 0 }).blocks)).toEqual(['staged_open'])
  })

  it('says whether the latest dry-run still matches', () => {
    const txt = (runs: RunRow[]): string => (renderParent({ ...base, runs }).blocks[1] as { text: { text: string } }).text.text
    expect(txt([])).toBe('No dry-run yet.\n_Delete for real_ appears after a finished dry-run of the current set (no dry-run of this plan yet).')
    expect(txt([run({})])).toBe('Latest dry-run would delete *2.0 TiB* / 1,234 objects (scan 2026-09-28T1201) — matches the current plan.')
    expect(txt([run({ plan_digest: 'OLD' })])).toBe('Latest dry-run would delete *2.0 TiB* / 1,234 objects (scan 2026-09-28T1201) — *stale*: the plan changed since.\n_Delete for real_ appears after a finished dry-run of the current set (the plan changed since the last dry-run; dry-run it again).')
  })

  it('puts the dry-run numbers and the recoverability in the real-delete confirm', () => {
    const real = (renderParent({ ...base, runs: [run({})] }).blocks[2] as { elements: Record<string, unknown>[] }).elements[2]
    expect(real.value).toBe('7:D1')
    expect((real.confirm as { text: { text: string } }).text.text).toBe('Deletes the 3 staged prefixes: 2.0 TiB / 1,234 objects per the dry-run on scan 2026-09-28T1201. Recoverable for 7 days (undo in www).')
  })
})

describe('run events', () => {
  it('reads as a sentence per phase', () => {
    expect(runEvent(run({ finished_ts: null }), 'dispatched', 'Slack')).toBe(':test_tube: Dry-run dispatched by ann via Slack on scan 2026-09-28T1201 (`cw-sweep-dry-1`)')
    expect(runEvent(run({ skipped_gone: 2 }), 'finished')).toBe(':test_tube: Dry-run finished: would delete *2.0 TiB* / 1,234 objects (gone since scan: 2, overwritten: 0).')
    expect(runEvent(run({ mode: 'real', run_id: 'cw-sweep-real-1', undo_deadline: 1_790_604_800 }), 'finished')).toBe(':white_check_mark: Real deletion finished: deleted *2.0 TiB* / 1,234 objects; undoable until 2026-09-28 14:13Z (www).')
    expect([fmtBytes(0), fmtBytes(1536), fmtBytes(3 * 1024 ** 3)]).toEqual(['0 B', '1.5 KiB', '3.0 GiB'])
  })
})
