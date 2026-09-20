import { describe, expect, it } from 'vitest'
import { HELP_EMPTY, helpReducer, helpText } from './helpState'
import type { HelpEvent, HelpState } from './helpState'

const run = (events: HelpEvent[], from: HelpState = HELP_EMPTY): HelpState => events.reduce(helpReducer, from)

describe('helpReducer', () => {
  it('enter/leave set and clear the hover text', () => {
    expect(run([{ type: 'enter', text: 'a' }])).toEqual({ hover: 'a', focus: null })
    expect(run([{ type: 'enter', text: 'a' }, { type: 'leave' }])).toEqual(HELP_EMPTY)
  })
  it('a leave after a focus keeps the focused text; blur clears it', () => {
    const s = run([{ type: 'focus', text: 'f' }, { type: 'enter', text: 'h' }, { type: 'leave' }])
    expect(s).toEqual({ hover: null, focus: 'f' })
    expect(helpText(s)).toBe('f')
    expect(run([{ type: 'blur' }], s)).toEqual(HELP_EMPTY)
  })
  it('the hovered control wins over the focused one; blur falls back to the hover', () => {
    const s = run([{ type: 'focus', text: 'f' }, { type: 'enter', text: 'h' }])
    expect(helpText(s)).toBe('h')
    expect(helpText(run([{ type: 'blur' }], s))).toBe('h')
  })
  it('no-op events return the same state object', () => {
    const s = run([{ type: 'enter', text: 'a' }])
    expect(helpReducer(s, { type: 'enter', text: 'a' })).toBe(s)
    expect(helpReducer(HELP_EMPTY, { type: 'leave' })).toBe(HELP_EMPTY)
    expect(helpReducer(HELP_EMPTY, { type: 'blur' })).toBe(HELP_EMPTY)
  })
  it('nothing hovered or focused shows nothing', () => {
    expect(helpText(HELP_EMPTY)).toBeNull()
  })
})
