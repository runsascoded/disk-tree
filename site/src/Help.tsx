import { createContext, useCallback, useContext, useId, useMemo, useReducer } from 'react'
import type { ReactNode } from 'react'
import { HELP_EMPTY, helpReducer, helpText } from './helpState'
import type { HelpEvent } from './helpState'
import { useHelpPref } from './prefs'

// The help line (specs/edu-drawer.md): explanations of controls leave the
// floating layer for one fixed strip at the bottom of the viewport, which
// mirrors whatever control is hovered or keyboard-focused and is empty
// otherwise. Data tips (cells, rows) stay `<Tooltip>`s.

const HelpCtx = createContext<{ text: ReactNode | null; send: (e: HelpEvent) => void } | null>(null)

export function HelpProvider({ children }: { children: ReactNode }) {
  const [state, send] = useReducer(helpReducer, HELP_EMPTY)
  const value = useMemo(() => ({ text: helpText(state), send }), [state])
  return <HelpCtx.Provider value={value}>{children}</HelpCtx.Provider>
}

/** Wrap a control: its explanation shows in the help line while the control
 * is hovered or focused, and is always available to assistive tech through
 * `aria-describedby`. Nothing floats. */
export function Explain({ text, children, className }: { text: ReactNode; children: ReactNode; className?: string }) {
  const ctx = useContext(HelpCtx)
  const [on] = useHelpPref()
  const id = useId()
  const send = ctx?.send
  const enter = useCallback(() => send?.({ type: 'enter', text }), [send, text])
  const leave = useCallback(() => send?.({ type: 'leave' }), [send])
  const focus = useCallback(() => send?.({ type: 'focus', text }), [send, text])
  const blur = useCallback(() => send?.({ type: 'blur' }), [send])
  const live = on === 'on' && !!send
  return (
    <span
      className={`has-help${className ? ` ${className}` : ''}`}
      aria-describedby={id}
      onPointerEnter={live ? enter : undefined}
      onPointerLeave={live ? leave : undefined}
      onFocus={live ? focus : undefined}
      onBlur={live ? blur : undefined}
    >
      {children}
      <span id={id} className="sr-only">{text}</span>
    </span>
  )
}

/** The strip itself; mounted once (Root). Rendered only while the
 * preference is on, with a one-line minimum so the page never jumps. */
export function HelpLine() {
  const ctx = useContext(HelpCtx)
  const [on] = useHelpPref()
  if (on !== 'on') return null
  return (
    <div className="help-line" role="status" aria-live="polite">
      {ctx?.text ?? <span className="idle">hover or focus a control for what it does · <kbd>h</kbd> hides this line</span>}
    </div>
  )
}
