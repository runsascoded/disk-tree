import { Component } from 'react'
import type { ErrorInfo, ReactNode } from 'react'
import { Alert, Button } from '@mui/material'

/**
 * Error boundary around a single visualization panel.
 *
 * React unmounts the *whole tree* when a render throws with no boundary above
 * it, so one bad accessor in one widget blanks the entire app — the table and
 * breadcrumbs included. (Seen for real: a missing import in a tooltip renderer
 * turned a hover into a white screen.) A panel that fails should cost you the
 * panel, not the page.
 */
interface Props {
  /** Shown in the fallback, e.g. "treemap". */
  label: string
  /** Remounting the panel is only worth offering if something can change. */
  onRetry?: () => void
  children: ReactNode
}

interface State {
  error: Error | null
}

export class VizBoundary extends Component<Props, State> {
  state: State = { error: null }

  static getDerivedStateFromError(error: Error): State {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error(`[${this.props.label}] render failed`, error, info.componentStack)
  }

  componentDidUpdate(prev: Props) {
    // A new panel (view switch) starts clean rather than inheriting the
    // previous one's failure.
    if (prev.children !== this.props.children && this.state.error) {
      this.setState({ error: null })
    }
  }

  render() {
    const { error } = this.state
    if (!error) return this.props.children
    return (
      <Alert
        severity="error"
        action={
          <Button
            size="small"
            color="inherit"
            onClick={() => {
              this.setState({ error: null })
              this.props.onRetry?.()
            }}
          >
            Retry
          </Button>
        }
      >
        The {this.props.label} failed to render: {error.message}
      </Alert>
    )
  }
}
