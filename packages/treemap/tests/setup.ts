import '@testing-library/jest-dom/vitest'

// jsdom doesn't ship a ResizeObserver; the Treemap uses one to track its
// container size. Stub it out so component tests don't crash.
class MockResizeObserver {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}
if (typeof globalThis.ResizeObserver === 'undefined') {
  (globalThis as unknown as { ResizeObserver: typeof MockResizeObserver }).ResizeObserver = MockResizeObserver
}
