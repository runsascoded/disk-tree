import type { ReactNode } from 'react'
import { formatSize, formatCount, formatNumber, timeAgo } from '../utils/format'

export type ColumnType = 'text' | 'path' | 'size' | 'count' | 'number' | 'time' | 'icon' | 'custom'

export interface Column<T> {
  key: string
  label: string
  type?: ColumnType
  /** Custom render function for the cell */
  render?: (row: T, index: number) => ReactNode
  /** Tooltip for the header */
  tooltip?: string
  /** Additional className for this column's cells */
  className?: string
  /** Override text alignment (default: right for numeric, left for text) */
  align?: 'left' | 'right' | 'center'
  /** Whether this column should shrink to fit (default: true for numeric columns) */
  shrink?: boolean
}

export interface DataTableProps<T> {
  columns: Column<T>[]
  data: T[]
  /** CSS class name for the table */
  className?: string
  /** Key extractor for rows */
  rowKey: (row: T, index: number) => string | number
  /** Optional row click handler */
  onRowClick?: (row: T, index: number, event: React.MouseEvent) => void
  /** Optional row class name generator */
  rowClassName?: (row: T, index: number) => string | undefined
  /** Optional row style generator */
  rowStyle?: (row: T, index: number) => React.CSSProperties | undefined
}

/**
 * Get the default alignment for a column type
 */
function getDefaultAlign(type: ColumnType | undefined): 'left' | 'right' | 'center' {
  switch (type) {
    case 'size':
    case 'count':
    case 'number':
    case 'time':
      return 'right'
    case 'icon':
      return 'center'
    default:
      return 'left'
  }
}

/**
 * Check if a column type should shrink to fit content
 */
function shouldShrink(type: ColumnType | undefined): boolean {
  switch (type) {
    case 'size':
    case 'count':
    case 'number':
    case 'time':
    case 'icon':
      return true
    default:
      return false
  }
}

/**
 * Format a value based on column type
 */
function formatValue<T>(row: T, col: Column<T>): ReactNode {
  if (col.render) {
    return col.render(row, 0)
  }

  const value = (row as Record<string, unknown>)[col.key]

  switch (col.type) {
    case 'size':
      return formatSize(value as number | null | undefined)
    case 'count':
      return formatCount(value as number | null | undefined)
    case 'number':
      return formatNumber(value as number | null | undefined)
    case 'time':
      return timeAgo(value as string | number | null | undefined)
    case 'path':
      return <code>{String(value ?? '')}</code>
    default:
      return value == null ? '-' : String(value)
  }
}

/**
 * Build cell style based on column config
 */
function getCellStyle<T>(col: Column<T>): React.CSSProperties {
  const align = col.align ?? getDefaultAlign(col.type)
  const shrink = col.shrink ?? shouldShrink(col.type)

  return {
    textAlign: align,
    ...(shrink && {
      width: '1%',
      whiteSpace: 'nowrap' as const,
      paddingLeft: align === 'right' ? '1em' : undefined,
    }),
  }
}

export function DataTable<T>({
  columns,
  data,
  className,
  rowKey,
  onRowClick,
  rowClassName,
  rowStyle,
}: DataTableProps<T>) {
  return (
    <table className={className}>
      <thead>
        <tr>
          {columns.map(col => (
            <th
              key={col.key}
              style={getCellStyle(col)}
              className={col.className}
              title={col.tooltip}
            >
              {col.label}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {data.map((row, index) => (
          <tr
            key={rowKey(row, index)}
            onClick={onRowClick ? e => onRowClick(row, index, e) : undefined}
            className={rowClassName?.(row, index)}
            style={{
              cursor: onRowClick ? 'pointer' : undefined,
              ...rowStyle?.(row, index),
            }}
          >
            {columns.map(col => (
              <td
                key={col.key}
                style={getCellStyle(col)}
                className={col.className}
              >
                {formatValue(row, col)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
}
