
export function sizeStr(size: number, iec = false) {
  const units = iec ? ['B', 'KiB', 'MiB', 'GiB', 'TiB'] : ['B', 'KB', 'MB', 'GB', 'TB']
  const base = iec ? 1024 : 1000
  let i = 0
  while (size > base && i < units.length - 1) {
    size /= base
    i++
  }
  const str = size.toPrecision(3).replace(/\.0+$/, '').replace(/\.(\d+)0+$/, '.$1')
  return `${str} ${units[i]}`
}

export function Size({ size, iec = false }: { size: number, iec?: boolean }) {
  return <span>{sizeStr(size, iec)}</span>
}
