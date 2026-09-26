import type { ReactNode } from 'react';

export type Column<T> = {
  key: string;
  header: ReactNode;
  cell: (row: T) => ReactNode;
  /** Right-aligned, tabular numbers. */
  numeric?: boolean;
  width?: string;
};

/**
 * A plain data table. `empty` renders in place of the body when there are no
 * rows. `onRowClick` is a pointer shortcut only: put a link or button to the
 * same place in a cell, so keyboard and screen-reader users can reach it.
 * Clicks on links and buttons inside the row are left to them.
 */
export function Table<T>({
  columns,
  rows,
  rowKey,
  empty,
  caption,
  onRowClick,
}: {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  empty?: ReactNode;
  caption?: string;
  onRowClick?: (row: T) => void;
}) {
  if (rows.length === 0 && empty) return <>{empty}</>;
  return (
    <div className="loam-table-wrap">
      <table className="loam-table">
        {caption && <caption className="sr-only">{caption}</caption>}
        <thead>
          <tr>
            {columns.map((c) => (
              <th
                key={c.key}
                className={c.numeric ? 'loam-num' : undefined}
                style={{ width: c.width }}
              >
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={rowKey(row)}
              data-href={onRowClick ? '' : undefined}
              onClick={
                onRowClick
                  ? (e) => {
                      if (
                        (e.target as HTMLElement).closest(
                          'a, button, input, select, textarea, label',
                        )
                      )
                        return;
                      onRowClick(row);
                    }
                  : undefined
              }
            >
              {columns.map((c) => (
                <td key={c.key} className={c.numeric ? 'loam-num' : undefined}>
                  {c.cell(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** A table cell with a strong first line and a muted second. */
export function Primary({ title, detail }: { title: ReactNode; detail?: ReactNode }) {
  return (
    <span className="loam-table-primary">
      <strong>{title}</strong>
      {detail && <span>{detail}</span>}
    </span>
  );
}
