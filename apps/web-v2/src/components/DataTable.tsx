import type { ReactNode } from "react";

type Column<T> = {
  key: string;
  header: string;
  align?: "left" | "right" | "center";
  render: (row: T) => ReactNode;
};

type DataTableProps<T> = {
  columns: Column<T>[];
  rows: T[];
  getRowKey: (row: T) => string;
  getRowClassName?: (row: T) => string | undefined;
  onRowClick?: (row: T) => void;
  emptyLabel?: string;
  loading?: boolean;
  loadingLabel?: string;
  loadingRowCount?: number;
};

export function DataTable<T>({
  columns,
  rows,
  getRowClassName,
  getRowKey,
  onRowClick,
  emptyLabel = "No rows available.",
  loading = false,
  loadingLabel = "Loading rows...",
  loadingRowCount = 6
}: DataTableProps<T>) {
  return (
    <div className="table-frame" aria-busy={loading}>
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th className={column.align ? `is-${column.align}` : undefined} key={column.key}>
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {loading ? (
            Array.from({ length: loadingRowCount }, (_, rowIndex) => (
              <tr className="table-loading-row" key={`loading-${rowIndex}`}>
                {columns.map((column, columnIndex) => (
                  <td className={column.align ? `is-${column.align}` : undefined} key={column.key}>
                    <span className={columnIndex === 0 ? "table-loading-cell table-loading-cell--wide" : "table-loading-cell"} />
                    {rowIndex === 0 && columnIndex === 0 ? <span className="sr-only">{loadingLabel}</span> : null}
                  </td>
                ))}
              </tr>
            ))
          ) : rows.length === 0 ? (
            <tr>
              <td className="table-empty" colSpan={columns.length}>
                {emptyLabel}
              </td>
            </tr>
          ) : (
            rows.map((row) => (
              <tr
                className={getRowClassName?.(row)}
                key={getRowKey(row)}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
                onKeyDown={onRowClick ? (event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    onRowClick(row);
                  }
                } : undefined}
                role={onRowClick ? "button" : undefined}
                tabIndex={onRowClick ? 0 : undefined}
              >
                {columns.map((column) => (
                  <td className={column.align ? `is-${column.align}` : undefined} key={column.key}>
                    {column.render(row)}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
