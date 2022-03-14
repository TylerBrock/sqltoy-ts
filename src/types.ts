export type Row = Record<string, any>;
export type Table = { name: string, rows: Array<Row> };
export type Database = { tables: Record<string, Table> };
export type Predicate = (row: Row) => boolean;
