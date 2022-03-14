import type {
  Database,
  Table,
  Row,
  Predicate
} from './types';

let database: Database;

function initSQLToy() {
	database = {
		tables: {}
	}
}

function CREATE_TABLE(name: string) {
	database.tables[name] = { name, rows: [] };
	return database.tables[name];
}

function INSERT_INTO(tableName: string, row: Object | Array<Object>) {
	let rows;
	if (Array.isArray(row)) {
		rows = row;
	} else {
		rows = [row];
	}
	const table = database.tables[tableName];
	table.rows = [...table.rows, ...rows];
}

function FROM(tableName: string) {
  return database.tables[tableName];
}

function CROSS_JOIN(a: Table, b: Table): Table {
	const result: Table = { name: '', rows: [] };

	for (const x of a.rows) {
		for (const y of b.rows) {
			const row: Row = {};

			for (const k in x) {
				const columnName = a.name ? `${a.name}.${k}` : k;
				row[columnName] = x[k];
			}

			for (const k in y) {
				const columnName = b.name ? `${b.name}.${k}` : k;
				row[columnName] = y[k];
			}

			row._tableRows = [x, y];

			result.rows.push(row);
		}
	}
	return result;
}

function INNER_JOIN(a: Table, b: Table, pred: Predicate): Table {
  return {
    name: '',
    rows: CROSS_JOIN(a, b).rows.filter(pred),
  }
}

function LEFT_JOIN(a: Table, b: Table, pred: Predicate): Table {
  const cp = CROSS_JOIN(a, b);
  let result: Table = { name: '', rows: [] };

  for (let aRow of a.rows) {
    // find all rows in cross product which come from this row in table a using the _tableRows array
    const cpa = cp.rows.filter((cpr) => cpr._tableRows.includes(aRow));

    const matches = cpa.filter(pred);

    if (matches.length) {
      result.rows.push(...matches);
    } else {
      const row: Row = {};

      // values from a
      for (const key in aRow) {
        row[`${a.name}.${key}`] = aRow[key];
      }

      // nulls for b
      for (const key in b.rows[0]) {
        row[`${b.name}.${key}`] = null;
      }

      result.rows.push(row);
    }
  }
  return result;
}

function RIGHT_JOIN(a: Table, b: Table, pred: Predicate): Table {
  return LEFT_JOIN(b, a, pred);
}

const UNIT_SEP = '␟';

function DISTINCT(table: Table, columns: Array<string>): Table {
  const distinct: Record<string, Row> = {};
  for(const row of table.rows) {
    let key = columns.map(column => row[column]).join(UNIT_SEP);
    distinct[key] = row;
  }

  const newRows: Array<Row> = [];
  for (const key in distinct) {
    const newRow: Row = {};
    for (const column of columns) {
      newRow[column] = distinct[key][column];
    }
    newRows.push(newRow);
  }

  return {
    name: table.name,
    rows: newRows,
  };
}

function GROUP_BY(table: Table, groupBys: Array<string>): Table {
  const keyRows: Record<string, Array<Row>> = {};

  for (const row of table.rows) {
    let key = groupBys.map(groupBy => row[groupBy]).join(UNIT_SEP);
    if (!keyRows[key]) {
      keyRows[key] = [];
    }
    keyRows[key].push({...row});
  }

  const resultRows: Array<Row> = [];

  for (const key in keyRows) {
    const resultRow: Row = { _groupRows: keyRows[key] };
    for (const groupBy of groupBys) {
      resultRow[groupBy] = keyRows[key][0][groupBy];
    }
    resultRows.push(resultRow);
  }

  return {
    name: table.name,
    rows: resultRows,
  }
}

function aggregateHelper(table: Table, column: string, aggName: string, aggFunc: Function) {
  for (const row of table.rows) {
    const values = row._groupRows.map((gr: Row) => gr[column]);
    row[`${aggName}(${column})`] = aggFunc(values);
  }
  return table;
}

function ARRAY_AGG(table: Table, column: string) {
  const aggFunction = (values: Array<any>) => JSON.stringify(values);
  return aggregateHelper(table, column, 'ARRAY_AGG', aggFunction);
}

function AVG(table: Table, column: string) {
  return aggregateHelper(table, column, 'AVG', (values: Array<number>) => {
    const total = values.reduce((p, c) => p + c, 0);
    return total / values.length;
  });
}

function MAX(table: Table, column: string) {
  const getMax = (a: number, b: number) => Math.max(a, b);
  return aggregateHelper(table, column, 'MAX', (values: Array<number>) => values.reduce(getMax));
}

function MIN(table: Table, column: string) {
  const getMin = (a: number, b: number) => Math.min(a, b);
  return aggregateHelper(table, column, 'MIN', (values: Array<number>) => values.reduce(getMin));
}

function COUNT(table: Table, column: string) {
  return aggregateHelper(table, column, 'COUNT', (values: Array<any>) => values.length);
}

function HAVING(table: Table, predicate: Predicate): Table {
  return {
    name: table.name,
    rows: table.rows.filter(predicate)
  };
}

function SELECT(table: Table, columns: Array<string>, aliases?: Record<string, string>): Table {
  const newRows: Array<Row> = [];
  const colNames: Record<string, string> = {};

  for (const col of columns) {
    colNames[col] = aliases && aliases[col] ? aliases[col] : col;
  }

  for (const row of table.rows) {
    let newRow: Row = {};
    for (let column of columns) {
      newRow[colNames[column]] = row[column];
    }
    newRows.push(newRow);
  }

  return {
    name: table.name,
    rows: newRows,
  }
}

function SORT_BY(table: Table, sortFn: (a: Row, b: Row) => number) {
  return {
    name: table.name,
    rows: table.rows.sort(sortFn),
  }
}

function OFFSET(table: Table, offset: number): Table {
  return {
    name: table.name,
    rows: table.rows.slice(offset),
  }
}

function LIMIT(table: Table, limit: number): Table {
  return {
    name: table.name,
    rows: table.rows.slice(0, limit),
  }
}

initSQLToy();
CREATE_TABLE('stories');
CREATE_TABLE('employee');
CREATE_TABLE('department');
INSERT_INTO('stories', [
  {id: 1, name: 'The Elliptical Machine that ate Manhattan', author_id: 1},
  {id: 2, name: 'Queen of the Bats', author_id: 2},
  {id: 3, name: 'ChocoMan', author_id: 3},
]);
INSERT_INTO('employee', [
  {id: 1, name: 'Josh', department_id: 1},
  {id: 2, name: 'Ruth', department_id: 2},
  {id: 3, name: 'Gregg', department_id: 5},
]);
INSERT_INTO('department', [
  {id: 1, name: 'Sales'},
  {id: 2, name: 'Marketing'},
  {id: 3, name: 'Engineering'},
])
console.log(JSON.stringify(database!.tables, null, 2));

function print(t: Table) {
  const output = [];
  for (const r of t.rows) {
    const outRow: Record<string, any> = {};
    for (const k in r) {
      if (k[0] === '_') {
        continue;
      }
      outRow[k] = r[k];
    }
    output.push(outRow);
  }
  console.table(output);
}

const employee = FROM('employee');
const department = FROM('department');
const result = CROSS_JOIN(employee, department);
console.log(JSON.stringify(result, null, 2));
const innerResult = INNER_JOIN(employee, department, (r) => r["employee.department_id"] === r["department.id"]);
console.log({ innerResult });
const leftResult = LEFT_JOIN(employee, department, (r) => r["employee.department_id"] === r["department.id"]);
print(leftResult);
const rightResult = RIGHT_JOIN(employee, department, (r) => r["employee.department_id"] === r["department.id"]);
print(rightResult);
const groupResult = GROUP_BY(employee, ['department_id']);
print(groupResult);
const arrAggResult = ARRAY_AGG(groupResult, 'name');
print(arrAggResult);
const selectResult = SELECT(employee, ['id', 'name']);
print(selectResult);
