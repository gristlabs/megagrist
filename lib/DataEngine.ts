import {ActionSet, ApplyResultSet, Query, QueryCursor, QueryResult, QuerySubId} from './types';
import {GristType, OrderByClause, ParsedPredicateFormula} from './types';
import {CellValue, DocAction} from './DocActions';
import {IDataEngine, QuerySubCallback} from './IDataEngine';
import SqliteDatabase from 'better-sqlite3';

export class DataEngine implements IDataEngine {
  private _querySubs = new Map<number, Query>();
  private _nextQuerySub = 1;

  constructor(private _db: SqliteDatabase.Database) {
  }

  public async fetchQuery(query: Query): Promise<QueryResult> {
    const bindParams = new BindParams();
    const sql = sqlSelectFromQuery(query, bindParams);
    // console.warn("RUNNING SQL", sql);
    const stmt = this._db.prepare(sql);
    return this._db.transaction(() => {
      const rows = stmt.raw().all(bindParams.getParams()) as CellValue[][];
      // console.warn("RESULT", rows);
      const queryResult: QueryResult = {
        tableId: query.tableId,
        tableData: { id: [] },
        actionNum: 0,       // TODO
      };
      for (const [index, col] of stmt.columns().entries()) {
        queryResult.tableData[col.name] = rows.map(r => r[index]);
      }
      return queryResult;
    })();
  }

  // See querySubscribe for requirements on unsubscribing.
  public async fetchAndSubscribe(query: Query, callback: QuerySubCallback): Promise<QueryResult> {
    const subId = this._doQuerySubscribe(query, callback);
    const queryResult = await this.fetchQuery(query);
    return {...queryResult, subId};
  }

  // If querySubscribe succeeds, the data engine is now maintaining a subscription. It is the
  // caller's responsibility to release it with queryUnsubscribe(), once it is no longer needed.
  public async querySubscribe(query: Query, callback: QuerySubCallback): Promise<QuerySubId> {
    return this._doQuerySubscribe(query, callback);
  }

  public async queryUnsubscribe(subId: QuerySubId): Promise<boolean> {
    return this._querySubs.delete(subId);
  }

  public async applyActions(actionSet: ActionSet): Promise<ApplyResultSet> {
    return this._db.transaction((): ApplyResultSet => {
      // TODO: In the future, we need to pass actionSet through Access Rules,
      // Data Engine (to trigger anything synchronous), Access Rules again, all within a
      // transaction to ensure we are seeing a consistent view of DB and no other connection
      // attempts to write meanwhile.

      const docActionApplier = new DocActionApplier(this._db);
      const results: unknown[] = [];
      for (const action of actionSet.actions) {
        results.push(docActionApplier.apply(action));
      }
      return {results};
    }).immediate();
  }

  private _doQuerySubscribe(query: Query, callback: QuerySubCallback): QuerySubId {
    const subId = this._nextQuerySub++;
    this._querySubs.set(subId, query);
    return subId;
  }
}

class BindParams {
  private _next = 1;
  private _params: {[id: string]: CellValue} = {};

  public getParams() { return this._params; }

  public addParam(value: CellValue) {
    const name = `p${this._next++}`;
    this._params[name] = value;
    return `:${name}`;
  }
}

function sqlSelectFromQuery(query: Query, params: BindParams): string {
  const filterExpr = sqlExprFromFilters(query.filters, params);
  const cursorExpr = query.cursor !== undefined ? sqlExprFromCursor(query.sort, query.cursor, params) : null;
  const whereExpr = cursorExpr ? `(${filterExpr}) AND (${cursorExpr})` : filterExpr;
  const orderBy = query.sort !== undefined ? `ORDER BY ${sqlOrderByFromSort(query.sort)}` : '';
  const limit = query.limit !== undefined ? `LIMIT ${query.limit}` : '';
  return `SELECT * FROM ${quoteIdent(query.tableId)} WHERE ${whereExpr} ${orderBy} ${limit}`;
}

function sqlExprFromFilters(filters: ParsedPredicateFormula, params: BindParams): string {
  function combine(args: ParsedPredicateFormula[], numArgs: number|null, cb: (args: string[]) => string) {
    if (numArgs !== null && args.length !== numArgs) {
      throw new Error(`Expected ${numArgs} arguments, but got ${args.length}`);
    }
    return '(' + cb(args.map(compileNode)) + ')';
  }

  function compileNode(node: ParsedPredicateFormula): string {
    const args = node.slice(1) as ParsedPredicateFormula[];
    switch (node[0]) {
      case 'And':   return combine(args, null, parts => parts.join(' AND '));
      case 'Or':    return combine(args, null, parts => parts.join(' OR '));
      case 'Add':   return combine(args, null, parts => parts.join(' + '));
      case 'Sub':   return combine(args, 2, ([a, b]) => `${a} - ${b}`);
      case 'Mult':  return combine(args, null, parts => parts.join(' * '));
      case 'Div':   return combine(args, 2, ([a, b]) => `${a} / ${b}`);
      case 'Mod':   return combine(args, 2, ([a, b]) => `MOD(${a}, ${b})`);
      case 'Not':   return combine(args, 1, ([a]) => `NOT ${a}`);
      case 'Eq':    return combine(args, 2, ([a, b]) => `${a} = ${b}`);
      case 'NotEq': return combine(args, 2, ([a, b]) => `${a} != ${b}`);
      case 'Lt':    return combine(args, 2, ([a, b]) => `${a} < ${b}`);
      case 'LtE':   return combine(args, 2, ([a, b]) => `${a} <= ${b}`);
      case 'Gt':    return combine(args, 2, ([a, b]) => `${a} > ${b}`);
      case 'GtE':   return combine(args, 2, ([a, b]) => `${a} >= ${b}`);
      case 'Is':    return combine(args, 2, ([a, b]) => `${a} IS ${b}`);
      case 'IsNot': return combine(args, 2, ([a, b]) => `${a} IS NOT ${b}`);
      case 'In':    return combine(args, 2, ([a, b]) => `${a} IN ${b}`);
      case 'NotIn': return combine(args, 2, ([a, b]) => `${a} NOT IN ${b}`);
      case 'List':  return combine(args, null, parts => parts.join(', '));
      case 'Const': return params.addParam(node[1]);
      case 'Name':  return quoteIdent(node[1] as string);
      case 'Attr':  throw new Error('Attr not supported in filters');
      case 'Comment': return compileNode(args[0]);
    }
    throw new Error(`Unknown node type '${node[0]}'`);
  }
  return compileNode(filters);
}

function sqlOrderByFromSort(sort: OrderByClause): string {
  const parts: string[] = [];
  for (const colSpec of sort) {
    const isDesc = colSpec.startsWith('-');
    const colId = isDesc ? colSpec.slice(1) : colSpec;
    parts.push(`${quoteIdent(colId)} ${isDesc ? 'DESC' : 'ASC'}`);
  }
  return parts.join(', ');
}

function sqlExprFromCursor(sort: OrderByClause|undefined, cursor: QueryCursor, params: BindParams): string {
  const cursorValues = cursor[1];
  if (sort?.length !== cursorValues.length) {
    throw new Error("Cursor must have as many fields as sort columns");
  }
  if (cursor[0] !== 'after') {
    throw new Error("Only 'after' cursor is currently supported");
  }
  const colSpecs = sort;
  function compileNode(index: number): string {
    if (index >= cursor.length) {
      return 'TRUE';
    }
    const next = compileNode(index + 1);
    const colSpec = colSpecs[index];
    const isDesc = colSpec.startsWith('-');
    const colId = isDesc ? colSpec.slice(1) : colSpec;
    const op = isDesc ? '>' : '<';
    const p = params.addParam(cursorValues[index]);
    return `${quoteIdent(colId)} ${op} ${p} OR (${quoteIdent(colId)} = ${p} AND (${next}))`;
  }
  return compileNode(0);
}

/**
 * Validate and quote SQL identifiers such as table and column names.
 */
export function quoteIdent(ident: string): string {
  if (!/^[\w.]+$/.test(ident)) {
    throw new Error(`SQL identifier is not valid: ${ident}`);
  }
  return `"${ident}"`;
}

class DocActionApplier {
  constructor(private _db: SqliteDatabase.Database) {}

  public apply(action: DocAction) {
    return this[action[0]](action as any);
  }

  public BulkAddRecord([_, tableId, rowIds, colValues]: DocAction.BulkAddRecord) {
    if (rowIds.length === 0) { return; }
    const cols = ['id', ...Object.keys(colValues)];
    const values = [rowIds, ...Object.values(colValues)];
    const placeholders = cols.map(c => '?');
    const stmt = this._db.prepare(`INSERT INTO ${quoteIdent(tableId)} (${cols.join(', ')}) VALUES (${placeholders})`);
    for (let i = 0; i < rowIds.length; i++) {
      stmt.run(values.map(col => col[i]));
    }
  }

  public BulkUpdateRecord([_, tableId, rowIds, colValues]: DocAction.BulkUpdateRecord) {
    // TODO For all data operations, including UPDATE, there are ways to deal with multiple rows
    // in one statement. With batcing, could run many times fewer statements. Question is whether
    // it's actually faster. For example:
    //    WITH u(id, col1, col2) AS (VALUES (?, ?, ?), (?, ?, ?), ...)
    //    UPDATE tableId AS t SET (t.col1, t.col2) = (u.col1, u.col2)
    //    FROM u WHERE t.id = u.id;
    const cols = Object.keys(colValues);
    if (rowIds.length === 0 || cols.length === 0) { return; }

    const colListSql = cols.map(c => `${quoteIdent(c)} = ?`);
    const values = Object.values(colValues);
    const stmt = this._db.prepare(`UPDATE ${quoteIdent(tableId)} SET ${colListSql} WHERE id=?`);
    for (let i = 0; i < rowIds.length; i++) {
      stmt.run(values.map(col => col[i]), rowIds[i]);
    }
  }

  public BulkRemoveRecord([_, tableId, rowIds]: DocAction.BulkRemoveRecord) {
    if (rowIds.length === 0) { return; }
    const stmt = this._db.prepare(`DELETE FROM ${quoteIdent(tableId)} WHERE id = ?`);
    for (let i = 0; i < rowIds.length; i++) {
      stmt.run(rowIds[i]);
    }
  }
  public ReplaceTableData(action: DocAction.ReplaceTableData) {
    throw new Error('Not yet implemented');
  }
  public AddColumn(action: DocAction.AddColumn) {
    throw new Error('Not yet implemented');
  }
  public RemoveColumn(action: DocAction.RemoveColumn) {
    throw new Error('Not yet implemented');
  }
  public RenameColumn(action: DocAction.RenameColumn) {
    throw new Error('Not yet implemented');
  }
  public ModifyColumn(action: DocAction.ModifyColumn) {
    throw new Error('Not yet implemented');
  }
  public AddTable([_, tableId, cols]: DocAction.AddTable) {
    const colSpecSql = cols.map(col =>
      `${quoteIdent(col.id)} ${getSqlTypeInfo(col.type).sqlType} DEFAULT ${getSqlTypeInfo(col.type).sqlDefault}`);

    // Every table needs an "id" column, and it should be an "integer primary key" type so that it
    // serves as the alias for the SQLite built-in "rowid" column. See
    // https://www.sqlite.org/lang_createtable.html#rowid for details.
    colSpecSql.unshift(`id INTEGER PRIMARY KEY`);

    const stmt = this._db.prepare(`CREATE TABLE ${quoteIdent(tableId)} (${colSpecSql.join(', ')})`);
    stmt.run();
  }
  public RemoveTable(action: DocAction.RemoveTable) {
    throw new Error('Not yet implemented');
  }
  public RenameTable(action: DocAction.RenameTable) {
    throw new Error('Not yet implemented');
  }
}

/**
 * For each Grist type, we pick a good Sqlite SQL type name, and default value to use. Sqlite
 * columns are loosely typed, and the types named here are not all distinct in terms of
 * 'affinities', but they are helpful as comments.  Type names chosen from:
 *   https://www.sqlite.org/datatype3.html#affinity_name_examples
 */
interface SqlTypeInfo {
  sqlType: string;
  sqlDefault: string;
  gristDefault: CellValue;
}

const gristBaseTypeToSql: {[key in GristType]: SqlTypeInfo} = {
  Any:              {sqlType: 'BLOB',     sqlDefault: "NULL",   gristDefault: null    },
  Attachments:      {sqlType: 'TEXT',     sqlDefault: "NULL",   gristDefault: null    },
  Blob:             {sqlType: 'BLOB',     sqlDefault: "NULL",   gristDefault: null    },
  // Bool is only supported by SQLite as 0 and 1 values.
  Bool:             {sqlType: 'BOOLEAN',  sqlDefault: "0",      gristDefault: false   },
  Choice:           {sqlType: 'TEXT',     sqlDefault: "''",     gristDefault: ''      },
  ChoiceList:       {sqlType: 'TEXT',     sqlDefault: "NULL",   gristDefault: null    },
  Date:             {sqlType: 'DATE',     sqlDefault: "NULL",   gristDefault: null    },
  DateTime:         {sqlType: 'DATETIME', sqlDefault: "NULL",   gristDefault: null    },
  Id:               {sqlType: 'INTEGER',  sqlDefault: "0",      gristDefault: 0       },
  Int:              {sqlType: 'INTEGER',  sqlDefault: "0",      gristDefault: 0       },
  // Note that "1e999" is a way to store Infinity into SQLite.
  // See also http://sqlite.1065341.n5.nabble.com/Infinity-td55327.html.
  ManualSortPos:    {sqlType: 'NUMERIC',  sqlDefault: "1e999",  gristDefault: Number.POSITIVE_INFINITY       },
  Numeric:          {sqlType: 'NUMERIC',  sqlDefault: "0",      gristDefault: 0       },
  PositionNumber:   {sqlType: 'NUMERIC',  sqlDefault: "1e999",  gristDefault: Number.POSITIVE_INFINITY       },
  Ref:              {sqlType: 'INTEGER',  sqlDefault: "0",      gristDefault: 0       },
  RefList:          {sqlType: 'TEXT',     sqlDefault: "NULL",   gristDefault: null    },
  Text:             {sqlType: 'TEXT',     sqlDefault: "''",     gristDefault: ''      },
};

function getSqlTypeInfo(fullColType: string): SqlTypeInfo {
  const baseType = fullColType.split(':')[0];
  return gristBaseTypeToSql[baseType as GristType] || gristBaseTypeToSql.Any;
}
