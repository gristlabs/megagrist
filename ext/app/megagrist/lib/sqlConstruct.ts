import {Query, QueryCursor} from './types';
import {OrderByClause, ParsedPredicateFormula} from './types';
import {CellValue} from './DocActions';
import {quoteIdent} from './sqlUtil';

// See ExpandedQuery in core/app/server/lib/ExpandedQuery.ts
export interface ExpandedQuery extends Query {
  // A list of join clauses to bring in data from other tables.
  joins?: string[];

  // A list of selections for regular data and data computed via formulas.
  // If query.selects is present, then query.columns is ignored.
  selects?: string[];
}

/**
 * When constructing a query, and values to include in the query are included as placeholders,
 * with the actual values collected in this BindParams object. They should be retrieved using
 * getParams() and included when the constructed statement is executed with .run(), .all(), etc.
 *
 * For example:
 *    const sql = sqlSelectFromQuery(query, bindParams);
 *    const stmt = db.prepare(sql);
 *    stmt.run(bindParams.getParams())
 */
export class BindParams {
  private _next = 1;
  // In theory, we could skip named values and use ?NNN syntax. In practice, better-sqlite3
  // doesn't support ?NNN syntax properly because it's hard to reconcile all bind options with its
  // interface. See https://github.com/WiseLibs/better-sqlite3/issues/576.
  private _params: {[id: string]: CellValue} = {};

  public getParams() { return this._params; }

  public addParam(value: CellValue) {
    const name = `p${this._next++}`;
    this._params[name] = value;
    return `:${name}`;
  }
}

/**
 * Construct SQL from the given query.
 */
export function sqlSelectFromQuery(query: ExpandedQuery, params: BindParams): string {
  return query.includePrevious ?
    sqlSelectWithPrevious(query, params) :
    sqlSelectNormal(query, params);
}

function sqlSelectNormal(query: ExpandedQuery, params: BindParams): string {
  const namePrefix = `${quoteIdent(query.tableId)}.`;
  const conditions = sqlSelectConditionsFromQuery(namePrefix, query, params);
  const joinClauses = query.joins ? query.joins.join(' ') : '';
  const selects = getSelects(namePrefix, query).join(", ");
  return `SELECT ${selects} FROM ${quoteIdent(query.tableId)} ${joinClauses} ${conditions}`;
}

function getSelects(namePrefix: string, query: ExpandedQuery): string[] {
  return query.selects ||
    query.columns?.map(c => namePrefix + quoteIdent(c)) ||
    [namePrefix + '*'];
}

function sqlSelectWithPrevious(query: ExpandedQuery, params: BindParams): string {
  // This happens to be an order of magnitude faser than sqlSelectWithPrevious1, which uses the
  // LAG window function. This surprises me.
  const namePrefix = `${quoteIdent(query.tableId)}.`;
  const selects = [...getSelects(namePrefix, query), '_prev_.id AS _grist_Previous'];
  const joins = [...(query.joins || []),
    `LEFT JOIN ${quoteIdent(query.tableId)} AS _prev_
      ON _prev_.id = (
        SELECT id FROM ${quoteIdent(query.tableId)} AS _tmp_
        WHERE (${sqlSortedBefore(query.sort, "_tmp_", quoteIdent(query.tableId))})
        ORDER BY ${sqlOrderByFromSort("_tmp_.", query.sort, {reverse: true})}
        LIMIT 1
      )
    `
  ];
  const adjustedQuery: ExpandedQuery = {...query, selects, joins};
  return sqlSelectNormal(adjustedQuery, params);
}


/**
 * Construct just the portion of SQL starting with WHERE, i.e. all conditions including ORDER BY
 * and LIMIT. It also prefixes each mention of the column with namePrefix (like `quotedTableName.`
 * or '' to omit the prefix). This helps for using the SQL in JOINs.
 */
export function sqlSelectConditionsFromQuery(namePrefix: string, query: ExpandedQuery, params: BindParams): string {
  const filterExpr = query.filters ? sqlExprFromFilters(namePrefix, query.filters, params) : null;
  const cursorExpr = query.cursor ? sqlExprFromCursor(namePrefix, query.sort, query.cursor, params) : null;
  const rowsExpr = query.rowIds ? sqlExprFromRowIds(namePrefix, query.rowIds) : null;
  const whereExpr = [filterExpr, cursorExpr, rowsExpr].filter(Boolean).map(expr => `(${expr})`).join(' AND ') || '1';
  const orderBy = `ORDER BY ${sqlOrderByFromSort(namePrefix, query.sort)}`;
  const limit = typeof query.limit === 'number' ? `LIMIT ${query.limit}` : '';
  return `WHERE ${whereExpr} ${orderBy} ${limit}`;
}

function sqlExprFromRowIds(namePrefix: string, rowIds: number[]) {
  if (!rowIds.every(Number.isInteger)) {
    throw new Error("Expected all rowIds to be integers");
  }
  return `${namePrefix}id in (${rowIds})`;
}

function sqlExprFromFilters(namePrefix: string, filters: ParsedPredicateFormula, params: BindParams): string {
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
      case 'Const': return params.addParam(node[1] as CellValue);
      case 'Name':  return namePrefix + quoteIdent(node[1] as string);
      case 'Attr':  throw new Error('Attr not supported in filters');
      case 'Comment': return compileNode(args[0]);
    }
    throw new Error(`Unknown node type '${node[0]}'`);
  }
  return compileNode(filters);
}

function sqlOrderByFromSort(namePrefix: string, sort: OrderByClause|undefined, options?: {reverse?: boolean}): string {
  const normalOrder = !options?.reverse;
  const parts: string[] = [];
  if (sort) {
    for (const colSpec of sort) {
      const isDesc = colSpec.startsWith('-');
      const colId = isDesc ? colSpec.slice(1) : colSpec;
      const fullColId = namePrefix + quoteIdent(colId);
      parts.push(`${fullColId} ${isDesc === normalOrder ? 'DESC NULLS FIRST' : 'ASC NULLS LAST'}`);
    }
  }
  parts.push(namePrefix + 'id' + (normalOrder ? '' : ' DESC'));
  return parts.join(', ');
}

function sqlExprFromCursor(
  namePrefix: string, sort: OrderByClause|undefined, cursor: QueryCursor, params: BindParams
): string {
  const cursorValues = cursor[1];
  if (sort?.length !== cursorValues.length) {
    throw new Error("Cursor must have as many fields as sort columns");
  }
  if (cursor[0] !== 'after') {
    throw new Error("Only 'after' cursor is currently supported");
  }
  const colSpecs = sort;
  function compileNode(index: number): string {
    if (index >= cursorValues.length) {
      return 'FALSE';
    }
    const next = (index + 1 < cursorValues.length) ? compileNode(index + 1) : null;
    const colSpec = colSpecs[index];
    const isDesc = colSpec.startsWith('-');
    const colId = isDesc ? colSpec.slice(1) : colSpec;
    const fullColId = namePrefix + quoteIdent(colId);
    const op = isDesc ? '<' : '>';
    const p = params.addParam(cursorValues[index]);
    // TODO does this need support for NULLS FIRST / NULLS LAST?
    return `${fullColId} ${op} ${p}` + (next ? ` OR (${fullColId} = ${p} AND (${next}))` : '');
  }
  return compileNode(0);
}

function sqlSortedBefore(sort: OrderByClause|undefined, scope1: string, scope2: string) {
  const colSpecs = sort ? [...sort, 'id'] : ['id'];
  const count = colSpecs.length;
  function compileNode(index: number): string {
    if (index >= count) {
      return 'FALSE';
    }
    const next = (index + 1 < count) ? compileNode(index + 1) : null;
    const colSpec = colSpecs[index];
    const isDesc = colSpec.startsWith('-');
    const colId = isDesc ? colSpec.slice(1) : colSpec;
    const c = quoteIdent(colId);
    const c1 = `${scope1}.${c}`;
    const c2 = `${scope2}.${c}`;
    const op = isDesc ? '>' : '<';
    // TODO does this need support for NULLS FIRST / NULLS LAST?
    return `${c1} ${op} ${c2}` + (next ? ` OR (${c1} = ${c2} AND (${next}))` : '');
  }
  return compileNode(0);
}
