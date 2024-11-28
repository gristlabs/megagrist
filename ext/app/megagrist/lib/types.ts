/**
 * This is loosely based on Grist's app/common/ActiveDocAPI.ts.
 */

import {CellValue, DocAction, TableColValues} from './DocActions';

// Filters could be specified in any form that can be safely converted to SQL WHERE clause
// (doesn't need to be limited to an AND of column-based filters as before). Here, we use
// "ParsedPredicateFormula" from Grist's app/common/PredicateFormula.ts, whose added benefit is
// that it is also used to express access rules conditions.
export type ParsedPredicateFormula = [string, ...(ParsedPredicateFormula|CellValue)[]];
export type QueryFilters = ParsedPredicateFormula;

/**
 * This is a list of column IDs, optionally prefixed by "-" for "descending".
 */
export type OrderByClause = string[];

/**
 * The cursor determines where to start among the ordered list of matching records. CellValues
 * must correspond to the "sort" clause. If cursor type is "after", will return records strictly
 * after the given values; if "before", then will return records strictly before.
 */
export type QueryCursor = [QueryCursorType, CellValue[]];
export type QueryCursorType = 'after'|'before';


/**
 * Represents a query for Grist data. The tableId is required. An empty set of filters indicates
 * the full table. Examples:
 *    {tableId: "Projects", filters: {}}
 *    {tableId: "Employees", filters: "Status = 'Active' AND Dept in ('Sales', 'HR')"}
 */
export interface Query {
  tableId: string;
  filters?: QueryFilters;
  sort?: OrderByClause;
  limit?: number;
  cursor?: QueryCursor;     // Which value to start returning results from.
  columns?: string[];
  rowIds?: number[];

  // When this is requested, results include a special column _grist_Previous, with the rowId of
  // the previous row according to the given sort and filters, regardless of whether this previous
  // row is included in rowIds.
  includePrevious?: boolean;
}

export interface QueryResultCommon {
  tableId: string;

  // Each state of the database is identified by an actionNum. Each change increments it. (Some
  // merged changes may increment it by more than 1.)
  actionNum: number;

  // It may also be appropriate to include attachment metadata referred to in tableData.
  // attachments?: TableColValues;
}

/**
 * Results of fetching a table. Includes the table data you would expect.
 */
export interface QueryResult extends QueryResultCommon {
  tableData: TableColValues;
}

/**
 * Results of fetching a table, with a streaming interface, suitable for StreamingRpc.
 */
export interface QueryResultStreaming {
  value: QueryResultCommon & {colIds: string[]};
  chunks: AsyncIterable<CellValue[][]>;
}

/**
 * Represents changes sent by a user, as well as processed changes to apply to the database and
 * broadcast to subscribers.
 */
export interface ActionSet {
  actions: DocAction[];
}

/**
 * When applying an ActionSet, caller gets results, one for each action.
 */
export interface ApplyResultSet {
  results: unknown[];
}

export type GristType = 'Any' | 'Attachments' | 'Blob' | 'Bool' | 'Choice' | 'ChoiceList' |
  'Date' | 'DateTime' |
  'Id' | 'Int' | 'ManualSortPos' | 'Numeric' | 'PositionNumber' | 'Ref' | 'RefList' | 'Text';
