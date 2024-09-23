import {ActionSet, ApplyResultSet, Query, QueryResult, QueryResultStreaming, QuerySubId} from './types';
import {CellValue} from './DocActions';
import {IDataEngine, QuerySubCallback} from './IDataEngine';
import {BindParams, sqlSelectFromQuery} from './sqlConstruct';
import {StoreDocAction} from './StoreDocAction';
import SqliteDatabase from 'better-sqlite3';

export class DataEngine implements IDataEngine {
  private _querySubs = new Map<number, Query>();
  private _nextQuerySub = 1;

  constructor(private _db: SqliteDatabase.Database) {
  }

  public async fetchQuery(query: Query): Promise<QueryResult> {
    const bindParams = new BindParams();
    const sql = sqlSelectFromQuery(query, bindParams);
    // console.warn("RUNNING SQL", sql, bindParams.getParams());
    const stmt = this._db.prepare(sql);
    return this._db.transaction(() => {
      const rows = stmt.raw().all(bindParams.getParams()) as CellValue[][];
      // console.warn("RESULT", rows);
      const queryResult: QueryResult = {
        tableId: query.tableId,
        tableData: {},
        actionNum: 0,       // TODO
      };
      for (const [index, col] of stmt.columns().entries()) {
        queryResult.tableData[col.name] = rows.map(r => r[index]);
      }
      return queryResult;
    })();
  }

  // TODO we need to come up with how to do streaming fetches over a network.
  public fetchQueryStreaming(query: Query, timeoutMs: number): QueryResultStreaming {
    const bindParams = new BindParams();
    const sql = sqlSelectFromQuery(query, bindParams);
    // console.warn("RUNNING SQL", sql, bindParams.getParams());

    // Note the convoluted flow here: we are returning an object, which includes a generator.
    // Caller is expected to iterate through the generator. This iteration happens inside a DB
    // transaction, so we keep a transaction going. In WAL mode this should not prevent other
    // reads or writes in parallel (on other connections!), but does prevent checkpointing. So
    // there is also a timeout. If the reader of the genrator doesn't finish within timeoutMs, the
    // generator will throw an exception, and end the transaction.
    // TODO this flow, especially error handling, needs careful testing.

    const db = this._db;

    let abortTimer: ReturnType<typeof setTimeout>|undefined;
    let iterator: IterableIterator<CellValue[]>|undefined;

    const cleanup = () => {
      if (abortTimer) {
        clearTimeout(abortTimer);
      }
      iterator?.return?.();
      db.exec('ROLLBACK');
    };

    db.exec('BEGIN');
    try {
      let timedOut = false;
      const onTimeout = () => {
        timedOut = true;
        cleanup();
      };
      abortTimer = setTimeout(onTimeout, timeoutMs);

      // This may be needed to force a snapshot to be taken (not sure). More sensibly, this may be
      // a good time to get actionNum (to identify the current state of the DB).
      // db.exec('SELECT 1');

      const stmt = this._db.prepare<unknown[], CellValue[]>(sql);

      function *generateRows() {
        try {
          iterator = stmt.raw().iterate(bindParams.getParams());
          for (const row of iterator) {
            yield row;
            if (timedOut) {
              throw new Error("Timed out");
            }
          }
        } finally {
          if (!timedOut) {
            cleanup();
          }
        }
      }

      const colIds = stmt.columns().map(c => c.name);

      const queryResult: QueryResultStreaming = {
        tableId: query.tableId,
        colIds,
        actionNum: 0,       // TODO
        rows: generateRows(),
      };
      return queryResult;
    } catch (e) {
      // This handles the case when we get an exception before returning to the caller, e.g. if
      // we constructed invalid SQL.
      cleanup();
      throw e;
    }
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

      const storeDocAction = new StoreDocAction(this._db);
      const results: unknown[] = [];
      for (const action of actionSet.actions) {
        results.push(storeDocAction.store(action));
      }
      // TODO For each subscription, query and queue the data to send to it.
      // NOTE: We could use a separate DB connection with an open read transaction to avoid
      // keeping the write transaction open; consider it if needed for performance.
      // (Efficient subscriptions are their own project, not done yet.

      return {results};
    }).immediate();
  }

  private _doQuerySubscribe(query: Query, callback: QuerySubCallback): QuerySubId {
    const subId = this._nextQuerySub++;
    this._querySubs.set(subId, query);
    return subId;
  }

}
