import {ActionSet, ApplyResultSet, QueryResult, QueryResultStreaming} from './types';
import {CellValue} from './DocActions';
import {IDataEngine, QueryStreamingOptions} from './IDataEngine';
import {BindParams, ExpandedQuery, sqlSelectFromQuery} from './sqlConstruct';
import {StoreDocAction} from './StoreDocAction';
import SqliteDatabase from 'better-sqlite3';
import {Emitter} from 'grainjs';

interface MinimalChannel {
  disconnectSignal: AbortSignal;
}

interface Context {
  // The important thing about channel is that it must be an object preserved for all calls on the
  // same connection. It is used as a key in a WeakMap to keep track of the query subscriptions
  // made by that connection.
  channel?: MinimalChannel;

  // Optionally, a call may include an AbortSignal, to allow it to abort a long-running operation.
  abortSignal?: AbortSignal;
}

export type DataEngineCallContext = Context;

abstract class BaseDataEngine implements IDataEngine<Context> {
  private _actionSetEmitter = new Emitter();

  public async fetchQuery(context: Context, query: ExpandedQuery): Promise<QueryResult> {
    const bindParams = new BindParams();
    const sql = sqlSelectFromQuery(query, bindParams);
    // console.warn("fetchQuery", sql, bindParams.getParams());
    return this.withDB((db) => db.transaction(() => {
      const stmt = db.prepare(sql);
      const rows = stmt.raw().all(bindParams.getParams()) as CellValue[][];
      // console.warn("RESULT", rows);
      const queryResult: QueryResult = {
        tableId: query.tableId,
        tableData: {id: []},
        actionNum: 0,       // TODO
      };
      for (const [index, col] of stmt.columns().entries()) {
        queryResult.tableData[col.name] = rows.map(r => r[index]);
      }
      return queryResult;
    })());
  }

  public async fetchQueryStreaming(
    context: Context, query: ExpandedQuery, options: QueryStreamingOptions
  ): Promise<QueryResultStreaming> {
    const abortSignal = context?.abortSignal;
    const bindParams = new BindParams();
    const sql = sqlSelectFromQuery(query, bindParams);
    // console.warn("fetchQueryStreaming", sql, bindParams.getParams());

    // Note the convoluted flow here: we are returning an object, which includes a generator.
    // Caller is expected to iterate through the generator. This iteration happens inside a DB
    // transaction, so we keep a transaction going. In WAL mode this should not prevent other
    // reads or writes in parallel (on other connections!), but does prevent checkpointing. So
    // there is also a timeout. If the reader of the generator doesn't finish within timeoutMs, the
    // generator will throw an exception, and end the transaction.
    // TODO this flow, especially abortSignal, timeout and error handling, needs careful testing.
    // Also acquireDB/releaseDB adds complexity. (Not that the new "using" construct might
    // simplify this substantially!)

    const timeoutSignal = AbortSignal.timeout(options.timeoutMs);
    const fullAbortSignal = abortSignal ? AbortSignal.any([abortSignal, timeoutSignal]) : timeoutSignal;

    // let abortTimer: ReturnType<typeof setTimeout>|undefined;
    let iterator: IterableIterator<CellValue[]>|undefined;

    let cleanupCalled = false;
    const cleanup = () => {
      cleanupCalled = true;
      fullAbortSignal.removeEventListener('abort', cleanup);
      iterator?.return?.();
      db.exec('ROLLBACK');
      this.releaseDB(db);
    };

    const db = this.acquireDB();
    try {
      db.exec('BEGIN');
    } catch (e) {
      this.releaseDB(db);
      throw e;
    }
    fullAbortSignal.addEventListener('abort', cleanup);
    try {
      // This may be needed to force a snapshot to be taken (not sure). More sensibly, this may be
      // a good time to get actionNum (to identify the current state of the DB).
      // db.exec('SELECT 1');

      const stmt = db.prepare<unknown[], CellValue[]>(sql);

      async function *generateRows() {
        try {
          iterator = stmt.raw().iterate(bindParams.getParams());
          let chunk: CellValue[][] = [];
          for (const row of iterator) {
            chunk.push(row);
            if (chunk.length === options.chunkRows) {
              yield chunk;
              fullAbortSignal.throwIfAborted();
              chunk = [];
            }
          }
          if (chunk.length > 0) {
            yield chunk;
            fullAbortSignal.throwIfAborted();
            chunk = [];
          }
        } finally {
          if (!cleanupCalled) {
            cleanup();
          }
        }
      }

      const colIds = stmt.columns().map(c => c.name);

      const queryResult: QueryResultStreaming = {
        value: {
          tableId: query.tableId,
          actionNum: 0,       // TODO
          colIds,
        },
        chunks: generateRows(),
      };
      return queryResult;
    } catch (e) {
      // This handles the case when we get an exception before returning to the caller, e.g. if
      // we constructed invalid SQL.
      cleanup();
      throw e;
    }
  }

  public async applyActions(context: Context, actionSet: ActionSet): Promise<ApplyResultSet> {
    return this.withDB((db) => db.transaction((): ApplyResultSet => {
      // TODO: In the future, we need to pass actionSet through Access Rules,
      // Data Engine (to trigger anything synchronous), Access Rules again, all within a
      // transaction to ensure we are seeing a consistent view of DB and no other connection
      // attempts to write meanwhile.

      const storeDocAction = new StoreDocAction(db);
      const results: unknown[] = [];
      for (const action of actionSet.actions) {
        results.push(storeDocAction.store(action));
      }
      // TODO For each subscription, query and queue the data to send to it.
      // NOTE: We could use a separate DB connection with an open read transaction to avoid
      // keeping the write transaction open; consider it if needed for performance.
      // (Efficient subscriptions are their own project, not done yet.)
      this._actionSetEmitter.emit(actionSet);

      return {results};
    }).immediate());
  }

  // Adds a callback to be called when any change happens in the document.
  public addActionListener(context: Context, callback: (actionSet: ActionSet) => void) {
    const listener = this._actionSetEmitter.addListener(callback);
    context.channel?.disconnectSignal.addEventListener('abort', () => listener.dispose(), {once: true});
    return listener;
  }

  protected abstract acquireDB(): SqliteDatabase.Database;
  protected abstract releaseDB(db: SqliteDatabase.Database): void;

  // Get a DB connection, and run the callback with it. It intentionally supports only synchronous
  // functions (like db.transaction()) -- anything async or streaming needs more care, in
  // particular to ensure we don't hold up a DB connection indefinitely.
  protected withDB<T>(callback: (db: SqliteDatabase.Database) => T): T {
    const db = this.acquireDB();
    try {
      return callback(db);
    } finally {
      this.releaseDB(db);
    }
  }
}

export class DataEngine extends BaseDataEngine {
  constructor(private _db: SqliteDatabase.Database) {
    super();
  }
  protected acquireDB(): SqliteDatabase.Database { return this._db; }
  protected releaseDB(db: SqliteDatabase.Database): void {}
}

export class DataEnginePooled extends BaseDataEngine {
  // TODO Probably a good idea to enforce some sort of max, and possibly also to clean up unused
  // connections after a spike, since unused connections at the minimum use up file descriptors.
  private _connectionPool: SqliteDatabase.Database[] = [];
  private _totalConnections = 0;
  private _inUseConnections = 0;

  constructor(private _dbPath: string, private _dbOptions: SqliteDatabase.Options) {
    super();
    this._connectionPool.push(this._createConnection());
  }

  protected acquireDB(): SqliteDatabase.Database {
    const db = this._connectionPool.pop() || this._createConnection();
    this._inUseConnections++;
    console.log(`DB ${this._dbPath}: acquire; ${this._inUseConnections} now in use`);
    return db;
  }

  protected releaseDB(db: SqliteDatabase.Database): void {
    this._connectionPool.push(db);
    this._inUseConnections--;
    console.log(`DB ${this._dbPath}: release; ${this._inUseConnections} now in use`);
  }

  private _createConnection() {
    const conn = SqliteDatabase(this._dbPath, this._dbOptions);
    this._totalConnections++;
    console.log(`DB ${this._dbPath}: added connection for a total of ${this._totalConnections}`);
    return conn;
  }
}

// TODO Hack to silence typescript error with older typescript version.
declare var AbortSignal: typeof globalThis.AbortSignal & {
  timeout(milliseconds: number): AbortSignal;
  any(signals: AbortSignal[]): AbortSignal;
}
