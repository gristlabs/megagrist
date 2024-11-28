// TODO darn, linting with typings isn't working in my editor in this directory :(
import type {GristWSConnection} from 'app/client/components/GristWSConnection';
import koArray, {KoArray} from 'app/client/lib/koArray';
import {DocData} from 'app/client/models/DocData';
import {DataRowModel} from 'app/client/models/DataRowModel';
import {reportError} from 'app/client/models/errors';
import {ISortedRowSet, RowList, RowSource} from 'app/client/models/rowset';
import * as rowset from 'app/client/models/rowset';
import {FilterColValues, QueryOperation} from 'app/common/ActiveDocAPI';
import {delay} from 'app/common/delay';
import {DisposableWithEvents} from 'app/common/DisposableWithEvents';
import {CellValue, DocAction, TableColValues, toTableDataAction} from 'app/common/DocActions';
import {isMegaEngineEnabled} from 'app/common/MegaEngineSettings';
import type {MinimalWebSocket} from 'app/common/MinimalWebSocket';
import type {GristLoadConfig} from 'app/common/gristUrls';
import type {CompareFunc} from 'app/common/gutil';
import {Sort} from 'app/common/SortSpec';
import type {TableData} from 'app/client/models/TableData';
import {isDataDocAction} from 'app/megagrist/lib/DocActions';
import type {ParsedPredicateFormula, Query} from 'app/megagrist/lib/types';
import type {UIRowId} from 'app/plugin/GristAPI';
import {WebSocketChannel} from 'app/megagrist/lib/WebSocketChannel';
import {DataEngineClient, IDataEngineCli} from 'app/megagrist/lib/DataEngineClient';
import {Emitter, IDisposableOwner} from 'grainjs';
import assert from 'assert';

type IDataEngine = IDataEngineCli;

export class MegaDocModel {
  public static isEnabled(engine?: string): boolean {
    const gristConfig: GristLoadConfig|undefined = (window as any).gristConfig;
    return isMegaEngineEnabled(engine, gristConfig?.supportEngines);
  }

  public static maybeCreate(conn: GristWSConnection, docData: DocData): MegaDocModel|null {
    const engine = docData.docSettings().engine;
    if (!this.isEnabled(engine)) {
      return null;
    }
    const megaDocModel = new MegaDocModel(docData);
    const socket = conn.getAuxSocket();
    if (socket) {
      megaDocModel._initDataEngineWithSocket(socket);
    } else {
      const onConnected = () => {
        conn.off('connectState', onConnected);
        megaDocModel._initDataEngineWithSocket(conn.getAuxSocket()!);
      };
      conn.on('connectState', onConnected);
    }
    return megaDocModel;
  }

  private _dataEngine: IDataEngine|null = null;
  private _tableEmitters = new WeakMap<TableData, Emitter>();

  constructor(private _docData: DocData) {}

  public createSortedRowSet(owner: IDisposableOwner, tableData: TableData, columns: TableData): ISortedRowSet {
    return MegaRowSet.create(owner, this, tableData, columns);
  }

  public getRowModelClass(rowSet: MegaRowSet): typeof DataRowModel{
    return class MegaRowModel extends DataRowModel {
      public assign(rowId: number|'new'|null) {
        this._withLoadedRow(rowId, () => super.assign(rowId), {eager: true});
      }
      protected _assignColumn(colName: string) {
        this._withLoadedRow(this.getRowId(), () => super._assignColumn(colName), {eager: false});
      }

      // Eager says to also call the callback before waiting for the row to load.
      private async _withLoadedRow(rowId: number|'new'|null, callback: () => void, options: {eager: boolean}) {
        const p: true|Promise<void> = rowSet.loadRow(rowId);
        if (p === true) {
          callback();
        } else {
          if (options.eager) {
            callback();
          }
          await p;
          if (this.getRowId() === rowId && !this.isDisposed()) {
            callback();
          }
        }
      }
    };
  }

  public get dataEngine(): IDataEngine {
    if (!this._dataEngine) {
      throw new Error("DataEngine not yet connected");
    }
    return this._dataEngine;
  }

  public getTableEmitter(tableData: TableData): Emitter {
    let emitter = this._tableEmitters.get(tableData);
    if (!emitter) {
      emitter = new Emitter();
      this._tableEmitters.set(tableData, emitter);
    }
    return emitter;
  }

  private _initDataEngineWithSocket(socket: MinimalWebSocket) {
    const channel = new WebSocketChannel(socket);
    this._dataEngine = new DataEngineClient({channel, verbose: console.log});
    this._dataEngine.addActionListener({}, (actions) => {
      console.warn("GOT ACTIONS", actions.actions);
      for (const action of actions.actions) {
        const tableId = action[1];
        const tableData = this._docData.getTable(tableId);
        if (tableData) { this.getTableEmitter(tableData).emit(action); }
      }
    });
  }
}

class MegaRowSet extends DisposableWithEvents implements ISortedRowSet {
  private _koArray: KoArray<UIRowId>;
  private _sortSpec: Sort.SortSpec = [];
  private _filterSpec: FilterColValues = {filters: {}, operations: {}};
  private _rowFetcher = new RowFetcher({
    batchDelayMsec: () => 100,
    fetch: (rowIds) => this._doFetchRows(rowIds),
    onError: (err) => this._onError(err),
  });
  private _onFinish: ((err?: Error|unknown) => void)|null = null;

  private _rebuildRowSet = makeDeferredFunc(0, makeAbortableFetcher({
    fetch: (abortController) => this._doRebuildRowSet(abortController),
    onError: (err) => this._doRebuildRowSetFinish(err),
  }));

  constructor(private _megaDocModel: MegaDocModel, private _tableData: TableData, private _columns: TableData) {
    super();
    this._koArray = this.autoDispose(koArray<UIRowId>());
    this.autoDispose(_megaDocModel.getTableEmitter(_tableData)?.addListener(this._onAction.bind(this)));
  }

  public loadRow(rowId: number|'new'|null): true|Promise<void> {
    return (typeof rowId === 'number') ? this._rowFetcher.loadRow(rowId) : true;
  }

  public getKoArray() { return this._koArray; }

  public updateSortSpec(sortSpec: Sort.SortSpec, cb: (err?: Error|unknown) => void): void {
    this._onFinish = cb;
    this._sortSpec = sortSpec;
    this._rebuildRowSet();
  }

  public updateFilters(linkingFilter: FilterColValues, cb: (err?: Error|unknown) => void): void {
    this._onFinish = cb;
    this._filterSpec = linkingFilter;
    this._rebuildRowSet();
  }

  public pause(doPause: boolean) {}
  public updateSort(compareFunc: CompareFunc<UIRowId>): void {}
  public subscribeTo(rowSource: RowSource): void {}
  public unsubscribeFrom(rowSource: RowSource): void {}
  public onAddRows(rows: RowList) { this._rebuildRowSet(); }
  public onRemoveRows(rows: RowList) { this._rebuildRowSet(); }
  public onUpdateRows(rows: RowList) { this._rebuildRowSet(); }

  private _getColIdsAffectingRowSet(): Set<string> {
    const colIds = new Set<string>();
    for (const colSpec of this._sortSpec) {
      const colRef = Sort.getColRef(colSpec);
      if (typeof colRef === 'number') {
        const colId = this._columns.getValue(colRef, 'colId');
        if (colId && typeof colId === 'string') {
          colIds.add(colId);
        }
      }
    }
    for (const colId of Object.keys(this._filterSpec)) {
      colIds.add(colId);
    }
    return colIds;
  }

  // Check if we can skip updating the rowset, i.e. the filtered and ordered list of rowIds.
  private _canSkipRowSetUpdate(action: DocAction) {
    if (action[0] === "BulkUpdateRecord") {
      const colValues = action[3];
      const colIds = Object.keys(colValues);
      const relevantColIds = this._getColIdsAffectingRowSet();
      // If none of the columns is relevant to sort or filters, then can skip updating rowset.
      if (!colIds.some(c => relevantColIds.has(c))) {
        return true;
      }
    }
    return false;
  }

  private _onAction(action: DocAction) {
    console.warn("GOT ACTION!", action);

    if (!isDataDocAction(action)) {
      throw new Error("We don't expect non-data actions through megagrist channel yet");
    }

    if (this._canSkipRowSetUpdate(action)) {
      console.warn("Action does not affect sort or filters");
    } else {
      const rowIds = action[2];
      if (rowIds.length > 0) {
        // Normal action, used for small changes.
        // TODO: this fetch may be interleaved with _rebuildRowSet() (e.g. due to linking
        // changes). Need clarity of which version of what we have.
        this._updateRowSet(rowIds);
      } else {
        // Stripped action, used for large changes. Rowset needs to be re-fetched.
        this._rebuildRowSet();
      }
    }

    // TODO For actions like Add, tableData.receiveAction() may not be the right thing (may add rows we
    // don't care to load; but also, that may be fine).
    if (action[0] == "BulkUpdateRecord") {
      const rowIds = action[2];
      if (rowIds.length > 0) {
        // Normal action, used for small changes. We can take values directly from the action.

        // Get the action applied to TableData. This triggers other events, but we, as the rowset,
        // don't listen to them (to skip unnecessary work), and emit the needed event manually.
        // Action may include rows not loaded; those should get ignored by TableData and
        // LazyArrayModel respectively.
        this._tableData.receiveAction(action);
        this.trigger('rowNotify', rowIds, action);
      } else {
        // Stripped action, used for large changes. We have to re-fetch all rows we care about. We
        // do it by invalidating all loaded rows and telling the "models" (LazyArrayModel &
        // RowModel instances) that the action applies to all rows. It so happens that
        // BaseRowModel pays attention to which columns changed but doesn't mind that rowIds list
        // is empty, so calls _assignColumn which does the right thing.
        this._rowFetcher.clearLoaded();
        this.trigger('rowNotify', rowset.ALL, action);
      }
    }
  }

  private async _updateRowSet(rowIds: number[]) {
    const query = this._buildQuery();
    query.rowIds = rowIds;
    query.includePrevious = true;

    const dataEngine = this._megaDocModel.dataEngine;
    // TODO small streaming calls wastefully reply with 3 messages instead of 1, but some stuff
    // (like on-demand formula expansions) were only added to the streaming version. Idea: keep a
    // single call in the interface (streaming), and make it not wasteful for small responses.
    const result = await dataEngine.fetchQueryStreaming({}, query, {timeoutMs: 60000, chunkRows: 10000});

    // What we get in this response is a list of pairs [rowId, previousRowId], one for each rowId
    // provided. We use this to rebuild the rowset in this._koArray, with the affected rows moved
    // into their correct positions. TODO this got the least of manual testing for reordering, and
    // none for adding/deleting. Unlikely to work correctly for those cases.
    const previousToRowId = new Map();
    const movedRowIds = new Set();
    for await (const chunk of result.chunks) {
      for (const row of chunk) {
        previousToRowId.set(row[1], row[0]);
        movedRowIds.add(row[0]);
      }
    }
    if (this.isDisposed()) { return; }
    const newRowIds: UIRowId[] = [];

    function addFollowing(rowId: UIRowId|null) {
      while (previousToRowId.has(rowId)) {
        const nextRowId = previousToRowId.get(rowId);
        newRowIds.push(nextRowId);
        rowId = nextRowId;
      }
    }
    addFollowing(null);
    for (let rowId of this._koArray.peek()) {
      if (movedRowIds.has(rowId)) {
        continue;
      }
      newRowIds.push(rowId);
      addFollowing(rowId);
    }
    this._koArray.assign(newRowIds);
  }

  private _onError(err: Error|unknown) {
    if (err instanceof Error && err.message?.includes('aborted')) { return; }
    alert(err);
  }

  private _buildQuery(): Query {
    // TODO: this translation doesn't (yet) support full SortSpec.
    const sort = [];
    for (const c of this._sortSpec) {
      if (typeof c === 'number') {
        const colRef = Math.abs(c);
        const colId = this._columns.getValue(colRef, 'colId');
        sort.push((c > 0 ? '' : '-') + colId);
      }
    }

    const filters = getLinkingFiltersAsPredicate(this._filterSpec) || ['Const', 1];
    return {
      tableId: this._tableData.tableId,
      sort,
      filters,
      columns: ['id'],
    };
  }


  private async _doRebuildRowSet(abortController: AbortController) {
    const query: Query = this._buildQuery();
    const dataEngine = this._megaDocModel.dataEngine;
    const abortSignal = abortController.signal;
    const result = await dataEngine.fetchQueryStreaming({abortSignal}, query, {timeoutMs: 60000, chunkRows: 10000});
    console.warn("GOT COLIDS", result.value.colIds);
    if (this.isDisposed()) { abortController.abort(); }
    abortSignal.throwIfAborted();

    this._koArray.splice(0);
    for await (const chunk of result.chunks) {
      if (this.isDisposed()) { abortController.abort(); }

      // Note that it's safe to exit the loop early; once the loop ends, any subsequent chunks that
      // arrive would get discarded, not queued.
      abortSignal.throwIfAborted();

      const rowIds = chunk.map(row => row[0] as number);
      this._koArray.push(...rowIds);
      // console.warn(`Got ${rowIds.length} rows for total of ${this._koArray.peekLength}`);
    }
    this._tableData.dataLoadedEmitter.emit([], []);
    this._doRebuildRowSetFinish();
    console.warn("TABLEDATA", this._tableData);
  }

  private _doRebuildRowSetFinish(err?: Error|unknown) {
    if (err && err instanceof Error) {
      if (err.message?.includes('aborted')) { return; }
      reportError(err);
    }
    this._onFinish?.(err);
    this._onFinish = null;
  }

  private async _doFetchRows(rowIds: number[]) {
    const tableId = this._tableData.tableId;
    const query: Query = {
      tableId,
      rowIds,
    };

    console.warn("Query", query);
    const dataEngine = this._megaDocModel.dataEngine;
    const result = await dataEngine.fetchQueryStreaming({}, query, {timeoutMs: 60000, chunkRows: 500});

    for await (const chunk of result.chunks) {
      const colValues: TableColValues = {id: []};
      for (let i = 0; i < result.value.colIds.length; i++) {
        colValues[result.value.colIds[i]] = chunk.map(row => row[i]) as CellValue[];
      }
      console.warn(`Loading ${chunk.length} rows`);
      this._tableData.loadPartial(toTableDataAction(tableId, colValues));
    }
  }
}

// TODO This function is generic to allow thorough testing.
function makeAbortableFetcher(options: {
  fetch: (abortController: AbortController) => Promise<void>;
  onError: (err: Error|unknown) => void;
}) {
  let currentAbortController: AbortController|undefined;
  return function() {
    currentAbortController?.abort();
    currentAbortController = new AbortController();
    Promise.resolve()
      .then(() => options.fetch(currentAbortController!))
      .catch(err => {
        if (err.name !== "AbortError") {
          options.onError(err);
        }
      });
  };
}

function makeDeferredFunc(msec: number, func: () => void) {
  let timeout: ReturnType<typeof setTimeout>|undefined;
  return function() {
    if (timeout) { clearTimeout(timeout); }
    timeout = setTimeout(func, msec);
  };
}


// TODO This class is made somewhat generic to allow thorough testing. This is important, it's
// fully of non-trivial async interactions.
class RowFetcher {
  // These are ready. We don't currently ever unload rows.
  private _loadedRows = new Set<number>;

  // We queue up requests for a small interval, to query as a batch.
  private _queuedRows = new Map<number, Promise<void>>;

  // These are rows being queried. New requests for these rows don't need a new query, but new
  // requests for other rows should get queued up for the next batch.
  private _pendingRows: Map<number, Promise<void>> | null = null;

  // For rows being loaded, the callback to call on load.
  private _queuedCallbacks: Array<(value?: Promise<void>) => void> = [];
  private _isFetchPending = false;

  constructor(private _options: {
    batchDelayMsec: () => number;
    fetch: (rowIds: number[]) => Promise<void>;
    onError: (err: Error|unknown) => void;
  }) {
  }

  // Load a row. Returns true if the row is already loaded; otherwise returns a promise for when
  // it is. The promise will get resolved if it loads, or rejected on failure. Any error
  // is separately reported to onError() callback, to allow ignoring the per-row rejection.
  public loadRow(rowId: number): true|Promise<void> {
    return (this._loadedRows.has(rowId) ||
      this._pendingRows?.get(rowId) ||
      this._queuedRows.get(rowId) ||
      this._addRowToQueue(rowId));
  }

  public invalidate(rowId: number) {
    this._loadedRows.delete(rowId);
  }

  public clearLoaded() {
    this._loadedRows.clear();
  }

  private _addRowToQueue(rowId: number) {
    const p = new Promise<void>((resolve) => {
      this._queuedCallbacks.push(resolve);
    });
    this._queuedRows.set(rowId, p);
    if (!this._isFetchPending) {
      this._maybeStartFetch();
    }
    return p;
  }

  private _maybeStartFetch() {
    this._startFetch().catch(err => this._options.onError(err));
  }

  private async _startFetch() {
    this._isFetchPending = true;
    await delay(this._options.batchDelayMsec());
    assert(!this._pendingRows);
    this._pendingRows = this._queuedRows;
    this._queuedRows = new Map();
    const queuedCallbacks = this._queuedCallbacks.splice(0);
    try {
      await this._options.fetch([...this._pendingRows.keys()]);
      for (const rowId of this._pendingRows.keys()) {
        this._loadedRows.add(rowId);
      }
      queuedCallbacks.forEach(cb => cb());
    } catch (err) {
      const result = Promise.reject(err);
      queuedCallbacks.forEach(cb => cb(result));
      throw err;
    } finally {
      this._pendingRows = null;
      this._isFetchPending = false;
    }
    // If no error, and more rows have been queued, schedule the next fetch.
    if (this._queuedRows.size > 0) {
      this._maybeStartFetch();
    }
  }
}

function getLinkingFiltersAsPredicate({filters, operations}: FilterColValues): ParsedPredicateFormula|null {
  const parts = Object.keys(filters).sort().map(colId =>
    getLinkingColFilterAsPredicate(colId, filters[colId], operations[colId]));
  return parts.length === 0 ? null : ['And', ...parts.filter(Boolean)];
}

function getLinkingColFilterAsPredicate(
  colId: string, values: CellValue[], operation: QueryOperation
): ParsedPredicateFormula|null {
  if (operation === "in") {
    if (values.length > 0) {
      return ['In', ['Name', colId], ['List', ...values.map<ParsedPredicateFormula>(v => ['Const', v])]];
    } else {
      return ['Const', 0];
    }
  } else {
    // TODO: the "intersection" operation may be hard to support.
    console.warn(`operation ${operation} (for ${colId}) not yet implemented in mega tables`);
    return null;
  }
}
