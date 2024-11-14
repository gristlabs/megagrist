import {ActionSet, ApplyResultSet, Query, QueryResult, QueryResultStreaming} from './types';
import type {IDisposable} from 'grainjs';

export interface QueryStreamingOptions {
  timeoutMs: number;    // Abort if call doesn't complete in this long (e.g. client isn't reading).
  chunkRows: number;    // How many rows to include in a chunk. There is some per-chunk overhead.
}

/**
 * This defines the typescript interface for the data engine.
 */
export interface IDataEngine<CallContext = null> {
  fetchQuery(context: CallContext, query: Query): Promise<QueryResult>;

  fetchQueryStreaming(
    context: CallContext, query: Query, options: QueryStreamingOptions
  ): Promise<QueryResultStreaming>;

  applyActions(context: CallContext, actionSet: ActionSet): Promise<ApplyResultSet>;

  // Adds a callback to be called when any change happens in the document.
  addActionListener(context: CallContext, callback: (actionSet: ActionSet) => void): IDisposable;
}
