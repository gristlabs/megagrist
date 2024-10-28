import {ActionSet, ApplyResultSet, Query, QueryResult, QueryResultStreaming, QuerySubId} from './types';

export interface QueryStreamingOptions {
  timeoutMs: number;    // Abort if call doesn't complete in this long (e.g. client isn't reading).
  chunkRows: number;    // How many rows to include in a chunk. There is some per-chunk overhead.
}

export type QuerySubCallback = (actionSet: ActionSet) => void;

/**
 * This defines the typescript interface for the data engine.
 */
export interface IDataEngine {
  fetchQuery(query: Query): Promise<QueryResult>;

  fetchQueryStreaming(
    query: Query, options: QueryStreamingOptions, abortSignal?: AbortSignal
  ): Promise<QueryResultStreaming>;

  // See querySubscribe for requirements on unsubscribing.
  fetchAndSubscribe(query: Query, callback: QuerySubCallback): Promise<QueryResult>;

  // If querySubscribe succeeds, the data engine is now maintaining a subscription. It is the
  // caller's responsibility to release it with queryUnsubscribe(), once it is no longer needed.
  querySubscribe(query: Query, callback: QuerySubCallback): Promise<QuerySubId>;

  queryUnsubscribe(subId: QuerySubId): Promise<boolean>;

  applyActions(actionSet: ActionSet): Promise<ApplyResultSet>;
}
