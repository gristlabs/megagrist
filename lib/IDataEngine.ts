import {ActionSet, ApplyResultSet, Query, QueryResult, QueryResultStreaming, QuerySubId} from './types';

/**
 * This defines the typescript interface for the data engine.
 */
export interface IDataEngine {
  fetchQuery(query: Query): Promise<QueryResult>;

  fetchQueryStreaming(query: Query, timeoutMs: number): Promise<QueryResultStreaming>;

  // See querySubscribe for requirements on unsubscribing.
  fetchAndSubscribe(query: Query, callback: QuerySubCallback): Promise<QueryResult>;

  // If querySubscribe succeeds, the data engine is now maintaining a subscription. It is the
  // caller's responsibility to release it with queryUnsubscribe(), once it is no longer needed.
  querySubscribe(query: Query, callback: QuerySubCallback): Promise<QuerySubId>;

  queryUnsubscribe(subId: QuerySubId): Promise<boolean>;

  applyActions(actionSet: ActionSet): Promise<ApplyResultSet>;
}

export type QuerySubCallback = (actionSet: ActionSet) => void;
