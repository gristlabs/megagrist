import {ActionSet, ApplyResultSet, Query, QueryResult, QueryResultStreaming, QuerySubId} from './types';

// If SubscriptionOptions asks for a new subscription, and the query succeeds, the data engine is
// now maintaining a subscription. Subsequent queries may create new subscriptions or update
// existing ones. Any subscriptions stays active until queryUnsubscribe() is called (for RPC-based
// implementation, a disconnect should also unsubscribe all subscriptions).

export interface SubscriptionOptions {
  newSubCallback?: QuerySubCallback;      // To make a new subscription
  existingSubId?: QuerySubId;             // To update an existing subscription
}

export interface QueryStreamingOptions {
  timeoutMs: number;    // Abort if call doesn't complete in this long (e.g. client isn't reading).
  chunkRows: number;    // How many rows to include in a chunk. There is some per-chunk overhead.
}

export type QuerySubCallback = (actionSet: ActionSet) => void;

/**
 * This defines the typescript interface for the data engine.
 */
export interface IDataEngine<CallContext = null> {
  fetchQuery(context: CallContext, query: Query, options?: SubscriptionOptions): Promise<QueryResult>;

  fetchQueryStreaming(
    context: CallContext, query: Query, options: QueryStreamingOptions & SubscriptionOptions
  ): Promise<QueryResultStreaming>;

  queryUnsubscribe(context: CallContext, subId: QuerySubId): Promise<boolean>;

  applyActions(context: CallContext, actionSet: ActionSet): Promise<ApplyResultSet>;
}
