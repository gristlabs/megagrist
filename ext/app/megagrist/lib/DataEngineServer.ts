import {IDataEngine} from '../lib/IDataEngine';
import {Channel, StreamingData, StreamingRpcOptions} from '../lib/StreamingRpc';
import {createStreamingRpc} from '../lib/StreamingRpcImpl';

interface Context {
  // The important thing about channel is that it must be an object preserved for all calls on the
  // same connection. It is used as a key in a WeakMap to keep track of the query subscriptions
  // made by that connection.
  channel: Channel;

  // Optionally, a call may include an AbortSignal, to allow it to abort a long-running operation.
  abortSignal?: AbortSignal;
}

type IDataEngineSvr = IDataEngine<Context>;

/**
 * Exposes a IDataEngine implementation as an RPC server over the given channel.
 */
export function createDataEngineServer(
  dataEngine: IDataEngineSvr,
  options: Pick<StreamingRpcOptions, "channel"|"verbose">
) {
  const rpc = createStreamingRpc({
    ...options,
    logWarn: (message: string, err: Error) => { console.warn(message, err); },
    callHandler: callHandler.bind(null, dataEngine, options.channel),
    signalHandler: () => { throw new Error("No signals implemented"); },
  });

  // NOTE that in this approach, server actions are always noticed and sent to the client. The
  // client's own addActionListerner only determines where received actions go next. That's not
  // wasteful in practice since clients normally subscribe, and we keep actions reasonably small.
  // Channel's disconnectSignal is used by dataEngine to unsubscribe automatically, so we don't
  // need a dedicated interface for that.
  dataEngine.addActionListener({channel: options.channel},
    (actionSet) => rpc.sendSignal({value: ["action", actionSet]}));

  return rpc;
}

// Maps each supported method name to whether or not it returns a streaming result.
const dataEngineMethods: {[key in keyof IDataEngineSvr]?: boolean} = {
  fetchQuery: false,
  fetchQueryStreaming: true,
  applyActions: false,
};

async function callHandler(
  dataEngine: IDataEngineSvr, channel: Channel, callData: StreamingData, abortSignal?: AbortSignal
): Promise<StreamingData> {
  // Parameters of methods are packaged as {value: [method, ...args]}.
  // Result is the `.value` property of the returned data, except for calls with streaming
  // results, for which the complete returned data is the result.
  // TODO This implementation doesn't check the validity of anything received.

  if (!Array.isArray(callData.value)) {
    throw new Error("Invalid call");
  }
  const [method, ...args] = callData.value as [keyof IDataEngineSvr, ...unknown[]];
  if (!dataEngineMethods.hasOwnProperty(method)) {
    throw new Error(`Invalid method to call: ${method}`);
  }

  const context: Context = {channel, abortSignal};
  const isStreaming = dataEngineMethods[method];
  const result = await (dataEngine[method] as (...args: unknown[]) => Promise<unknown>)(context, ...args);
  if (isStreaming) {
    return result as StreamingData;
  } else {
    return {value: result};
  }
}
