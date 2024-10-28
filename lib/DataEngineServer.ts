import {IDataEngine} from '../lib/IDataEngine';
import {StreamingData, StreamingRpcOptions} from '../lib/StreamingRpc';
import {createStreamingRpc} from '../lib/StreamingRpcImpl';

/**
 * Exposes a IDataEngine implementation as an RPC server over the given channel.
 */
export function createDataEngineServer(
  dataEngine: IDataEngine,
  options: Pick<StreamingRpcOptions, "channel"|"verbose">
) {
  return createStreamingRpc({
    ...options,
    logWarn: (message: string, err: Error) => { console.warn(message, err); },
    callHandler: callHandler.bind(null, dataEngine),
    signalHandler: () => { throw new Error("No signals implemented"); },
  });
}

// Maps each supported method name to whether or not it returns a streaming result.
const dataEngineMethods: {[key in keyof IDataEngine]: boolean} = {
  fetchQuery: false,
  fetchQueryStreaming: true,
  fetchAndSubscribe: false,
  querySubscribe: false,
  queryUnsubscribe: false,
  applyActions: false,
};

async function callHandler(
  dataEngine: IDataEngine, callData: StreamingData, abortSignal?: AbortSignal
): Promise<StreamingData> {
  // Parameters of methods are packaged as {value: [method, ...args]}.
  // Result is the `.value` property of the returned data, except for calls with streaming
  // results, for which the complete returned data is the result.
  // TODO This implementation doesn't check the validity of anything received.

  if (!Array.isArray(callData.value)) {
    throw new Error("Invalid call");
  }
  const [method, ...args] = callData.value as [keyof IDataEngine, ...unknown[]];
  if (!dataEngineMethods.hasOwnProperty(method)) {
    throw new Error(`Invalid method to call: ${method}`);
  }

  const isStreaming = dataEngineMethods[method];
  const result = await (dataEngine[method] as (...args: unknown[]) => Promise<unknown>)(...args, abortSignal);
  if (isStreaming) {
    return result as StreamingData;
  } else {
    return {value: result};
  }
}
