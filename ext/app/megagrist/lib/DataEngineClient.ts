import {IDataEngine} from '../lib/IDataEngine';
import {StreamingRpc, StreamingRpcOptions} from '../lib/StreamingRpc';
import {createStreamingRpc} from '../lib/StreamingRpcImpl';

export class DataEngineClient implements IDataEngine {
  public fetchQuery = this._makeMethod("fetchQuery");
  public fetchQueryStreaming = this._makeMethod("fetchQueryStreaming", true);
  public fetchAndSubscribe = this._makeMethod("fetchAndSubscribe");
  public querySubscribe = this._makeMethod("querySubscribe");
  public queryUnsubscribe = this._makeMethod("queryUnsubscribe");
  public applyActions = this._makeMethod("applyActions");

  private _rpc: StreamingRpc;

  constructor(options: Pick<StreamingRpcOptions, "channel"|"verbose">) {
    this._rpc = createStreamingRpc({
      ...options,
      logWarn: (message: string, err: Error) => { console.warn(message, err); },
      callHandler: () => { throw new Error("No calls implemented"); },
      signalHandler: () => { throw new Error("No signals implemented"); },
    });
  }

  private _makeMethod<Method extends keyof IDataEngine>(method: Method, streaming = false) {
    // Parameters of methods are packaged as {value: [method, ...args]}.
    // Result is the `.value` property of the returned data, except for calls with streaming
    // results, for which the complete returned data is the result.
    // TODO This implementation doesn't check the validity of anything received.
    return async (...args: Parameters<IDataEngine[Method]>): Promise<Awaited<ReturnType<IDataEngine[Method]>>> => {
      let abortSignal: AbortSignal|undefined;
      const lastArg = args[args.length - 1];
      if (lastArg instanceof AbortSignal) {
        abortSignal = lastArg;
        args.pop();
      }
      const data = {value: [method, ...args]};
      const result = await this._rpc.makeCall(data, abortSignal);
      return (streaming ? result : result.value) as Awaited<ReturnType<IDataEngine[Method]>>;
    };
  }
}
