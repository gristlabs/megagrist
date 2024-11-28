import {IDataEngine} from '../lib/IDataEngine';
import {StreamingData, StreamingRpc, StreamingRpcOptions} from '../lib/StreamingRpc';
import {createStreamingRpc} from '../lib/StreamingRpcImpl';
import {ActionSet} from './types';
import {Emitter} from 'grainjs';

interface Context {
  // Optionally, a call may include an AbortSignal, to allow it to abort a long-running operation.
  abortSignal?: AbortSignal;
}

export type IDataEngineCli = IDataEngine<Context>;

export class DataEngineClient implements IDataEngineCli {
  public fetchQuery = this._makeMethod("fetchQuery");
  public fetchQueryStreaming = this._makeMethod("fetchQueryStreaming", true);
  public applyActions = this._makeMethod("applyActions");

  private _rpc: StreamingRpc;
  private _actionSetEmitter = new Emitter();

  constructor(options: Pick<StreamingRpcOptions, "channel"|"verbose">) {
    this._rpc = createStreamingRpc({
      ...options,
      logWarn: (message: string, err: Error) => { console.warn(message, err); },
      callHandler: () => { throw new Error("No calls implemented"); },
      signalHandler: (signalData) => this._onSignal(signalData),
    });
  }

  // Adds a callback to be called when any change happens in the document.
  public addActionListener(context: Context, callback: (actionSet: ActionSet) => void) {
    return this._actionSetEmitter.addListener(callback);
  }

  private _makeMethod<Method extends keyof IDataEngineCli>(method: Method, streaming = false) {
    // Parameters of methods are packaged as {value: [method, ...args]}.
    // Result is the `.value` property of the returned data, except for calls with streaming
    // results, for which the complete returned data is the result.
    // TODO This implementation doesn't check the validity of anything received.
    return async (...args: Parameters<IDataEngineCli[Method]>): Promise<Awaited<ReturnType<IDataEngineCli[Method]>>> => {
      const context: Context = args[0];
      const data = {value: [method, ...args.slice(1)]};
      const result = await this._rpc.makeCall(data, context.abortSignal);
      return (streaming ? result : result.value) as Awaited<ReturnType<IDataEngineCli[Method]>>;
    };
  }

  private _onSignal(signalData: StreamingData): void {
    if (!Array.isArray(signalData.value)) {
      throw new Error("Invalid signal");
    }
    const [eventName, ...args] = signalData.value;
    if (eventName === 'action') {
      return this._actionSetEmitter.emit(...args);
    }
    throw new Error(`Invalid signal type ${eventName}`);
  }
}
