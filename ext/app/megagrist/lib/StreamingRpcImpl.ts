import {IMessage, MsgType, StreamingData, StreamingRpc, StreamingRpcOptions} from './StreamingRpc';

/**
 * Primary way of creating a StreamingRpc object.
 */
export function createStreamingRpc(options: StreamingRpcOptions): StreamingRpc {
  const rpc: StreamingRpc = new StreamingRpcImpl();
  rpc.initialize(options);
  return rpc;
}

export class SendError extends Error {
  constructor(public origError: Error) {
    super(origError.message);
  }
}

// We don't care about the actual types here, but define unique types to get the strongest type
// checking internally within this module.
interface UniqueTypes {
  readonly Value: unique symbol;
  readonly Chunk: unique symbol;
}

type StreamingDataChecked = StreamingData<UniqueTypes["Value"], UniqueTypes["Chunk"]>;

interface CallObj {
  reqId: number;
  resolve: (data: StreamingDataChecked | Promise<StreamingDataChecked>) => void;
}

export class StreamingRpcImpl implements StreamingRpc {
  private _options: StreamingRpcOptions;
  private _lastReqId = 0;
  private _pendingCalls = new Map<number, CallObj>();
  // key may represent an incoming call or signal, or an outgoing response; we encode these as
  // streamKey of `${mtype}:${reqId}`.
  private _pendingStreams = new Map<string, StreamingHelper>();

  // Any time we make a call, we keep an AbortController in case the other side wants to abort it.
  private _callControllers = new Map<number, AbortController>();

  public initialize(options: StreamingRpcOptions): void {
    this._options = options;
    options.channel.onmessage = this.dispatch.bind(this);

    const disconnectHandler = () => this._onDisconnect(this.disconnectSignal.reason as Error);
    this.disconnectSignal.addEventListener('abort', disconnectHandler, {once: true});
  }

  public get disconnectSignal() {
    return this._options.channel.disconnectSignal;
  }

  public makeCall(callData: StreamingDataChecked, abortSignal?: AbortSignal): Promise<StreamingDataChecked> {
    return new Promise<StreamingDataChecked>((resolve, reject) => {
      const reqId = ++this._lastReqId;
      const callObj: CallObj = {reqId, resolve};
      abortSignal?.addEventListener('abort', this._sendAbort.bind(this, reqId));
      this._pendingCalls.set(reqId, callObj);
      this._sendStreamingData(MsgType.Call, reqId, callData).catch((err: Error) => {
        // If _sendMessage fails, reject immediately.
        this._pendingCalls.delete(reqId);
        reject(err);
      });
    });
  }

  public sendSignal(signalData: StreamingDataChecked): Promise<void> {
    const reqId = ++this._lastReqId;
    return this._sendStreamingData(MsgType.Signal, reqId, signalData);
  }

  /**
   * Returns whether the message was successfully processed.
   */
  public dispatch(msg: IMessage): boolean {
    try {
      this._options.verbose?.("Received message:", pick(msg, 'mtype', 'reqId', 'more', 'abort'));

      // Check if this message is part of a streaming portion.
      const streamKey = getStreamKey(msg);
      const chunks = this._pendingStreams.get(streamKey);
      if (chunks) {
        if (msg.error) {
          chunks._supplyError(this._options.channel.msgToError(msg.error));
        } else if (!msg.more) {
          chunks._finishChunks();
        } else {
          chunks._supplyChunk(msg.data as UniqueTypes["Chunk"]);
        }
      } else {
        switch (msg.mtype) {
          case MsgType.Call: { this._onMessageCall(msg); break; }
          case MsgType.Signal: { this._onMessageSignal(msg); break; }
          case MsgType.Resp: { this._onMessageResp(msg); break; }
          default: throw new Error('Invalid mtype received in message');
        }
      }
      return true;
    } catch (err) {
      this._options.logWarn('dispatch failed', err);
      return false;
    }
  }

  private _onDisconnect(reason: Error) {
    const calls = Array.from(this._pendingCalls.values());
    this._pendingCalls.clear();
    for (const callObj of calls) {
      callObj.resolve(Promise.reject(reason));
    }

    const streams = Array.from(this._pendingStreams.values());
    this._pendingStreams.clear();
    for (const stream of streams) {
      stream._supplyError(reason);
    }
  }

  // Sends data.value, and iterates through chunks (if any) sending them too. May raise exception
  // on failure to send; but on failure to iterate through chunks, will send an error message to
  // the other wise.
  private async _sendStreamingData(mtype: MsgType, reqId: number, data: StreamingDataChecked): Promise<void> {
    if (!data.chunks) {
      await this._sendMessage({mtype, reqId, data: data.value});
    } else {
      await this._sendMessage({mtype, reqId, data: data.value, more: true});
      // Note that we only wait to drain for the streaming portion of calls and responses, which is
      // the only part where we control how fast we are consuming it.
      try {
        for await (const chunk of data.chunks) {
          // TODO does this loop exit if request is aborted???
          this.disconnectSignal.throwIfAborted();
          await this._options.channel.waitToDrain();
          await this._sendMessage({mtype, reqId, data: chunk, more: true});
        }
        await this._sendMessage({mtype, reqId});
      } catch (err) {
        // TODO: see if calling data.chunks.return/throw would allow implementer to abort work.
        if (err instanceof SendError) {
          throw err;
        }
        await this._sendMessage({mtype, reqId, error: this._options.channel.errorToMsg(err)});
      }
    }
  }

  private async _sendMessage(msg: IMessage) {
    try {
      this._options.verbose?.("Sending message:", pick(msg, 'mtype', 'reqId', 'more', 'abort'));
      await this._options.channel.sendMessage(msg);
    } catch (err) {
      // Wrap sending errors, to be able to tell them apart from errors that come from handlers.
      throw new SendError(err);
    }
  }

  private _sendAbort(reqId: number) {
    const streamKey = getStreamKey({mtype: MsgType.Resp, reqId});
    if (this._pendingCalls.has(reqId) || this._pendingStreams.has(streamKey)) {
      this._sendMessage({mtype: MsgType.Call, reqId, abort: true})
      .catch(() => {});     // Ignore errors from such a send.
    }
  }

  private _onMessageCall(msg: IMessage): void {
    const reqId = msg.reqId;
    if (msg.error) {
      throw new Error(`Unexpected call with an error, reqId ${reqId}`);
    }
    if (msg.abort) {
      this._callControllers.get(reqId)?.abort();
      return;
    }
    const chunks = this._getChunksIfStreaming(msg);
    const abortController = new AbortController();
    this._callControllers.set(reqId, abortController);
    (async () => {
      try {
        const abortSignal = AbortSignal.any([abortController.signal, this.disconnectSignal]);
        const respData = await this._options.callHandler(
          {value: msg.data, chunks}, abortSignal) as StreamingDataChecked;
        abortSignal.throwIfAborted();
        await this._sendStreamingData(MsgType.Resp, reqId, respData);
      } catch (err) {
        if (err instanceof SendError) {
          throw err;
        }
        this._options.verbose?.("Responding with an error", err);
        await this._sendMessage({mtype: MsgType.Resp, reqId, error: this._options.channel.errorToMsg(err)});
      } finally {
        this._callControllers.delete(reqId);
      }
    })().catch((err: Error) => {
      this._options.logWarn('failed to send response', err);
    });
  }

  private _onMessageSignal(msg: IMessage): void {
    const reqId = msg.reqId;
    if (msg.error) {
      throw new Error(`Unexpected signal with an error, reqId ${reqId}`);
    }
    const chunks = this._getChunksIfStreaming(msg);
    this._options.signalHandler({value: msg.data, chunks});
  }

  private _onMessageResp(msg: IMessage): void {
    const callObj = this._pendingCalls.get(msg.reqId);
    if (!callObj) {
      throw new Error(`Response to unknown reqId ${msg.reqId}`);
    }
    this._pendingCalls.delete(msg.reqId);
    if (msg.error) {
      // Call failed.
      callObj.resolve(Promise.reject<StreamingDataChecked>(this._options.channel.msgToError(msg.error)));
    } else {
      const chunks = this._getChunksIfStreaming(msg);
      callObj.resolve({value: msg.data as UniqueTypes["Value"], chunks});
    }
  }

  private _getChunksIfStreaming(msg: IMessage): StreamingHelper|undefined {
    if (!msg.more) { return undefined; }
    const streamKey = getStreamKey(msg);
    const cleanupCallback = () => this._pendingStreams.delete(streamKey);
    const chunks = new StreamingHelper(cleanupCallback);
    this._pendingStreams.set(streamKey, chunks);
    return chunks;
  }
}

type ChunkIterResult = IteratorResult<UniqueTypes["Chunk"], undefined>;

class StreamingHelper implements AsyncIterableIterator<UniqueTypes["Chunk"]> {
  private _queuedChunks: ChunkIterResult[] = [];
  private _queuedCallbacks: Array<(chunk: ChunkIterResult | Promise<ChunkIterResult>) => void> = [];
  // If finished, will skip further processing
  private _finished = false;
  // First value to supply to next() when iterator ends (successfully or via exception). Once the
  // first value is supplied, _endValue is reset, and subsequent calls will return a bland "stream
  // is done" result, as generators do.
  private _endValue: Promise<ChunkIterResult> | undefined;

  constructor(private _cleanupCallback: () => void) {}

  public [Symbol.asyncIterator]() {
    return this;
  }

  public next(): Promise<ChunkIterResult> {
    const chunk = this._queuedChunks.shift();
    if (chunk) {
      return Promise.resolve(chunk);
    } else {
      if (this._finished) { return this._useEndValue(); }
      return new Promise<ChunkIterResult>(resolve => {
        this._queuedCallbacks.push(resolve);
      });
    }
  }

  public return(value: undefined): Promise<ChunkIterResult> {
    if (this._finished) { return this._useEndValue(); }
    this._finished = true;
    const endValue: Promise<ChunkIterResult> = Promise.resolve({value, done: true});
    this._cleanup(endValue);
    return endValue;
  }

  // The method prefixed with underscore are only public for use by StreamingRpcImpl, and should
  // be considered private by outside users.

  public _supplyChunk(chunk: UniqueTypes["Chunk"]): void {
    if (this._finished) { return; }
    this._pushItem({value: chunk, done: false});
  }

  public _finishChunks(): void {
    if (!this._finished) {
      this._pushItem({value: undefined, done: true});
      this._cleanup();
    }
    this._cleanupCallback();
  }

  public _supplyError(errObj: Error): void {
    if (!this._finished) {
      const endValue = Promise.reject(errObj);
      this._cleanup(endValue);
    }
    this._cleanupCallback();
  }

  private _cleanup(endValue?: Promise<ChunkIterResult>) {
    this._finished = true;
    this._endValue = endValue;
    for (const callback of this._queuedCallbacks) {
      callback(this._useEndValue());
    }
    this._queuedCallbacks = [];
    // We do NOT clear _queuedChunks, next() calls should continue picking them up until the end.
    // We also don't call _cleanupCallback() here; it is only called once incoming messages
    // indicate the end of the stream, at which point we know to expect no more messages for it.
  }

  private _useEndValue(): Promise<ChunkIterResult> {
    if (this._endValue) {
      const endValue = this._endValue;
      this._endValue = undefined;
      return endValue;
    } else {
      return Promise.resolve({value: undefined, done: true});
    }
  }

  private _pushItem(item: ChunkIterResult) {
    const callback = this._queuedCallbacks.shift();
    if (callback) {
      callback(item);
    } else {
      this._queuedChunks.push(item);
    }
  }
}

function getStreamKey(msg: IMessage) {
  return `${msg.mtype}:${msg.reqId}`;
}

function pick<T, K extends keyof T>(obj: T, ...keys: K[]): Pick<T, K> {
  const result = {} as Pick<T, K>;
  for (const key of keys) {
    if (key in obj) {
      result[key] = obj[key];
    }
  }
  return result;
}

// TODO Hack to silence typescript error with older typescript version.
declare var AbortSignal: typeof globalThis.AbortSignal & {
  timeout(milliseconds: number): AbortSignal;
  any(signals: AbortSignal[]): AbortSignal;
}
