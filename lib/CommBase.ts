import {IMessage, MsgType, StreamingData, StreamingRpc, StreamingRpcOptions} from './StreamingRpc';

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
  private _lastReqId = 0;
  private _pendingCalls = new Map<number, CallObj>();
  // key may represent an incoming call or signal, or an outgoing response; we encode these as
  // streamKey of `${mtype}:${reqId}`.
  private _pendingStreams = new Map<string, StreamingHelper>();

  constructor(private _options: StreamingRpcOptions) {}

  public initialize(options: StreamingRpcOptions): void {
    this._options = options;
  }

  public makeCall(callData: StreamingDataChecked): Promise<StreamingDataChecked> {
    return new Promise<StreamingDataChecked>((resolve, reject) => {
      const reqId = ++this._lastReqId;
      const callObj: CallObj = {reqId, resolve};
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
      // Check if this message is part of a streaming portion.
      const streamKey = getStreamKey(msg);
      const chunks = this._pendingStreams.get(streamKey);
      if (chunks) {
        if (msg.error) {
          chunks._supplyError(this._options.msgToError(msg.error));
          this._pendingStreams.delete(streamKey);
        } else if (!msg.more) {
          chunks._finishChunks();
          this._pendingStreams.delete(streamKey);
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
          await this._options.sender.waitToDrain();
          await this._sendMessage({mtype, reqId, data: chunk, more: true});
        }
        await this._sendMessage({mtype, reqId});
      } catch (err) {
        if (err instanceof SendError) {
          throw err;
        }
        await this._sendMessage({mtype, reqId, error: this._options.errorToMsg(err)});
      }
    }
  }

  private async _sendMessage(msg: IMessage) {
    try {
      await this._options.sender.sendMessage(msg);
    } catch (err) {
      // Wrap sending errors, to be able to tell them apart from errors that come from handlers.
      throw new SendError(err);
    }
  }

  private _onMessageCall(msg: IMessage): void {
    const reqId = msg.reqId;
    if (msg.error) {
      throw new Error(`Unexpected call with an error, reqId ${reqId}`);
    }
    const chunks = this._getChunksIfStreaming(msg);
    (async () => {
      try {
        const respData = await this._options.callHandler({value: msg.data, chunks}) as StreamingDataChecked;
        await this._sendStreamingData(MsgType.Resp, reqId, respData);
      } catch (err) {
        if (err instanceof SendError) {
          throw err;
        }
        await this._sendMessage({mtype: MsgType.Resp, reqId, error: this._options.errorToMsg(err)});
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
      callObj.resolve(Promise.reject<StreamingDataChecked>(this._options.msgToError(msg.error)));
    } else {
      const chunks = this._getChunksIfStreaming(msg);
      callObj.resolve({value: msg.data as UniqueTypes["Value"], chunks});
    }
  }

  private _getChunksIfStreaming(msg: IMessage): StreamingHelper|undefined {
    if (!msg.more) { return undefined; }
    // The call itself has a streaming portion.
    const chunks = new StreamingHelper();
    this._pendingStreams.set(getStreamKey(msg), chunks);
    return chunks;
  }
}

// Encoding of messages. This is specific to parseMessage/serializeMessage. Different variants
// would be fine, as long as both sides agree. (In particular, protocols more conscious of
// versioning may be better).
//
// Call     C<Flag><ID>:<opaque input>
// Signal   S<Flag><ID>:<opaque input>
// Resp     R<Flag><ID>:<opaque result of method>
//
// Here, <ID> is reqId, and <Flag> is "+" for `more: true`, and "!" for error (in which case data
// after ":" is the error portion). If colon (":") is missing, there is no data.

const mtypeCodes: [MsgType, string][] = [
  [MsgType.Call,    "C"],
  [MsgType.Signal,  "S"],
  [MsgType.Resp,    "R"],
];

const mtypeToCode = new Map<MsgType, string>(mtypeCodes);
const codeToMtype = new Map<string, MsgType>(mtypeCodes.map(([m, c]) => [c, m]));

export function parseMessage(input: string, parseData: (data: string) => unknown): IMessage {
  const mtype = codeToMtype.get(input[0]);
  if (!mtype) {
    throw new Error("Invalid input message (mtype)");
  }
  let reqIdStart = 1;
  let isError = false;
  let more = false;
  if (input[1] === '!') {
    isError = true;
    reqIdStart++;
  } else if (input[1] === '+') {
    more = true;
    reqIdStart++;
  }

  const dataStart = (input.indexOf(":") + 1) || input.length;
  const reqId = parseInt(input.slice(reqIdStart, dataStart - 1), 10);
  if (!reqId) {
    throw new Error("Invalid input message (reqId)");
  }
  const data = parseData(input.slice(dataStart));
  if (isError) {
    return {mtype, reqId, error: data};
  } else {
    return {mtype, reqId, data, more};
  }
}

export function serializeMessage(msg: IMessage, serializeData: (data: unknown) => string): string {
  const code = mtypeToCode.get(msg.mtype);
  if (!code) {
    throw new Error("Invalid message (mtype)");
  }
  let flag = "";
  let data = msg.data;
  if (msg.error) {
    flag = "!";
    data = msg.error;
  } else if (msg.more) {
    flag = "+";
  }
  return code + flag + msg.reqId + ":" + serializeData(data);
}

//======================================================================

type ChunkIterResult = IteratorResult<UniqueTypes["Chunk"], unknown>;

class StreamingHelper implements AsyncIterableIterator<UniqueTypes["Chunk"]> {
  private _queuedChunks: ChunkIterResult[] = [];
  private _queuedCallbacks: Array<(chunk: ChunkIterResult | Promise<ChunkIterResult>) => void>;
  private _finished = false;    // If finished, will skip further processing.

  public [Symbol.asyncIterator]() {
    return this;
  }

  public next(): Promise<ChunkIterResult> {
    const chunk = this._queuedChunks.shift();
    if (chunk) {
      return Promise.resolve(chunk);
    } else {
      return new Promise<ChunkIterResult>(resolve => {
        this._queuedCallbacks.push(resolve);
      });
    }
  }

  public return(value: unknown): Promise<ChunkIterResult> {
    this._finished = true;
    this._cleanup();
    return Promise.resolve({value, done: true});
  }

  public _supplyChunk(chunk: UniqueTypes["Chunk"]): void {
    if (this._finished) { return; }
    this._pushItem({value: chunk, done: false});
  }

  public _finishChunks(): void {
    if (this._finished) { return; }
    this._pushItem({value: undefined, done: true});
    this._cleanup();
  }

  public _supplyError(errObj: Error): void {
    if (this._finished) { return; }
    for (const callback of this._queuedCallbacks) {
      callback(Promise.reject(errObj));
    }
    this._cleanup();
  }

  private _cleanup() {
    this._queuedChunks = [];
    this._queuedCallbacks = [];
    this._finished = true;
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
