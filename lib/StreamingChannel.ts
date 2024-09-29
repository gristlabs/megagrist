import {Channel, IMessage, MsgType} from './StreamingRpc';
import type {WebSocket as WSWebSocket} from 'ws';

// How full ws.bufferedAmount should be before we start waiting.
const defaultHighWaterMark = 1024 * 512;

// How long to wait, in ms, before re-checking ws.bufferedAmount when it's full.
const defaultBufferTimeout = 250;

export class WebSocketChannel implements Channel {
  private _abortController = new AbortController();
  private _messageHandler: ((msg: IMessage) => void) | null = null;

  // The following implements backpressure handling based on ws.bufferedAmount, which should work
  // for browser WebSocket and for `ws` module. Engine.IO may work differently. TODO this part
  // feels unnecessarily custom, and it would be better to use something more standardized
  // (perhaps websocket-stream?)
  private _highWaterMark: number;
  private _bufferTimeout: number;
  private _drainPromise: Promise<void> | null = null;
  private _drainResolve: (() => void) | null = null;
  private _drainCheckInterval: ReturnType<typeof setInterval> | null = null;

  constructor(private _ws: WebSocket|WSWebSocket, options?: {highWaterMark?: number, bufferTimeout?: number}) {
    this._highWaterMark = options?.highWaterMark || defaultHighWaterMark;
    this._bufferTimeout = options?.bufferTimeout || defaultBufferTimeout;
    _ws.onmessage = this._onWSMessage.bind(this);
    _ws.onclose = this._onWSClose.bind(this);
  }

  public get disconnectSignal() {
    return this._abortController.signal;
  }

  public set onmessage(cb: ((msg: IMessage) => void)) {
    this._messageHandler = cb;
  }

  public async sendMessage(msg: IMessage): Promise<void> {
    return this._ws.send(serializeMessage(msg, JSON.stringify));
  }

  // If sending should be paused (because it is filling up a buffer which needs to drain), should
  // return a promise for when sending may resume. Otherwise should return null.
  public waitToDrain(): null|Promise<void> {
    if (this._checkIfDrained()) {
      return null;
    }
    if (!this._drainPromise) {
      this._drainPromise = new Promise((resolve) => {
        this._drainResolve = resolve;
      });
      this._drainCheckInterval = setInterval(() => this._checkIfDrained(), this._bufferTimeout);
    }
    return this._drainPromise;
  }

  // Turns a {message, code?} object into an Error with an optional `code` property.
  public msgToError(msgError: unknown): Error {
    const obj = (msgError && typeof msgError === 'object') ? msgError : {};
    const message = ('message' in obj && typeof obj.message === 'string') ? obj.message : 'Unknown remote error';
    const error = new Error(message);
    if ('code' in obj) {
      (error as any).code = obj.code;
    }
    return error;
  }

  // Turns an Error into {message, code?} object.
  public errorToMsg(error: Error): unknown {
    return {message: error.message, ...('code' in error ? {code: error.code} : {})};
  }

  private _checkIfDrained(): boolean {
    if (this._ws.bufferedAmount >= this._highWaterMark) {
      return false;
    }
    if (this._drainCheckInterval) {
      clearTimeout(this._drainCheckInterval);
      this._drainCheckInterval = null;
    }
    if (this._drainResolve) {
      this._drainResolve();
      this._drainResolve = null;
    }
    return true;
  }

  private _onWSMessage(ev: MessageEvent<string>) {
    this._messageHandler?.(parseMessage(ev.data, JSON.parse));
  }

  private _onWSClose() {
    this._abortController.abort(new Error("WebSocket closed"));
  }
}

// Encoding of messages. This is specific to parseMessage/serializeMessage. Different variants
// would be fine, as long as both sides agree. TODO: A protocol more conscious of
// versioning, or more standard, would be be better.
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

function parseMessage(input: string, parseData: (data: string) => unknown): IMessage {
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

function serializeMessage(msg: IMessage, serializeData: (data: unknown) => string): string {
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
