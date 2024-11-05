/**
 * When communicating with the client, we may send around big sets of data, e.g. many rows from a
 * table. To support many clients without overloading the server memory, we support **streaming**
 * data.
 *
 * This is possible with WebSockets. The idea is that we produce data to fill up a buffer, and
 * then pause until enough is read from the buffer to make space for more. Note that this only
 * affects sending of data.
 *
 * (On the receiving side, it will be read as the network makes it available, and emitted as
 * events. I found no options to throttle reads for a WebSocket, but also no need yet. But see
 * https://developer.mozilla.org/en-US/docs/Web/API/WebSocketStream for what may work some day.)
 *
 * In Node, the sending side (ws library) implements Stream.Writable interface. In fact,
 * 'websocket-stream' npm module may help with streaming data from Node (perhaps as
 * Readable.from(iterable).pipe(websocketStream).)
 *
 * In browser, there is no help for this, BUT WebSocket exposes 'bufferedAmonunt' variable, so
 * we could implement our own backpressure handling, based on a buffered "high water mark".
 *
 * The purpose of THIS module is to define communication that supports streaming, and exposes a
 * friendly interface. The interface is generic:
 *
 *  rpc = new StreamingRpc({
 *      callHandler: (callData: Data): Promise<Data>;
 *      signalHandler: (data: Data): void;
 *  });
 *  rpc.makeCall(callData: Data): Promise<Data>;
 *
 * In all cases, Data has this shape:
 *  {
 *    value: Value;
 *    chunks?: AsyncIterable<Chunk>;
 *  }
 *
 * If `chunks` member is present, it is an async iterable. I.e. you can go over them as:
 *
 *  for await (const chunk of data.chunks) { ...dealWithChunk(chunk); }
 *
 * To create a Data value, you can use an async generator:
 *  async function *generateChunks() {
 *    for (...) {
 *      yield chunk;
 *      rpc.disconnectSignal.throwIfAborted();    // Optionally
 *    }
 *  }
 *  rpc.makeCall({value, chunks: generateChunks()});
 */

/**
 * Defines the shape of data that supports streaming.
 */
export interface StreamingData<Value = unknown, Chunk = unknown> {
  value: Value;
  chunks?: AsyncIterable<Chunk>;
}

/**
 * Defines what we need for sending and receiving messages.
 */
export interface Channel {
  // Should be signalled when the channel has disconnected.
  disconnectSignal: AbortSignal;

  // Channel allows setting 'onmessage' callback, and will call it for every message received.
  set onmessage(cb: (msg: IMessage) => void);

  sendMessage(msg: IMessage): Promise<void>;

  // If sending should be paused (because it is filling up a buffer which needs to drain), should
  // return a promise for when sending may resume. Otherwise should return null.
  waitToDrain(): null|Promise<void>;

  // We also give Channel the responsibility to encode/decode exceptions into a serializable form.
  msgToError(errorFromMsg: unknown): Error;
  errorToMsg(error: Error): unknown;
}

/**
 * These are the options you need to provide to make and receive streaming RPC calls.
 */
export interface StreamingRpcOptions {
  channel: Channel;
  logWarn: (message: string, err: Error) => void;
  callHandler: (callData: StreamingData, abortSignal?: AbortSignal) => Promise<StreamingData>;
  signalHandler: (signalData: StreamingData) => void;
  // A function like console.log for verbose logging.
  verbose?: (...args: unknown[]) => void;
}

/**
 * The main StreamingRpc interface.
 */
export interface StreamingRpc {
  // May be used to detect when the channel has disconnected, and abort unnecessary work.
  // This is simply a reference to the disconnectSignal provided by the channel.
  disconnectSignal: AbortSignal;

  initialize(options: StreamingRpcOptions): void;
  makeCall(callData: StreamingData, abortSignal?: AbortSignal): Promise<StreamingData>;
  sendSignal(signalData: StreamingData): Promise<void>;
  dispatch(msg: IMessage): boolean;
}

// The rest should not be needed by users of StreamingRpc, but needed by StreamingRpc
// implementations, and to implement the channels to pass around messages.
//
// Messaging happens as follows:
// Call plain:      -> {Call, reqId, data}
// Call streaming:  -> {Call, reqId, data, more: true}      => seen as value
//                  -> {Call, reqId, data, more: true} * N  => seen as chunks
//                  -> {Call, reqId}                        => seen as end of chunks
// Aborting a call: -> {Call, reqId, abort: true}           => signal to abort response to a call
// Responses and Signals follow the same pattern, but start with MsgType.Resp or MsgType.Signal,
// respectively.
//
// If any of the messages contain an `error` field, then it always ends the streaming message, and
// `more` and `data` fields are ignored.

export interface IMessage {
  mtype: MsgType;
  reqId: number;
  more?: boolean;     // Is more data expected (i.e. more streamed chunks)? Defaults to false.
  abort?: boolean;    // If present in a Call message, the response should be ended with an "Aborted" error.
  data?: unknown;
  error?: unknown;    // If this message represents an error; then `more` and `data` are ignored.
}

export enum MsgType {
  Call,       // RPC call, which expects a response.
  Signal,     // A message or a call that doesn't expect a response.
  Resp,       // Response to a call.
}
