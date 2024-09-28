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
 * events. I found no options to throttle reads for a WebSocket, but also no need.)
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
 *    }
 *  }
 *  makeCall({value, chunks: generateChunks()});
 */
export interface IMessage {
  mtype: MsgType;
  reqId: number;
  more?: boolean;     // Is more data expected (i.e. more streamed chunks)? Defaults to false.
  data?: unknown;
  error?: unknown;    // If this message represents an error; then `more` and `data` are ignored.
}

// Messaging happens as follows:
// Call plain:      -> {Call, reqId, data}
// Call streaming:  -> {Call, reqId, data, more: true}      => seen as value
//                  -> {Call, reqId, data, more: true} * N  => seen as chunks
//                  -> {Call, reqId}                        => seen as end of chunks
// Responses and Signals follow the same pattern, but start with MsgType.Resp or MsgType.Signal,
// respectively.
//
// If any of the messages contain an `error` field, then it always ends the streaming message, and
// `more` and `data` fields are ignored.
export enum MsgType {
  Call,       // RPC call, which expects a response.
  Signal,     // A message or a call that doesn't expect a response.
  Resp,       // Response to a call.
}

export interface StreamingData<Value = unknown, Chunk = unknown> {
  value: Value;
  chunks?: AsyncIterable<Chunk>;
}

/**
 * Defines what we need for sending messages to the other side.
 */
export interface Sender {
  sendMessage(msg: IMessage): Promise<void>;

  // If sending should be paused (because it is filling up a buffer which needs to drain), should
  // return a promise for when sending may resume. Otherwise should return null.
  waitToDrain(): null|Promise<void>;
}

export interface StreamingRpcOptions {
  sender: Sender;
  logWarn: (message: string, err: Error) => void;
  callHandler: (callData: StreamingData) => Promise<StreamingData>;
  signalHandler: (callData: StreamingData) => void;
  msgToError: (errorFromMsg: unknown) => Error;
  errorToMsg: (error: Error) => unknown;
}

export interface StreamingRpc {
  initialize(options: StreamingRpcOptions): void;
  makeCall(callData: StreamingData): Promise<StreamingData>;
  sendSignal(signalData: StreamingData): Promise<void>;
  dispatch(msg: IMessage): boolean;
}
