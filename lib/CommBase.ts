/**
 * Here we define a little protocol to support Rpc-like calls with streaming.
 * For example, we want to be able to say:
 *    const api = getClientForMyApi(...);
 *    const [response, iterable] = await api.myMethod(...args);
 *    for await (const item of iterable) {
 *      process(item);
 *    }
 */
interface IMessage {
  mtype: MsgType;
  reqId: number;
  data: unknown;
}

export enum MsgType {
  Call,
  RespPart,   // Used when more parts are expected in the response.
  RespDone,   // Used for the last part of the response, including responses
              // without a streaming portion.
  RespErr,
}

interface CallObj {
  reqId: number;
  // We keep "resolve" callbacks for each promise, omitting "reject" for simplicity, since we can
  // always reject by calling resolve() with a rejected promise.
  callResolve?: (data: Response | Promise<Response>) => void;
  queuedChunks?: ResponseChunkIter[];
  queuedCallbacks?: Array<(chunk: ResponseChunkIter | Promise<ResponseChunkIter>) => void>;
}

interface Response<Result = unknown, Chunk = ResponseChunk> {
  data: Result;
  chunks?: AsyncIterable<Chunk>;
}

export const Dummy = Symbol('Dummy');
type ResponseChunk = typeof Dummy;

type ResponseChunkIter = IteratorResult<ResponseChunk, undefined>;

interface Sender {
  sendMessage(msg: IMessage): Promise<void>;

  // If sending should be paused because it is filling up a buffer which needs to drain, returns a
  // promise for when sending may resume. Otherwise returns null;
  waitToDrain(): null|Promise<void>;
}

// TODO
function checkData(data: unknown): ResponseChunk {
  return data as ResponseChunk;
}
// TODO
function checkError(data: unknown): Error {
  return data as Error;
}

export class Rpc {
  private _lastReqId = 0;
  private _pendingCalls = new Map<number, CallObj>();
  private _warn: (message: string) => void;     // TODO
  private _sender: Sender;  // TODO
  private _callback: (data: unknown) => Promise<Response>; // TODO

  public makeCall(data: unknown): Promise<Response> {
    return new Promise<Response>((callResolve, reject) => {
      const reqId = ++this._lastReqId;
      const callObj: CallObj = {reqId, callResolve};
      this._pendingCalls.set(reqId, callObj);
      const msg: IMessage = {mtype: MsgType.Call, reqId, data};
      // We send with checking for draining here because no mechanism to pause getting called
      // anyway; that mechanism is only used for streaming.
      this._sender.sendMessage(msg).catch((err: Error) => {
        // If _sendMessage fails, reject immediately.
        this._pendingCalls.delete(reqId);
        reject(err);
      });
    });
  }

  /**
   * Returns whether the message was successfully processed.
   */
  public dispatch(msg: IMessage): boolean {
    try {
      switch (msg.mtype) {
        case MsgType.Call: { this._onMessageCall(msg); break; }
        case MsgType.RespPart:
        case MsgType.RespDone: {
          const callObj = this._getRequiredCallObj(msg.reqId);
          const callResolve = callObj.callResolve;
          if (callResolve) {
            // Processing the main result of the call.
            callObj.callResolve = undefined;
            let chunks: AsyncIterableIterator<ResponseChunk>|undefined = undefined;
            if (msg.mtype === MsgType.RespPart) {
              callObj.queuedChunks = [];
              callObj.queuedCallbacks = [];
              const queuedChunks = callObj.queuedChunks;
              const queuedCallbacks = callObj.queuedCallbacks;
              chunks = {
                [Symbol.asyncIterator]() { return this; },
                next(): Promise<ResponseChunkIter> {
                  if (queuedChunks.length > 0) {
                    return Promise.resolve(queuedChunks.shift()!);
                  } else {
                    return new Promise<ResponseChunkIter>(resolve => {
                      queuedCallbacks.push(resolve);
                    });
                  }
                }
              };
            } else {
              this._pendingCalls.delete(msg.reqId);
            }
            callResolve({data: msg.data, chunks});
          } else {
            // Processing chunks of the iterable.
            const queuedChunks = callObj.queuedChunks;
            const queuedCallbacks = callObj.queuedCallbacks;
            if (!queuedCallbacks || !queuedChunks) {
              throw new Error("Bug");
            }
            const push = (item: ResponseChunkIter): void => {
              if (queuedCallbacks.length > 0) {
                const callback = queuedCallbacks.shift()!;
                callback(item);
              } else {
                queuedChunks.push(item);
              }
            };
            push({value: checkData(msg.data), done: false});
            if (msg.mtype === MsgType.RespDone) {
              push({value: undefined, done: true});
            }
          }
          break;
        }
        case MsgType.RespErr: {
          const callObj = this._popRequiredCallObj(msg.reqId);
          const errObj = checkError(msg.data);
          if (callObj.callResolve) {
            callObj.callResolve(Promise.reject<Response>(errObj));
          } else {
            const queuedCallbacks = callObj.queuedCallbacks;
            if (!queuedCallbacks) {
              throw new Error("Bug");
            }
            for (const callback of queuedCallbacks) {
              callback(Promise.reject(errObj));
            }
          }
          break;
        }
        default: throw new Error('Invalid message received (mtype)');
      }
      return true;
    } catch (err) {
      this._warn(`_dispatch failed: ${err}`);
      return false;
    }
  }

  private _onMessageCall(msg: IMessage): void {
    const reqId = msg.reqId;
    const doCall = async () => {
      try {
        const {data, chunks} = await this._callback(msg.data);
        if (chunks) {
          // Send response along with a subsequent stream of chunks.
          await this._sendResponseMsg({mtype: MsgType.RespPart, reqId, data});
          for await (const chunk of chunks) {
            await this._sendResponseMsg({mtype: MsgType.RespPart, reqId, data: chunk});
          }
          await this._sendResponseMsg({mtype: MsgType.RespDone, reqId, data: undefined});
        } else {
          // No streaming portion; just sent the response.
          await this._sendResponseMsg({mtype: MsgType.RespDone, reqId, data});
        }
      } catch (e) {
        await this._sendResponseMsg({mtype: MsgType.RespErr, reqId, data: e});
      }
    };
    doCall().catch((e: unknown) => this._warn(`Unexpected rejection in _onMessageCall: ${e}`));
  }

  private _sendResponseMsg(msg: IMessage): Promise<void>|null {
    this._sender.sendMessage(msg).catch((err: Error) => {
      this._warn(`failed to send response: ${err}`);
    });
    return this._sender.waitToDrain();
  }

  private _getRequiredCallObj(reqId: number): CallObj {
    const callObj = this._pendingCalls.get(reqId);
    if (!callObj) {
      throw new Error(`Response to unknown reqId ${reqId}`);
    }
    return callObj;
  }
  private _popRequiredCallObj(reqId: number): CallObj {
    const callObj = this._getRequiredCallObj(reqId);
    this._pendingCalls.delete(reqId);
    return callObj;
  }
}

// Encoding of messages. This is specific to parseMessage/serializeMessage. Other options would be
// fine, as long as both sides agree. (In particular, protocols more conscious of versioning may
// be better).
//
// Call      C<ID>:<opaque input>
// RespPart  r<ID>:<opaque result of method or stream chunk, with more chunks to follow>
// RespDone  R<ID>:<opaque result of method>
// RespErr   E<ID>:<opaque error value>

const mtypeCodes: [MsgType, string][] = [
  [MsgType.Call, "C"],
  [MsgType.RespPart, "r"],
  [MsgType.RespDone, "R"],
  [MsgType.RespErr, "E"],
];

const mtypeToCode = new Map<MsgType, string>(mtypeCodes);
const codeToMtype = new Map<string, MsgType>(mtypeCodes.map(([m, c]) => [c, m]));

export function parseMessage(input: string, parseData: (data: string) => unknown): IMessage {
  const mtype = codeToMtype.get(input[0]);
  if (!mtype) {
    throw new Error("Invalid input message (mtype)");
  }
  const colon = input.indexOf(":");
  const reqId = parseInt(input.slice(1, colon), 10);
  if (!reqId) {
    throw new Error("Invalid input message (reqId)");
  }
  const data = parseData(input.slice(colon + 1));
  return {mtype, reqId, data};
}

export function serializeMessage(msg: IMessage, serializeData: (data: unknown) => string): string {
  const code = mtypeToCode.get(msg.mtype);
  if (!code) {
    throw new Error("Invalid message (mtype)");
  }
  return code + msg.reqId + ":" + serializeData(msg.data);
}

//======================================================================
/**
 * This describes the minimum of communication that we need between the client and the data
 * engine.
 */
export interface CommBase {
  setSendMessage(sendMessage: (msg: unknown) => void): void;
  receiveMessage(msg: unknown): void;
}

// TODO should it just be a Duplex stream? No because no stream impl in browser.
// Goal:
// friendly interface that allows reading streaming data via an iterator
//    const response = await rpcStub.fetchQueryStreaming(...);
//    for await (const row of response.rows) {
//    }
// translates to:
//    sendMessage(requestId, methodName, args), register callback for requestId
//    on(message)
//      find callback by requestId
//      call it (resolve promise) with {
//        data      // what's directly in message
//        iterable  // iterating through should yield more data
//      }
//
//  OPTION 1 for iterable:
//    function * generator() {
//      while (read from socket) {
//        yield chunk we read
//      }
//    }
//  BUT, let's say another RPC call was initiated earlier, and server has a response ready to
//  send. It's hard for the server to delay the response past end-of-stream; and hard for client
//  to handle it sooner.
//
//  whlie loop could essentially call on(message) for anything that's NOT part of the stream.
//
//  OPTION 2 for iterable:
//    on(message) finds iterable by requestId
//      calls resolve method for async iterator's next() method, i.e. resolves the next chunk.
//


/*
  fetchQuery(query: Query): Promise<QueryResult>;
  fetchQueryStreaming(query: Query, timeoutMs: number): Promise<QueryResultStreaming>;
  fetchAndSubscribe(query: Query, callback: QuerySubCallback): Promise<QueryResult>;
  querySubscribe(query: Query, callback: QuerySubCallback): Promise<QuerySubId>;
  queryUnsubscribe(subId: QuerySubId): Promise<boolean>;
  applyActions(actionSet: ActionSet): Promise<ApplyResultSet>;


*/

/*

// Protobuf rpc. Supports streaming, but not sure how.
/Users/dmitry/devel/card-reader/node_modules/protobufjs/src/rpc.js

// Custom RPC implementation for form-ts-checker-webpack-plugin. Really similar to grainrpc. Just matches
// responses with calls. For some reason they switched to this custom one over worker-rpc.
/Users/dmitry/devel/software/tui.calendar/node_modules/fork-ts-checker-webpack-plugin/lib/rpc/RpcMessage.js

// Same as above, used by OLDER version of fork-ts-checker.
/Users/dmitry/devel/grist-tmp/node_modules/worker-rpc/lib/RpcProvider.js
/Users/dmitry/devel/software/tui.calendar/node_modules/worker-rpc/lib/RpcProvider.js

// ???
/Users/dmitry/devel/software/electron/lib/browser/rpc-server.js

// ???
/Users/dmitry/devel/grist-zapier/node_modules/zapier-platform-core/src/tools/create-rpc-client.js

=========================================================================
GRR. Websockets don't seem to implement Stream interface on both sides.

The sending side on Node does implement Writable interface (via ws library), including 'drain'
events. But the receiving side, even on Node, just emits 'message' events. There is no way to
process those "slowly", as far as I can tell. Actually, on Node side, can call pause()/unpause().

On browser side, typescript interfaces only show send() that takes neither a callback nor returns
a value; and addEventListener. So no concepts of backpressure in either direction.
Ooooh, I am wrong. WEbsocket exposes bufferedAmount! Which means I can keep my own high water mark
for writing.

What does this mean for a streaming interface?
- Only makes sense to stream data FROM node TO browser.
- There is no backpressure on browser, data will keep coming, if browser is slow to process it,
it's the browser's memory (wherever data is being queued) that would fill up.
- From Browser to Node, can we stream? E.g. uploading?

======================================================================
1. High level input

    a. Call comes in: fetchQueryStreaming(...args)
    b. Implementation responds with [result, asyncIterable]
       websocket.send({data: result, done: false})
       for (item of asyncIterable) {
          websocket.send({data: item, done: false})
       // ????
       websocket.send({data: undefined, done: true})
       // The above matches iterator.next() protocol.

    ^^^ should i translate to a lower level [value, nextPromise] chain?
    ^^^ how to respect send() backpressure?

            in "readable-stream" npm module:
              'Readable.from is not available in the browser'
              That's confusing -- no obvious reason for it. Could look further into why.

        Readable.from(asyncIterable).pipe(websocketStream)

        IF doable, could be an argument to stick with iterable interface, without translating
        down. BUT: (1) no Readable.from() in browser; (2) likely no websocketStream in browser.
        Alternative is to implement manually.

    c. To implement manually...
       websocket.send({data: result, done: false})
       if (returned false or reached high water mark) {
          await drain()
       }
       for (item of asyncIterable) {
          websocket.send({data: item, done: false})
          if (returned false or reached high water mark) {
             await drain()
          }
       }
       // ????
       websocket.send({data: undefined, done: true})
       drain() {
          on('drain', resolve)    // in node
          setTimeout() => if < high water mark, resolve



2. Lower-level input
      Implementation response (after await) with {data: result, next: nextPromise}
      websocket.send({result, done: false})
      await nextPromise => {data: item, next: nextPromise}
      websocket.send({item, done: false})
      await nextPromise => {data: item, next: nextPromise}
      websocket.send({item, done: false})
      await nextPromise => {data: item, next: undefined}
      websocket.send({item, done: true})

3. Translate high-level to low-level:
      await, get [result, asyncIterable]
      let asyncIterator;
      function proc(result) {
        const next = new Promise((resolve, reject) => {
          const value = asyncIterator.next();
          Promise.resolve(value).then(proc);
        });
        return {data: result, next};

    =======
    I may have a fundamental misunderstanding of the difference between (async) iterators and
    promise chains. An iterator doesn't produce the next value until next() is called. A Promise
    chain expects nothing from the caller. If caller never awaits, it can still resolve fully on
    its own schedule, perhaps immediately.

    So....
    Iterator protocol is the better fit here.


  private _callback: (data: unknown) => Promise<Response<unknown>>; // TODO
interface Response<T> {
  data: T;
  next?: Promise<Response<T>>;   // For streaming responses.
}




*/

