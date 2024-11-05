/**
 * This is a helper for tests that start a child process to call between the main test process and
 * the child process. It is used by Test3 and _test3Child.
 */
import {Channel, IMessage} from 'ext/app/megagrist/lib/StreamingRpc';
import type {ChildProcess} from 'child_process';

export class IpcChannel implements Channel {
  private _abortController = new AbortController();

  constructor(private _proc: ChildProcess | typeof process) {}
  public get disconnectSignal() { return this._abortController.signal; }
  public set onmessage(cb: (msg: IMessage) => void) { this._proc.on('message', cb); }
  public async sendMessage(msg: IMessage) { this._proc.send!(msg); }
  public waitToDrain() { return null; }
  public msgToError(errorFromMsg: unknown): Error { return errorFromMsg as Error; }
  public errorToMsg(error: Error): unknown { return error; }
};
