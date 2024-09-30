// TODO Test a few rpc interactions, including basic streaming, leave TODOs for more tests (errors, etc.)

import {WebSocketChannel} from '../lib/StreamingChannel';
import {StreamingData, StreamingRpc} from '../lib/StreamingRpc';
import {createStreamingRpc} from '../lib/StreamingRpcImpl';
import {assert} from 'chai';
import {AddressInfo} from 'net';
import * as ws from 'ws';

const verbose = process.env.VERBOSE ? console.log : undefined;

describe('StreamingRpc', function() {
  this.timeout(60000);

  function addressToUrl(address: string|AddressInfo|null): string {
    if (!address || typeof address !== 'object') { throw new Error(`Invalid address ${address}`); }
    return `http://localhost:${address.port}/`;
  }

  async function createClient(url: string): Promise<[ws.WebSocket, StreamingRpc]> {
    const websocket = new ws.WebSocket(url);
    await new Promise(resolve => websocket.once('upgrade', resolve));
    const channel = new WebSocketChannel(websocket, {verbose});
    return [websocket, createStreamingRpc({
      channel,
      logWarn: (message: string, err: Error) => { console.warn(message, err); },
      callHandler: () => { throw new Error("No calls implemented"); },
      signalHandler: () => { throw new Error("No signals implemented"); },
      verbose,
    })];
  }

  async function callHandler(data: StreamingData): Promise<StreamingData> {
    return data;
  }

  async function createServer(): Promise<ws.Server> {
    // Start websocket server on any available port.
    const server = new ws.Server({port: 0});
    server.on('connection', (websocket) => {
      const channel = new WebSocketChannel(websocket, {verbose});
      createStreamingRpc({
        channel,
        logWarn: (message: string, err: Error) => { console.warn(message, err); },
        callHandler,
        signalHandler: () => { throw new Error("No signals implemented"); },
        verbose,
      });
    });
    await new Promise(resolve => server.once('listening', resolve));
    console.warn("Running server at address", server.address());
    return server;
  }

  let server: ws.Server;
  before(async function() {
    server = await createServer();
  });
  after(async function() {
    await new Promise(resolve => server.close(resolve));
  });

  it('should complete a basic call', async function() {
    const [websocket, client] = await createClient(addressToUrl(server.address()));
    try {
      const response = await client.makeCall({value: "hello world"});
      assert.equal(response.value, "hello world");
      assert.equal(response.chunks, undefined);
    } finally {
      websocket.close();
    }
  });
});
