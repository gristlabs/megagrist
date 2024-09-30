import * as ws from 'ws';
import {DataEngineClient} from '../lib/DataEngineClient';
import {WebSocketChannel} from '../lib/StreamingChannel';
import {StreamingData} from '../lib/StreamingRpc';
import {createStreamingRpc} from '../lib/StreamingRpcImpl';
import {IpcChannel} from './ipcChannel';

const verbose = process.env.VERBOSE ? console.log : undefined;

async function getDataEngineClient(url: string): Promise<[ws.WebSocket, DataEngineClient]> {
  const websocket = new ws.WebSocket(url);
  await new Promise(resolve => websocket.once('upgrade', resolve));
  const channel = new WebSocketChannel(websocket, {verbose});
  return [websocket, new DataEngineClient({channel, verbose})];
}

async function withDataEngineClient<T>(url: string, cb: (dataEngine: DataEngineClient) => Promise<T>) {
  const [websocket, dataEngine] = await getDataEngineClient(url);
  try {
    return await cb(dataEngine);
  } finally {
    websocket.close();
  }
}

const readDBOptions: {[name: string]: (url: string, limit?: number) => Promise<unknown>} = {
  async readDBFull(url: string, limit?: number) {
    return await withDataEngineClient(url, async (dataEngine) => {
      const result = await dataEngine.fetchQuery({tableId: 'Table1', sort: ['id'], limit});
      let count = 0;
      let sumRowIds = 0;
      for (const rowId of result.tableData.id) {
        count += 1;
        if (count % 1000 === 0) {
          // console.warn("readDB at", count);
          // Return to event loop occasionally.
          await new Promise(r => setTimeout(r, 0));
        }
        sumRowIds += rowId as number;
      }
      return {count, sumRowIds};
    });
  },

  async readDBStreaming(url: string, limit?: number) {
    return await withDataEngineClient(url, async (dataEngine) => {
      const result = await dataEngine.fetchQueryStreaming({tableId: 'Table1', sort: ['id'], limit}, {
        timeoutMs: 60_000,
        chunkRows: 500,
      });
      let count = 0;
      let sumRowIds = 0;
      for await (const chunk of result.chunks) {
        // console.warn("readDB at", count);
        // Return to event loop after every chunk of rows.
        await new Promise(r => setTimeout(r, 0));
        for (const row of chunk) {
          ++count;
          sumRowIds += row[0] as number;
        }
      }
      return {count, sumRowIds};
    });
  },
};

function main() {
  async function callHandler(msg: StreamingData) {
    const {url, rows, readDBName} = msg.value as any;
    const result = await readDBOptions[readDBName](url, rows);
    return {value: result};
  }

  createStreamingRpc({
    channel: new IpcChannel(process),
    logWarn: (message: string, err: Error) => { console.warn(message, err); },
    callHandler,
    signalHandler: () => { throw new Error("No signals implemented"); },
    verbose,
  });
}

main();
