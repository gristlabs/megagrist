// Like Test2, but via WebSockets:
// - Create WebSocketServer, and connect to it 25 times.
// - Each connection should do streaming reading from it.
//   All in one process is fine.
// - Keep track of memory used.

import {DataEngine} from '../lib/DataEngine';
import {createDataEngineServer} from '../lib/DataEngineServer';
import {DataEngineClient} from '../lib/DataEngineClient';
import {WebSocketChannel} from '../lib/StreamingChannel';
import * as sample1 from './sample1';
import {createTestDir, withTiming} from './testutil';
import {assert} from 'chai';
import SqliteDatabase from 'better-sqlite3';
import * as colors from 'ansi-colors';
import {AddressInfo} from 'net';
import * as ws from 'ws';

const verbose = process.env.VERBOSE ? console.log : undefined;

interface Stat { first?: number, max?: number, last?: number };

function updateStat(stat: Stat, value: number) {
  if (stat.first === undefined) {
    stat.first = value;
  }
  if (stat.max === undefined || value > stat.max) {
    stat.max = value;
  }
  stat.last = value;
}

class Memory {
  // These are measured in MB.
  public rss: Stat = {};
  public heap: Stat = {};
  public note() {
    const usage = process.memoryUsage();
    updateStat(this.rss, usage.rss / 1024 / 1024);
    updateStat(this.heap, usage.heapUsed / 1024 / 1024);
  }

  public deltaRss() { return this.rss.last! - this.rss.first!; }
  public maxDeltaRss() { return this.rss.max! - this.rss.first!; }
  public deltaHeap() { return this.heap.last! - this.heap.first!; }
  public maxDeltaHeap() { return this.heap.max! - this.heap.first!; }
}


/**
 * This test parallels Test2, but via WebSockets. We do many fetches in parallel, via WebSockets
 * and RPC, and measure time and memory consumption. In particular, we compare full fetches to
 * streaming fetches to ensure that the latter reduce memory consumption even via WebSockets.
 */
describe('Test3', function() {
  this.timeout(60000);

  let testDir: string;
  before(async function() {
    testDir = await createTestDir('Test3');
  });

  async function setUpDB(dbPath: string) {
    const db: SqliteDatabase.Database = SqliteDatabase(dbPath, {
      verbose: process.env.VERBOSE ? console.log : undefined
    });
    db.exec("PRAGMA journal_mode=WAL");
    const dataEngine = new DataEngine(db);

    // Run actions to create a table.
    await sample1.createTable(dataEngine, 'Table1');
    await sample1.populateTable(dataEngine, 'Table1', 1000, 1000);
    return dataEngine;
  }

  function addressToUrl(address: string|AddressInfo|null): string {
    if (!address || typeof address !== 'object') { throw new Error(`Invalid address ${address}`); }
    return `http://localhost:${address.port}/`;
  }

  async function getDataEngineClient(address: AddressInfo): Promise<[ws.WebSocket, DataEngineClient]> {
    const websocket = new ws.WebSocket(addressToUrl(address));
    await new Promise(resolve => websocket.once('upgrade', resolve));
    const channel = new WebSocketChannel(websocket, {verbose});
    return [websocket, new DataEngineClient({channel, verbose})];
  }

  async function withDataEngineClient<T>(address: AddressInfo, cb: (dataEngine: DataEngineClient) => Promise<T>) {
    const [websocket, dataEngine] = await getDataEngineClient(address);
    try {
      return await cb(dataEngine);
    } finally {
      websocket.close();
    }
  }

  async function readDBFull(address: AddressInfo, memory: Memory, limit?: number) {
    return await withDataEngineClient(address, async (dataEngine) => {
      const result = await dataEngine.fetchQuery({tableId: 'Table1', sort: ['id'], limit});
      let count = 0;
      let sumRowIds = 0;
      for (const rowId of result.tableData.id) {
        count += 1;
        if (count % 1000 === 0) {
          // console.warn("readDB at", count);
          // Return to event loop occasionally.
          await new Promise(r => setTimeout(r, 0));
          memory.note();
        }
        sumRowIds += rowId as number;
      }
      return {count, sumRowIds};
    });
  }

  async function readDBStreaming(address: AddressInfo, memory: Memory, limit?: number) {
    return await withDataEngineClient(address, async (dataEngine) => {
      const result = await dataEngine.fetchQueryStreaming({tableId: 'Table1', sort: ['id'], limit}, {
        timeoutMs: 60_000,
        chunkRows: 500,
      });
      let count = 0;
      let sumRowIds = 0;
      for await (const chunk of result.chunks) {
        console.warn("readDB at", count);
        // Return to event loop after every chunk of rows.
        await new Promise(r => setTimeout(r, 0));
        memory.note();
        for (const row of chunk) {
          ++count;
          sumRowIds += row[0] as number;
        }
      }
      return {count, sumRowIds};
    });
  }

  for (const [index, readDB] of [readDBStreaming, readDBFull].entries()) {
    describe(`with ${readDB.name} ${index}`, function() {

      async function createServer(dbPath: string): Promise<ws.Server> {
        // Start websocket server on any available port.
        const server = new ws.Server({port: 0});
        server.on('connection', (websocket) => {
          const db: SqliteDatabase.Database = SqliteDatabase(dbPath, {});
          const dataEngine = new DataEngine(db);
          const channel = new WebSocketChannel(websocket, {verbose});
          createDataEngineServer(dataEngine, {channel, verbose});
        });
        await new Promise(resolve => server.once('listening', resolve));
        return server;
      }

      it('parallel reads', async function() {
        const dbPath = `${testDir}/parallel_reads${index}.grist`;
        await setUpDB(dbPath);
        const server = await createServer(dbPath);
        const address = server.address() as AddressInfo;
        try {

          const NReps = 25;
          const NRows = 200_000;

          await withTiming(`single fetch of ${NRows} rows`, async() => {
            const memory = new Memory();
            memory.note();
            const result = await readDB(address, memory, NRows);
            console.warn(`maxDelta: rss ${memory.maxDeltaRss()} MB, heap ${memory.maxDeltaHeap()} MB`);
            assert.equal(result.count, NRows);
            assert.equal(result.sumRowIds, NRows * (NRows + 1) / 2);
          });

          await withTiming(`parallel fetches: ${NReps} of ${NRows} rows`, async () => {
            // Start off with some garbage-collecting, to reduce variability of results, hopefully.
            gc?.();

            const memory = new Memory();
            memory.note();
            const results = await Promise.all(Array.from(Array(NReps), () => readDB(address, memory, NRows)));
            assert.lengthOf(results, NReps);
            console.warn(`maxDelta: rss ${memory.maxDeltaRss()} MB, heap ${memory.maxDeltaHeap()} MB`);
            console.warn(`- per query: rss ${memory.maxDeltaRss() / NReps} MB, ` +
              colors.blue(`heap ${memory.maxDeltaHeap() / NReps} MB`));
            for (const result of results) {
              assert.equal(result.count, NRows);
              assert.equal(result.sumRowIds, NRows * (NRows + 1) / 2);
            }
          });
        } finally {
          server.close();
        }
      });
    });
  }
});
