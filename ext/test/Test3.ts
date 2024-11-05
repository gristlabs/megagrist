// Like Test2, but via WebSockets:
// - Create WebSocketServer, and connect to it 25 times.
// - Each connection should do streaming reading from it.
//   All in one process is fine.
// - Keep track of memory used.

import {DataEngine} from 'ext/app/megagrist/lib/DataEngine';
import {createDataEngineServer} from 'ext/app/megagrist/lib/DataEngineServer';
import {createStreamingRpc} from 'ext/app/megagrist/lib/StreamingRpcImpl';
import {WebSocketChannel} from 'ext/app/megagrist/lib/WebSocketChannel';
import * as sample1 from './sample1';
import {createTestDir, withTiming} from './testutil';
import {IpcChannel} from './ipcChannel';
import {assert} from 'chai';
import SqliteDatabase from 'better-sqlite3';
import * as colors from 'ansi-colors';
import * as childProcess from 'child_process';
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

  for (const [index, readDBName] of ["readDBStreaming", "readDBFull"].entries()) {
    describe(`with ${readDBName} ${index}`, function() {
      function addressToUrl(address: string|AddressInfo|null): string {
        if (!address || typeof address !== 'object') { throw new Error(`Invalid address ${address}`); }
        return `ws://localhost:${address.port}/`;
      }

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
        const cleanup: (() => void)[] = [];
        try {
          const server = await createServer(dbPath);
          cleanup.push(() => { server.close(); });

          const url = addressToUrl(server.address());

          const childProc = childProcess.fork(`${__dirname}/_test3Child.js`);
          cleanup.push(() => childProc.kill());

          const childChannel = new IpcChannel(childProc);
          const childRpc = createStreamingRpc({
            channel: childChannel,
            logWarn: (message: string, err: Error) => { console.warn(message, err); },
            callHandler: () => { throw new Error("No calls implemented"); },
            signalHandler: () => { throw new Error("No signals implemented"); },
            verbose,
          });

          async function readDB(rows: number): Promise<{count: number, sumRowIds: number}> {
            const result = await childRpc.makeCall({value: {url, rows, readDBName}});
            return result.value as any;
          }

          const NReps = 25;
          const NRows = 100_000;

          await withTiming(`single fetch of ${NRows} rows`, () => withMemory(100, async (memory) => {
            const result = await readDB(NRows);
            console.warn(`maxDelta: rss ${memory.maxDeltaRss()} MB, heap ${memory.maxDeltaHeap()} MB`);
            assert.equal(result.count, NRows);
            assert.equal(result.sumRowIds, NRows * (NRows + 1) / 2);
          }));

          await withTiming(`parallel fetches: ${NReps} of ${NRows} rows`, () => withMemory(100, async (memory) => {
            const results = await Promise.all(Array.from(Array(NReps), () => readDB(NRows)));
            assert.lengthOf(results, NReps);
            console.warn(`maxDelta: rss ${memory.maxDeltaRss()} MB, heap ${memory.maxDeltaHeap()} MB`);
            console.warn(`- per query: rss ${memory.maxDeltaRss() / NReps} MB, ` +
              colors.blue(`heap ${memory.maxDeltaHeap() / NReps} MB`));
            for (const result of results) {
              assert.equal(result.count, NRows);
              assert.equal(result.sumRowIds, NRows * (NRows + 1) / 2);
            }
          }));
        } finally {
          for (const cleanupCallback of cleanup) {
            cleanupCallback();
          }
        }
      });
    });
  }
});

async function withMemory(intervalMs: number, callback: (memory: Memory) => Promise<void>): Promise<void> {
  const memory = new Memory();
  // Start off with some garbage-collecting, to reduce variability of results, hopefully.
  gc?.();
  memory.note();
  const interval = setInterval(() => memory.note(), intervalMs);
  try {
    await callback(memory);
  } finally {
    clearInterval(interval);
  }
}
