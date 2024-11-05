import {DataEngine} from 'ext/app/megagrist/lib/DataEngine';
import * as sample1 from './sample1';
import {createTestDir, withTiming} from './testutil';
import {assert} from 'chai';
import SqliteDatabase from 'better-sqlite3';
import * as colors from 'ansi-colors';

interface Stat { first?: number, max?: number, last?: number }

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
 * In this test, we do many fetches in parallel, and measure time and memory consumption. In
 * particular, we compare full fetches to streaming fetches to ensure that the latter provide a
 * way to minimize memory consumption.
 */
describe('Test2', function() {
  this.timeout(60000);

  let testDir: string;
  before(async function() {
    testDir = await createTestDir('Test2');
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

  async function readDBFull(dbPath: string, memory: Memory, limit?: number) {
    const db2: SqliteDatabase.Database = SqliteDatabase(dbPath, {});
    const dataEngine2 = new DataEngine(db2);
    const result = await dataEngine2.fetchQuery({tableId: 'Table1', sort: ['id'], limit});
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
  }

  async function readDBStreaming(dbPath: string, memory: Memory, limit?: number) {
    const db2: SqliteDatabase.Database = SqliteDatabase(dbPath, {});
    const dataEngine2 = new DataEngine(db2);
    const result = await dataEngine2.fetchQueryStreaming({tableId: 'Table1', sort: ['id'], limit}, {
      timeoutMs: 60_000,
      chunkRows: 500,
    });
    let count = 0;
    let sumRowIds = 0;
    for await (const chunk of result.chunks) {
      // console.warn("readDB at", count);
      // Return to event loop after every chunk of rows.
      await new Promise(r => setTimeout(r, 0));
      memory.note();
      for (const row of chunk) {
        ++count;
        sumRowIds += row[0] as number;
      }
    }
    return {count, sumRowIds};
  }

  for (const [index, readDB] of [readDBStreaming, readDBFull].entries()) {
    describe(`with ${readDB.name} ${index}`, function() {

      it('parallel reads', async function() {
        const dbPath = `${testDir}/parallel_reads${index}.grist`;
        await setUpDB(dbPath);

        const NReps = 25;
        const NRows = 100_000;

        await withTiming(`single fetch of ${NRows} rows`, async() => {
          const memory = new Memory();
          const result = await readDB(dbPath, memory, NRows);
          console.warn(`maxDelta: rss ${memory.maxDeltaRss()} MB, heap ${memory.maxDeltaHeap()} MB`);
          assert.equal(result.count, NRows);
          assert.equal(result.sumRowIds, NRows * (NRows + 1) / 2);
        });

        await withTiming(`parallel fetches: ${NReps} of ${NRows} rows`, async () => {
          // Start off with some garbage-collecting, to reduce variability of results, hopefully.
          gc?.();

          const memory = new Memory();
          memory.note();
          const results = await Promise.all(Array.from(Array(NReps), () => readDB(dbPath, memory, NRows)));
          assert.lengthOf(results, NReps);
          console.warn(`maxDelta: rss ${memory.maxDeltaRss()} MB, heap ${memory.maxDeltaHeap()} MB`);
          console.warn(`- per query: rss ${memory.maxDeltaRss() / NReps} MB, ` +
            colors.blue(`heap ${memory.maxDeltaHeap() / NReps} MB`));
          for (const result of results) {
            assert.equal(result.count, NRows);
            assert.equal(result.sumRowIds, NRows * (NRows + 1) / 2);
          }
        });
      });
    });
  }
});
