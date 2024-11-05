/**
 * TODO
 * This is an unfinished experiment with seeing how fast we can get random ranges of rows in a
 * sorted order.
 */

import {DocAction} from 'ext/app/megagrist/lib/DocActions';
import {IDataEngine} from 'ext/app/megagrist/lib/IDataEngine';
import {DataEngine} from 'ext/app/megagrist/lib/DataEngine';
import {QueryFilters} from 'ext/app/megagrist/lib/types';
import {createTestDir, withTiming} from './testutil';
import SqliteDatabase from 'better-sqlite3';
import {assert} from 'chai';

const timestampStart = new Date('2020-01-01').getTime() / 1000;
const timestampEnd = new Date('2030-01-01').getTime() / 1000;
function randomTimestamp() {
  return timestampStart + Math.floor(Math.random() * (timestampEnd - timestampStart));
}

namespace sample2 {
  export function createTable(dataEngine: IDataEngine, tableId: string) {
    // Run actions to create a table.
    return dataEngine.applyActions({actions: [
      ['AddTable', 'Table1', [
        {id: 'SomeName', type: 'Text'},
        {id: 'Email', type: 'Text'},
        {id: 'RandomTime', type: 'DateTime'},
      ]]
    ]});
  }

  export async function populateTable(dataEngine: IDataEngine, tableId: string, numChunks: number, chunkSize: number) {
    // Run actions to create numChunks * chunkSize rows in our table.
    for (let chunk = 0; chunk < numChunks; chunk++) {
      const array = Array(chunkSize);
      const offset = chunk * array.length;
      const addAction: DocAction = ['BulkAddRecord',
        'Table1',
        Array.from(array, (x, i) => offset + i + 1), {
          SomeName: Array.from(array, (x, i) => `Bob #${offset + i}`),
          Email: Array.from(array, (x, i) => `bob${offset + i}@example.com`),
          RandomTime: Array.from(array, (x, i) => randomTimestamp()),
        }
      ];
      await dataEngine.applyActions({actions: [addAction]});
    }
  }
}

describe('TestRank', function() {
  this.timeout(60000);

  let testDir: string;
  before(async function() {
    testDir = await createTestDir('TestRank');
  });

  it('scenario 1', async function() {
    // Create database in a tmp directory.
    const dbPath = `${testDir}/scenario1.grist`;
    const db: SqliteDatabase.Database = SqliteDatabase(dbPath, {
      verbose: process.env.VERBOSE ? console.log : undefined
    });
    db.exec("PRAGMA journal_mode=WAL");
    const dataEngine = new DataEngine(db);

    // Run actions to create a table, and add a lot of rows to it.
    await withTiming("create and populate table", async () => {
      await sample2.createTable(dataEngine, 'Table1');
      await sample2.populateTable(dataEngine, 'Table1', 1000, 1000);
    });

    // Run query to read this table.
    for (const withIndex of [false, true]) {
      const N = withIndex ? 100 : 10;
      const seenTimestamps: number[] = [];
      const ranks: number[] = [];
      if (withIndex) {
        db.exec('CREATE INDEX index_random_time ON Table1(RandomTime)');
      }
      try {
        await withTiming(`get ${N} random rows with index=${withIndex}`, async () => {
          for (let i = 0; i < N; i++) {
            const timestamp = randomTimestamp();
            const filters: QueryFilters = ['GtE', ['Name', 'RandomTime'], ['Const', timestamp]];
            const row = await dataEngine.fetchQuery({tableId: 'Table1', filters, sort: ['RandomTime'], limit: 1});
            assert.isAtLeast(row.tableData.RandomTime[0] as number, timestamp);
            seenTimestamps.push(row.tableData.RandomTime[0] as number);
            console.log(db.prepare('SELECT COUNT(id) FROM Table1 WHERE RandomTime < ?').get(timestamp));
          }
        });
        const middle = (timestampEnd + timestampStart) / 2;
        const countAboveMiddle = seenTimestamps.filter(t => t >= middle).length;
        assert.isAbove(countAboveMiddle, N / 4);
        assert.isBelow(countAboveMiddle, 3 * N / 4);
        console.warn("Ranks", ranks);
      } finally {
        if (withIndex) {
          db.exec('DROP INDEX index_random_time');
        }
      }
    }
  });
});
