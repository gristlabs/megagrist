import {DataEngine} from '../lib/DataEngine';
import {DocAction} from '../lib/DocActions';
import {QueryCursor, QueryFilters, QueryResult} from '../lib/types';
import {createTestDir} from './testutil';
import {assert} from 'chai';
import SqliteDatabase from 'better-sqlite3';

describe('Test1', function() {
  this.timeout(60000);

  let testDir: string;
  before(async function() {
    testDir = await createTestDir('Test1');
  });

  it('scenario 1', async function() {
    // Create database in a tmp directory.
    const db: SqliteDatabase.Database = SqliteDatabase(`${testDir}/scenario1.grist`, {
      verbose: process.env.VERBOSE ? console.log : undefined
    });
    db.exec("PRAGMA journal_mode=WAL");
    const dataEngine = new DataEngine(db);

    // Run actions to create a table.
    await withTiming("create table", () => {
      return dataEngine.applyActions({actions: [
        ['AddTable', 'Table1', [
          {id: 'Name', type: 'Text'},
          {id: 'Email', type: 'Text'},
          {id: 'DOB', type: 'Date'},
          {id: 'Age', type: 'Numeric'},
        ]]
      ]});
    });

    // Run actions to create 1m rows in this table.
    await withTiming("populate table", async () => {
      for (let chunk = 0; chunk < 1000; chunk++) {
        const array = Array(1000);
        const offset = chunk * array.length;
        const addAction: DocAction = ['BulkAddRecord',
          'Table1',
          Array.from(array, (x, i) => offset + i + 1), {
            Name: Array.from(array, (x, i) => `Bob #${offset + i}`),
            Email: Array.from(array, (x, i) => `bob${offset + i}@example.com`),
            DOB: Array.from(array, (x, i) => 1000000000 + (offset + i) * 86400),
            Age: Array.from(array, (x, i) => Math.floor(i / 10)),
          }
        ];
        await dataEngine.applyActions({actions: [addAction]});
      }
    });

    const db2: SqliteDatabase.Database = SqliteDatabase(`${testDir}/scenario1.grist`, {
      verbose: process.env.VERBOSE ? console.log : undefined
    });
    const dataEngine2 = new DataEngine(db2);
    await runQueries("same connection", dataEngine);
    await runQueries("new connection", dataEngine2);

  });

  async function runQueries(desc: string, dataEngine: DataEngine) {
    // Run query to read this table.
    const result2 = await withTiming(`${desc}: query table 10k`, () => {
      const filters: QueryFilters = ['GtE', ['Name', 'Age'], ['Const', 99]];
      return dataEngine.fetchQuery({tableId: 'Table1', filters});
    });
    assert.lengthOf(result2.tableData.id, 10000);
    assert.isTrue(result2.tableData.Age.every(age => (age === 99)));

    // Run queries to scan through the entire table.
    await withTiming(`${desc}: scan table`, async () => {
      let cursor: QueryCursor|undefined;
      let prevResult: QueryResult|undefined;
      while (true) {    // eslint-disable-line no-constant-condition
        const result = await dataEngine.fetchQuery({tableId: 'Table1', sort: ['id'], cursor, limit: 1000});
        if (!result.tableData.id.length) {
          break;
        }
        assert.equal(result.tableData.id[0], prevResult ? last(prevResult.tableData.id) + 1 : 1);
        prevResult = result;
        cursor = ["after", [last(result.tableData.id)]];
      }
    });
  }
});

function last<T>(arr: T[]): T { return arr[arr.length - 1]; }

async function withTiming<T>(desc: string, func: () => Promise<T>): Promise<T> {
  const start = Date.now();
  try {
    return await func();
  } finally {
    const end = Date.now();
    console.log(`${desc}: took ${end - start}ms`);
  }
}
