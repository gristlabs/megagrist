import {DataEngine} from 'ext/app/megagrist/lib/DataEngine';
import {Deps as StoreDocActionDeps} from 'ext/app/megagrist/lib/StoreDocAction';
import {QueryCursor, QueryFilters, QueryResult} from 'ext/app/megagrist/lib/types';
import * as sample1 from './sample1';
import {changePropertyForTest, createTestDir, withTiming} from './testutil';
import {assert} from 'chai';
import SqliteDatabase from 'better-sqlite3';

describe('Test1', function() {
  this.timeout(60000);

  let testDir: string;
  before(async function() {
    testDir = await createTestDir('Test1');
  });

  for (const virtualTables of [false, true]) {
    describe(`with USE_VIRTUAL_TABLES=${virtualTables}`, function() {
      changePropertyForTest(StoreDocActionDeps, 'USE_VIRTUAL_TABLES', virtualTables);

      it('scenario 1', async function() {
        // Create database in a tmp directory.
        const dbPath = `${testDir}/scenario1-${virtualTables}.grist`;
        const db: SqliteDatabase.Database = SqliteDatabase(dbPath, {
          verbose: process.env.VERBOSE ? console.log : undefined
        });
        db.exec("PRAGMA journal_mode=WAL");
        const dataEngine = new DataEngine(db);

        // Run actions to create a table.
        await withTiming("create table", () =>
          sample1.createTable(dataEngine, 'Table1'));

        // Run actions to create 1m rows in this table.
        await withTiming("populate table", () =>
          sample1.populateTable(dataEngine, 'Table1', 1000, 1000));

        const db2: SqliteDatabase.Database = SqliteDatabase(dbPath, {
          verbose: process.env.VERBOSE ? console.log : undefined
        });
        const dataEngine2 = new DataEngine(db2);
        await runQueries("same connection", dataEngine);
        await runQueries("new connection", dataEngine2);
      });
    });
  }

  async function runQueries(desc: string, dataEngine: DataEngine) {
    // Run query to read this table.
    const result2 = await withTiming(`${desc}: query table 10k`, () => {
      const filters: QueryFilters = ['GtE', ['Name', 'Age'], ['Const', 99]];
      return dataEngine.fetchQuery({}, {tableId: 'Table1', filters});
    });
    assert.lengthOf(result2.tableData.id, 10000);
    assert.isTrue(result2.tableData.Age.every(age => (age === 99)));

    // Run queries to scan through the entire table.
    await withTiming(`${desc}: scan table`, async () => {
      let cursor: QueryCursor|undefined;
      let prevResult: QueryResult|undefined;
      while (true) {    // eslint-disable-line no-constant-condition
        const result = await dataEngine.fetchQuery({}, {tableId: 'Table1', sort: ['id'], cursor, limit: 1000});
        if (!result.tableData.id.length) {
          break;
        }
        assert.equal(result.tableData.id[0], prevResult ? last(prevResult.tableData.id as number[]) + 1 : 1);
        prevResult = result;
        cursor = ["after", [last(result.tableData.id)]];
      }
    });
  }
});

function last<T>(arr: T[]): T { return arr[arr.length - 1]; }
