import {DataEngine} from '../lib/DataEngine';
import {QueryFilters, QueryResult} from '../lib/types';
import * as sample1 from './sample1';
import {createTestDir, withTiming} from './testutil';
import {assert} from 'chai';
import SqliteDatabase from 'better-sqlite3';

/**
 * In this test, we do many fetches in parallel, and measure time, or (when done slowly) memory
 * consumption.
 */
describe('Test2', function() {
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
    await sample1.createTable(dataEngine, 'Table1');
    await sample1.populateTable(dataEngine, 'Table1', 1000, 1000);

    // TODO: The idea here is to
    // (1) call fetchQuery N times in parallel, fetching a lot, and somehow measure memory use. In
    // this synchronous implementation, we should just see memory of O(N * size of data).
    // (2) add streaming implementation of fetchQuery, whatever that means exactly, and do
    // the same thing. We should see memory of just O(N).
    const db2: SqliteDatabase.Database = SqliteDatabase(`${testDir}/scenario1.grist`, {
      verbose: process.env.VERBOSE ? console.log : undefined
    });
    const dataEngine2 = new DataEngine(db2);
    const filters: QueryFilters = ['GtE', ['Name', 'Age'], ['Const', 99]];
    const result: QueryResult = await withTiming('fetchQuery', () =>
      dataEngine2.fetchQuery({tableId: 'Table1', filters}));
    assert.lengthOf(result.tableData.id, 10000);
    assert.isTrue(result.tableData.Age.every(age => (age === 99)));
    throw new Error("This test isn't done yet");
  });
});
