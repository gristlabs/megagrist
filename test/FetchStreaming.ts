import {DataEngine} from '../lib/DataEngine';
import {QueryStreamingOptions} from '../lib/IDataEngine';
import * as sample1 from './sample1';
import {createTestDir} from './testutil';
import {assert} from 'chai';
import SqliteDatabase from 'better-sqlite3';

describe('FetchStreaming', function() {
  this.timeout(60000);

  const verbose = process.env.VERBOSE ? console.log : undefined;

  let testDbPath: string;
  let testDir: string;
  before(async function() {
    testDir = await createTestDir('FetchStreaming');
    testDbPath = `${testDir}/FetchStreaming.grist`;
    await setUpDB(testDbPath);
  });

  async function setUpDB(dbPath: string) {
    const db: SqliteDatabase.Database = SqliteDatabase(dbPath, {verbose});
    db.exec("PRAGMA journal_mode=WAL");
    const dataEngine = new DataEngine(db);

    // Run actions to create a table.
    await sample1.createTable(dataEngine, 'Table1');
    await sample1.populateTable(dataEngine, 'Table1', 100, 1000);
    return dataEngine;
  }

  it('should fail if a single db is used for overlapping reads', async function() {
    const db: SqliteDatabase.Database = SqliteDatabase(testDbPath, {verbose});
    const dataEngine = new DataEngine(db);
    const options: QueryStreamingOptions = {
      timeoutMs: 60_000,
      chunkRows: 500,
    };
    const result1 = await dataEngine.fetchQueryStreaming({tableId: 'Table1', sort: ['id']}, options);
    assert.equal(result1.value.tableId, 'Table1');
    assert.deepEqual(result1.value.colIds, ['id', 'Name', 'Email', 'MyDate', 'Age']);

    await assert.isRejected(dataEngine.fetchQueryStreaming({tableId: 'Table1', sort: ['id']}, options),
      /cannot start a transaction within a transaction/);

    // Try reading a chunk, to start the streaming portion.
    const chunk = await result1.chunks[Symbol.asyncIterator]().next();
    assert.lengthOf(chunk.value, 500);

    // Another parallel query should also be rejected, though the error happens to be different.
    await assert.isRejected(dataEngine.fetchQueryStreaming({tableId: 'Table1', sort: ['id']}, options),
      /This database connection is busy executing a query/);
  });

  it('should pass if an overlapping read aborts the previous one first', async function() {
    const db: SqliteDatabase.Database = SqliteDatabase(testDbPath, {verbose});
    const dataEngine = new DataEngine(db);
    const options: QueryStreamingOptions = {
      timeoutMs: 60_000,
      chunkRows: 500,
    };
    const abortController1 = new AbortController();
    const result1 = await dataEngine.fetchQueryStreaming({tableId: 'Table1', sort: ['id']},
      {...options, abortSignal: abortController1.signal});
    assert.equal(result1.value.tableId, 'Table1');
    assert.deepEqual(result1.value.colIds, ['id', 'Name', 'Email', 'MyDate', 'Age']);

    abortController1.abort();
    const abortController2 = new AbortController();
    const result2 = await dataEngine.fetchQueryStreaming({tableId: 'Table1', sort: ['id']},
      {...options, abortSignal: abortController2.signal});
    assert.equal(result2.value.tableId, 'Table1');
    assert.deepEqual(result2.value.colIds, ['id', 'Name', 'Email', 'MyDate', 'Age']);

    // Try reading a chunk, to start the streaming portion.
    const chunk = await result2.chunks[Symbol.asyncIterator]().next();
    assert.lengthOf(chunk.value, 500);

    // Aborting at this stage should also work, so the next query can start right away.
    abortController2.abort();
    const abortController3 = new AbortController();
    const result3 = await dataEngine.fetchQueryStreaming({tableId: 'Table1', sort: ['id']},
      {...options, abortSignal: abortController3.signal});
    assert.equal(result3.value.tableId, 'Table1');
    assert.deepEqual(result3.value.colIds, ['id', 'Name', 'Email', 'MyDate', 'Age']);
  });
});
