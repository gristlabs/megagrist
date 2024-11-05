import fs from 'fs/promises';
import {tmpdir} from 'os';
import path from 'path';
import * as chai from 'chai';
import chaiAsPromised from 'chai-as-promised';

chai.use(chaiAsPromised);
chai.config.includeStack = true;    // Affects asserts like isRejected and isFulfilled.

// Having a file that imports mocha allows using describe() and it() globals in tests. It is an
// alternative to including `"types": ["mocha"]` into tsconfig.json.
import 'mocha';

// Enable source-map-support for stack traces.
import 'source-map-support/register';

/**
 * Create test directory named suiteName under process.env.TESTDIR, defaulting to tmpdir().
 * Hard-deletes any directory already there.
 */
export async function createTestDir(suiteName: string): Promise<string> {
  const tmpRootDir = process.env.TESTDIR || tmpdir();
  const testDir = path.join(tmpRootDir, suiteName);
  // Remove any previous tmp dir, and create the new one.
  await fs.rm(testDir, {force: true, recursive: true});
  await fs.mkdir(testDir, {recursive: true});
  console.warn(`Test logs and data are at: ${testDir}/`);
  return testDir;
}

/**
 * Times execution of func(). Prints out the time to console, and returns func's return value.
 */
export async function withTiming<T>(desc: string, func: () => Promise<T>): Promise<T> {
  const start = Date.now();
  try {
    return await func();
  } finally {
    const end = Date.now();
    console.log(`${desc}: took ${end - start}ms`);
  }
}

/**
 * Add before()/after() callbacks to set obj[key] to newValue before the test, and restore the
 * previous value after.
 */
export function changePropertyForTest<T, P extends keyof T>(obj: T, key: P, newValue: T[P]) {
  let previous: T[P];

  before(() => {
    previous = obj[key];
    obj[key] = newValue;
  });

  after(() => {
    obj[key] = previous;
  });
}
