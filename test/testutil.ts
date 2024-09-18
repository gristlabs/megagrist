import fs from 'fs/promises';
import path from 'path';

// Having a file that imports mocha allows using describe() and it() globals in tests. It is an
// alternative to including `"types": ["mocha"]` into tsconfig.json.
import 'mocha';

// Enable source-map-support for stack traces.
import 'source-map-support/register';

export async function createTestDir(suiteName: string): Promise<string> {
  const tmpRootDir = process.env.TESTDIR || "./_testoutputs";
  const testDir = path.join(tmpRootDir, suiteName);
  // Remove any previous tmp dir, and create the new one.
  await fs.rm(testDir, {force: true, recursive: true});
  await fs.mkdir(testDir, {recursive: true});
  console.warn(`Test logs and data are at: ${testDir}/`);
  return testDir;
}
