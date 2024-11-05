import {DocAction} from 'ext/app/megagrist/lib/DocActions';
import {IDataEngine} from 'ext/app/megagrist/lib/IDataEngine';

export function createTable(dataEngine: IDataEngine, tableId: string) {
  // Run actions to create a table.
  return dataEngine.applyActions({actions: [
    ['AddTable', 'Table1', [
      {id: 'Name', type: 'Text'},
      {id: 'Email', type: 'Text'},
      {id: 'MyDate', type: 'Date'},
      {id: 'Age', type: 'Numeric'},
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
        Name: Array.from(array, (x, i) => `Bob #${offset + i}`),
        Email: Array.from(array, (x, i) => `bob${offset + i}@example.com`),
        MyDate: Array.from(array, (x, i) => 1000000000 + (offset + i) * 86400),
        Age: Array.from(array, (x, i) => Math.floor(i / 10)),
      }
    ];
    await dataEngine.applyActions({actions: [addAction]});
  }
}
