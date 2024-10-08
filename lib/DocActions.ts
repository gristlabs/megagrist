/**
 * This is based on Grist's app/common/DocActions and app/plugin/GristData.
 */

export type GristObjCode = string;
export type CellValue = number|string|boolean|null|[GristObjCode, ...unknown[]];

export interface BulkColValues { [colId: string]: CellValue[]; }

// Multiple records in column-oriented format, same as BulkColValues, but this one is expected to
// include the column containing rowIds (normally named "id", but don't insist on that here).
export type TableColValues = BulkColValues;

// Reduced version of Grist's current DocActions, omits single-record data actions.
export namespace DocAction {
  export type BulkAddRecord = ['BulkAddRecord', string, number[], BulkColValues];
  export type BulkRemoveRecord = ['BulkRemoveRecord', string, number[]];
  export type BulkUpdateRecord = ['BulkUpdateRecord', string, number[], BulkColValues];
  export type ReplaceTableData = ['ReplaceTableData', string, number[], BulkColValues];
  export type AddColumn = ['AddColumn', string, string, ColInfo];
  export type RemoveColumn = ['RemoveColumn', string, string];
  export type RenameColumn = ['RenameColumn', string, string, string];
  export type ModifyColumn = ['ModifyColumn', string, string, Partial<ColInfo>];
  export type AddTable = ['AddTable', string, ColInfoWithId[]];
  export type RemoveTable = ['RemoveTable', string];
  export type RenameTable = ['RenameTable', string, string];
}

export interface ColInfo { type: string; }
export interface ColInfoWithId extends ColInfo { id: string; }

export type DataDocAction = (
  DocAction.BulkAddRecord |
  DocAction.BulkRemoveRecord |
  DocAction.BulkUpdateRecord |
  DocAction.ReplaceTableData
);

export type SchemaDocAction = (
  DocAction.AddColumn |
  DocAction.RemoveColumn |
  DocAction.RenameColumn |
  DocAction.ModifyColumn |
  DocAction.AddTable |
  DocAction.RemoveTable |
  DocAction.RenameTable
);

export type DocAction = (
  DataDocAction |
  SchemaDocAction
);

export function isDataDocAction(action: DocAction): action is DataDocAction {
  switch (action[0]) {
    case 'BulkAddRecord':
    case 'BulkRemoveRecord':
    case 'BulkUpdateRecord':
    case 'ReplaceTableData':
      return true;
  }
  return false;
}
