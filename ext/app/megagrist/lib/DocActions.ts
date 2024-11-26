/**
 * This is based on Grist's app/common/DocActions and app/plugin/GristData.
 */
export * from 'app/common/DocActions';

import * as common from 'app/common/DocActions';

// Reduced version of Grist's current DocActions, omits single-record data actions.
export namespace DocAction {
  export type BulkAddRecord = common.BulkAddRecord;
  export type BulkRemoveRecord = common.BulkRemoveRecord;
  export type BulkUpdateRecord = common.BulkUpdateRecord;
  export type ReplaceTableData = common.ReplaceTableData;
  export type AddColumn = common.AddColumn;
  export type RemoveColumn = common.RemoveColumn;
  export type RenameColumn = common.RenameColumn;
  export type ModifyColumn = common.ModifyColumn;
  export type AddTable = common.AddTable;
  export type RemoveTable = common.RemoveTable;
  export type RenameTable = common.RenameTable;
}

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

export type DocAction = common.DocAction;

export function isDataDocActionName(actionName: string|unknown): actionName is DataDocAction[0] {
  switch (actionName) {
    case 'BulkAddRecord':
    case 'BulkRemoveRecord':
    case 'BulkUpdateRecord':
    case 'ReplaceTableData':
      return true;
  }
  return false;
}

export function isDataDocAction(action: common.DocAction): action is DataDocAction {
  return isDataDocActionName(action[0]);
}
