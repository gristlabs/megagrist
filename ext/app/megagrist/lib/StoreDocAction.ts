import {AddRecord, UpdateRecord, RemoveRecord} from 'app/common/DocActions';
import {BulkColValues, DocAction, isDataDocActionName} from './DocActions';
import {isSchemaAction} from './DocActions';
import {getSqlTypeInfo, quoteIdent} from './sqlUtil';
import SqliteDatabase from 'better-sqlite3';

export const Deps = {
  // TODO: this is for testing and timing, to control whether to try the "virtual tables"
  // implementation of bulk actions.
  USE_VIRTUAL_TABLES: false,

  // The name of the column containing rowIds. Grist normally always uses "id". We treat it as
  // configurable to make it easier to support tables with a different column for rowId.
  ROW_ID_COL: 'id',
};

export class StoreDocAction {
  constructor(private _db: SqliteDatabase.Database) {}

  public store(action: DocAction) {
    if (isSchemaAction(action)) {
      return this[action[0]](action as any);
    } else if (isDataDocActionName(action[0])) {
      return this[action[0]](action as any);
    }
    throw new Error(`Unsupported action type: ${action[0]}`);
  }

  // TODO For all data operations, including UPDATE, there are ways to deal with multiple rows
  // in one statement. With batcing, could run many times fewer statements. Question is whether
  // it's actually faster. For example:
  //    WITH u(id, col1, col2) AS (VALUES (?, ?, ?), (?, ?, ?), ...)
  //    UPDATE tableId AS t SET (t.col1, t.col2) = (u.col1, u.col2)
  //    FROM u WHERE t.id = u.id;
  //
  // Experiment with batching INSERTs (to max of 900 params) showed a performance improvement of
  // about 20%, which is nice but may not be worth the added complexity.
  //
  // A neat alternative is virtual table, made easy by better-sqlite3's table() function. This
  // allows a single statement for any number of rows, without batching! Includes other overhead
  // though, so performance is TBD.

  public AddRecord([_, tableId, rowId, colValues]: AddRecord) {
    return this.BulkAddRecord(['BulkAddRecord', tableId, [rowId],
      Object.fromEntries(Object.entries(colValues).map(([k, v]) => [k, [v]]))]);
  }
  public UpdateRecord([_, tableId, rowId, colValues]: UpdateRecord) {
    return this.BulkUpdateRecord(['BulkUpdateRecord', tableId, [rowId],
      Object.fromEntries(Object.entries(colValues).map(([k, v]) => [k, [v]]))]);
  }
  public RemoveRecord([_, tableId, rowId]: RemoveRecord) {
    return this.BulkRemoveRecord(['BulkRemoveRecord', tableId, [rowId]]);
  }

  public BulkAddRecord([_, tableId, rowIds, colValues]: DocAction.BulkAddRecord) {
    if (rowIds.length === 0) { return; }
    const cols = [Deps.ROW_ID_COL, ...Object.keys(colValues)].map(quoteIdent);
    if (Deps.USE_VIRTUAL_TABLES) {
      // The virtual table created this way is only visible in the connection where it's created.
      // better-sqlite3 doesn't provide a way to drop it (SQLite does, by passing a null module to
      // sqlite3_create_module_v2). It should be safe to reuse the virtual table name.
      this.makeVirtualTableForAction('tmp_bulk_action', rowIds, colValues);
      this._db.prepare(`INSERT INTO ${quoteIdent(tableId)} (${cols.join(', ')}) SELECT * FROM tmp_bulk_action`).run();
    } else {
      const placeholders = cols.map(c => '?');
      const stmt = this._db.prepare(`INSERT INTO ${quoteIdent(tableId)} (${cols.join(', ')}) VALUES (${placeholders})`);
      const values = Object.values(colValues);
      for (let i = 0; i < rowIds.length; i++) {
        stmt.run(rowIds[i], values.map(col => col[i]));
      }
    }
  }

  public BulkUpdateRecord([_, tableId, rowIds, colValues]: DocAction.BulkUpdateRecord) {
    const cols = Object.keys(colValues);
    if (rowIds.length === 0 || cols.length === 0) { return; }

    const colListSql = cols.map(c => `${quoteIdent(c)} = ?`);
    const values = Object.values(colValues);
    const stmt = this._db.prepare(
      `UPDATE ${quoteIdent(tableId)} SET ${colListSql} WHERE ${quoteIdent(Deps.ROW_ID_COL)}=?`);
    for (let i = 0; i < rowIds.length; i++) {
      stmt.run(values.map(col => col[i]), rowIds[i]);
    }
  }

  public BulkRemoveRecord([_, tableId, rowIds]: DocAction.BulkRemoveRecord) {
    if (rowIds.length === 0) { return; }
    const stmt = this._db.prepare(
      `DELETE FROM ${quoteIdent(tableId)} WHERE ${quoteIdent(Deps.ROW_ID_COL)} = ?`);
    for (let i = 0; i < rowIds.length; i++) {
      stmt.run(rowIds[i]);
    }
  }
  public ReplaceTableData(action: DocAction.ReplaceTableData) {
    throw new Error('Not yet implemented');
  }
  public AddColumn(action: DocAction.AddColumn) {
    throw new Error('Not yet implemented');
  }
  public RemoveColumn(action: DocAction.RemoveColumn) {
    throw new Error('Not yet implemented');
  }
  public RenameColumn(action: DocAction.RenameColumn) {
    throw new Error('Not yet implemented');
  }
  public ModifyColumn(action: DocAction.ModifyColumn) {
    throw new Error('Not yet implemented');
  }
  public AddTable([_, tableId, cols]: DocAction.AddTable) {
    const colSpecSql = cols.map(col =>
      `${quoteIdent(col.id)} ${getSqlTypeInfo(col.type).sqlType} DEFAULT ${getSqlTypeInfo(col.type).sqlDefault}`);

    // Every table needs an "id" column, and it should be an "integer primary key" type so that it
    // serves as the alias for the SQLite built-in "rowid" column. See
    // https://www.sqlite.org/lang_createtable.html#rowid for details.
    colSpecSql.unshift(`${quoteIdent(Deps.ROW_ID_COL)} INTEGER PRIMARY KEY`);

    const stmt = this._db.prepare(`CREATE TABLE ${quoteIdent(tableId)} (${colSpecSql.join(', ')})`);
    stmt.run();
  }
  public RemoveTable(action: DocAction.RemoveTable) {
    throw new Error('Not yet implemented');
  }
  public RenameTable(action: DocAction.RenameTable) {
    throw new Error('Not yet implemented');
  }

  public makeVirtualTableForAction(tableName: string, rowIds: number[], colValues?: BulkColValues) {
    const cols = [Deps.ROW_ID_COL, ...(colValues ? Object.keys(colValues) : [])];
    const values = [rowIds, ...(colValues ? Object.values(colValues) : [])];
    this._db.table(tableName, {
      columns: cols,
      rows: function*() {
        for (let i = 0; i < rowIds.length; i++) {
          yield values.map(col => col[i]);
        }
      },
    });
  }
}
