import {ProcessedActionBundle} from 'app/common/AlternateActions';
import {CellValue, UserAction} from 'app/common/DocActions';
import {AddRecord, UpdateRecord, RemoveRecord} from 'app/common/DocActions';
import {DocData} from 'app/common/DocData';
import {EngineCode} from 'app/common/DocumentSettings';
import {isListType} from 'app/common/gristTypes';
import * as marshal from 'app/common/marshal';
import {MEGA_ENGINE} from 'app/common/MegaEngineSettings';
import {MinimalWebSocket} from 'app/common/MinimalWebSocket';
import {DocAction, isDataDocAction} from 'app/megagrist/lib/DocActions';
import {WebSocketChannel} from 'app/megagrist/lib/WebSocketChannel';
import {DataEngineCallContext, DataEnginePooled} from 'app/megagrist/lib/DataEngine';
import {createDataEngineServer} from 'app/megagrist/lib/DataEngineServer';
import {QueryStreamingOptions} from 'app/megagrist/lib/IDataEngine';
import {Query, QueryResultStreaming} from 'app/megagrist/lib/types';
import {ExpandedQuery} from 'app/megagrist/lib/sqlConstruct';
import {appSettings} from 'app/server/lib/AppSettings';
import {expandQuery} from 'app/server/lib/ExpandedQuery';
import * as ICreate from 'app/server/lib/ICreate';
import {OptDocSession} from 'app/server/lib/DocSession';
import * as log from 'app/server/lib/log';


const enableMegaDataEngine = appSettings.section('dataEngine').flag('enableMega').readBool({
  envVar: 'GRIST_DATA_ENGINE_ENABLE_MEGA',
  defaultValue: true,
});

export function getSupportedEngineChoices(): EngineCode[]|undefined {
  const base = ICreate.getSupportedEngineChoices();
  if (!enableMegaDataEngine) {
    return base;
  }
  return [...base || ['python3'], MEGA_ENGINE];
}

function logDebug(x: unknown, ...args: unknown[]) {
  log.debug(String(x), ...args);
}

export class MegaDataEngine {
  public static maybeCreate(dbPath: string, docData: DocData): MegaDataEngine|null {
    const engine = docData.docSettings().engine;
    const isEnabled = enableMegaDataEngine && engine === MEGA_ENGINE;
    return isEnabled ? new MegaDataEngine(dbPath, docData) : null;
  }

  public static maybeMakeChannel(socket: MinimalWebSocket): WebSocketChannel|null {
    return new WebSocketChannel(socket);
  }

  private _dataEngine: DataEnginePooled;

  constructor(dbPath: string, docData: DocData) {
    this._dataEngine = new UnmarshallingDataEngine(docData, dbPath, {verbose: logDebug});
    // db.exec("PRAGMA journal_mode=WAL");  <-- TODO we want this, but leaving for later.
  }

  public async applyUserActions(
    docSession: OptDocSession|null, userActions: UserAction[]
  ): Promise<ProcessedActionBundle> {
    const actions: DocAction[] = userActions.map(a => transformUserAction(a));
    const context: DataEngineCallContext = {channel: docSession?.client?.getAuxChannel()};
    const {results} = await this._dataEngine.applyActions(context, {actions});
    return {stored: actions, undo: [], retValues: results};
  }

  public serve(channel: WebSocketChannel): void {
    createDataEngineServer(this._dataEngine, {channel, verbose: logDebug});
  }
}

export namespace MegaDataEngine {
  export type Channel = WebSocketChannel;
}

class UnmarshallingDataEngine extends DataEnginePooled {
  constructor(private _docData: DocData, ...args: ConstructorParameters<typeof DataEnginePooled>) {
    super(...args);
  }

  public async fetchQueryStreaming(
    context: DataEngineCallContext, query: Query, options: QueryStreamingOptions
  ): Promise<QueryResultStreaming> {

    // TODO ugh, this is only for fetchQueryStreaming??? What about fetchQuery? We should leave
    // only one method in the interface that's the best of both.

    // We use the metadata about the table to get the grist types of columns, which determine how
    // to decode values.
    const tableData = this._docData.getTable(query.tableId)

    let expandedQuery: ExpandedQuery;
    if (query.columns) {
      expandedQuery = query;
    } else {
      const expanded = expandQuery({tableId: query.tableId, filters: {}}, this._docData, true);
      expandedQuery = {...query, joins: expanded.joins, selects: expanded.selects};
    }

    const queryResult = await super.fetchQueryStreaming(context, expandedQuery, options);
    const decoders = queryResult.value.colIds.map(c => getDecoder(tableData?.getColType(c)));

    async function *generateRows() {
      for await (const chunk of queryResult.chunks) {
        yield chunk.map(row => row.map((cell, i) => decoders[i](cell as CellValue)));
      }
    }
    return {
      value: queryResult.value,
      chunks: generateRows()
    };
  }
}

type DecoderFunc = (val: CellValue) => CellValue;

const baseDecoder = (val: CellValue) => (Buffer.isBuffer(val) ? marshal.loads(val) : val);
function getDecoder(gristType?: string): DecoderFunc {
  if (gristType === 'Bool') {
    return (val: CellValue) => {
      val = baseDecoder(val);
      return (val === 0 || val === 1) ? Boolean(val) : val;
    };
  } else if (gristType && isListType(gristType)) {
    return (val: CellValue) => {
      val = baseDecoder(val);
      if (typeof val === 'string' && val.startsWith('[')) {
        try {
          return ['L', ...JSON.parse(val)] as CellValue;
        } catch (e) {
          // Fall through without parsing
        }
      }
      return val;
    };
  }
  return baseDecoder;
}


/**
 * We currently only accept UserActions that happen to be DocActions, only data ones (not schema
 * ones), and transform single-record actions to bulk actions, so that code that deals with data
 * actions doesn't need to bother with multiple variations.
 */
function transformUserAction(userAction: UserAction): DocAction {
  const actionName = userAction[0];
  const method = _transformUserAction[actionName as keyof typeof _transformUserAction];
  if (typeof method === 'function') {
    return method(userAction as any);
  } else if (isDataDocAction(userAction as DocAction)) {
    return userAction as DocAction;
  }
  throw new Error(`UserAction unsupported: ${actionName}`);
}

const _transformUserAction = {
  AddRecord([_, tableId, rowId, colValues]: AddRecord): DocAction.BulkAddRecord {
    return ['BulkAddRecord', tableId, [rowId],
      Object.fromEntries(Object.entries(colValues).map(([k, v]) => [k, [v]]))];
  },
  UpdateRecord([_, tableId, rowId, colValues]: UpdateRecord): DocAction.BulkUpdateRecord {
    return ['BulkUpdateRecord', tableId, [rowId],
      Object.fromEntries(Object.entries(colValues).map(([k, v]) => [k, [v]]))];
  },
  RemoveRecord([_, tableId, rowId]: RemoveRecord): DocAction.BulkRemoveRecord {
    return ['BulkRemoveRecord', tableId, [rowId]];
  },
};
