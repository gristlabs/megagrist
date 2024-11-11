import {CellValue} from 'app/common/DocActions';
import {DocData} from 'app/common/DocData';
import {EngineCode} from 'app/common/DocumentSettings';
import {isListType} from 'app/common/gristTypes';
import * as marshal from 'app/common/marshal';
import {MEGA_ENGINE} from 'app/common/MegaEngineSettings';
import {MinimalWebSocket} from 'app/common/MinimalWebSocket';
import {WebSocketChannel} from 'app/megagrist/lib/WebSocketChannel';
import {DataEnginePooled} from 'app/megagrist/lib/DataEngine';
import {createDataEngineServer} from 'app/megagrist/lib/DataEngineServer';
import {QueryStreamingOptions} from 'app/megagrist/lib/IDataEngine';
import {Query, QueryResultStreaming} from 'app/megagrist/lib/types';
import {ExpandedQuery} from 'app/megagrist/lib/sqlConstruct';
import {appSettings} from 'app/server/lib/AppSettings';
import {expandQuery} from 'app/server/lib/ExpandedQuery';
import * as ICreate from 'app/server/lib/ICreate';


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

export class MegaDataEngine {
  public static maybeCreate(dbPath: string, docData: DocData): MegaDataEngine|null {
    const engine = docData.docSettings().engine;
    const isEnabled = enableMegaDataEngine && engine === MEGA_ENGINE;
    return isEnabled ? new MegaDataEngine(dbPath, docData) : null;
  }

  private _dataEngine: DataEnginePooled;

  constructor(dbPath: string, docData: DocData) {
    this._dataEngine = new UnmarshallingDataEngine(docData, dbPath, {verbose: console.log});
    // db.exec("PRAGMA journal_mode=WAL");  <-- TODO we want this, but leaving for later.
  }

  public serve(socket: MinimalWebSocket): void {
    const channel = new WebSocketChannel(socket);
    createDataEngineServer(this._dataEngine, {channel, verbose: console.log});
  }
}

class UnmarshallingDataEngine extends DataEnginePooled {
  constructor(private _docData: DocData, ...args: ConstructorParameters<typeof DataEnginePooled>) {
    super(...args);
  }

  public async fetchQueryStreaming(
    query: Query, options: QueryStreamingOptions, abortSignal?: AbortSignal
  ): Promise<QueryResultStreaming> {

    // We use the metadata about the table to get the grist types of columns, which determine how
    // to decode values.
    const tableData = this._docData.getTable(query.tableId)

    const expanded = expandQuery({tableId: query.tableId, filters: {}}, this._docData, true);
    const expandedQuery: ExpandedQuery = {...query, joins: expanded.joins, selects: expanded.selects};

    const queryResult = await super.fetchQueryStreaming(expandedQuery, options, abortSignal);
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
