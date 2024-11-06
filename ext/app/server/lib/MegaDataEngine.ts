import {CellValue} from 'app/common/DocActions';
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
import {appSettings} from 'app/server/lib/AppSettings';
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
  public static maybeCreate(dbPath: string, engine?: string): MegaDataEngine|null {
    const isEnabled = enableMegaDataEngine && engine === MEGA_ENGINE;
    return isEnabled ? new MegaDataEngine(dbPath) : null;
  }

  private _dataEngine: DataEnginePooled;

  constructor(dbPath: string) {
    this._dataEngine = new UnmarshallingDataEngine(dbPath, {verbose: console.log});
    // db.exec("PRAGMA journal_mode=WAL");  <-- TODO we want this, but leaving for later.
  }

  public serve(socket: MinimalWebSocket): void {
    const channel = new WebSocketChannel(socket);
    createDataEngineServer(this._dataEngine, {channel, verbose: console.log});
  }
}

class UnmarshallingDataEngine extends DataEnginePooled {
  public async fetchQueryStreaming(
    query: Query, options: QueryStreamingOptions, abortSignal?: AbortSignal
  ): Promise<QueryResultStreaming> {

    // Get the grist types of columns, which determine how to decode values.
    // TODO This is poor. If we do two queries, they need to be in a transaction, to ensure no
    // changes to DB can happen in between. And also column types is something Grist already knows
    // how to maintain in memory; seems silly to query (but fine, save for the transaction point).
    let columnTypes: Map<string, string>;
    this.withDB((db) => db.transaction(() => {
      const stmt = db.prepare('SELECT colId, type'
        + ' FROM _grist_Tables_column c LEFT JOIN _grist_Tables t ON c.parentID = t.id'
        + ' WHERE t.tableId=?');
      columnTypes = new Map(stmt.raw().all(query.tableId) as Array<[string, string]>);
    })());

    const queryResult = await super.fetchQueryStreaming(query, options, abortSignal);
    const decoders = queryResult.value.colIds.map(c => getDecoder(columnTypes.get(c)));

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
