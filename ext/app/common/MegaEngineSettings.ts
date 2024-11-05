import {EngineCode} from 'app/common/DocumentSettings';

// Force-cast because this value hasn't been added to the enum;
export const MEGA_ENGINE: EngineCode = 'mega:python3' as EngineCode;

export function isMegaEngineEnabled(engine: string|undefined, supportedEngines: EngineCode[]|undefined): boolean {
  return engine === MEGA_ENGINE && (supportedEngines || []).includes(MEGA_ENGINE);
}
