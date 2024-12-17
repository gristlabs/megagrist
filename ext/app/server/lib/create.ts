import {ICreate} from "app/server/lib/ICreate";
import {makeCoreCreator} from "app/server/lib/coreCreator";
import {getSupportedEngineChoices} from 'app/server/lib/MegaDataEngine';
import * as fs from 'fs';

export const create: ICreate = makeCoreCreator({
  getSupportedEngineChoices,
  sandboxFlavor: (fs.existsSync('/usr/bin/sandbox-exec') ? 'macSandboxExec' : 'gvisor'),
});

/**
 * Fetch the ICreate object for grist-core.
 * Placeholder to enable eventual refactoring away from a global singleton constant.
 * Needs to exist in all repositories before core can be switched!
 */
export function getCreator(): ICreate {
  return create;
}
