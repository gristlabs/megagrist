import {GristType} from './types';
import {CellValue} from './DocActions';

/**
 * Validate and quote SQL identifiers such as table and column names.
 * We quote every identifier, as recommended, to avoid conflicts with SQLite keywords.
 */
export function quoteIdent(ident: string): string {
  if (!/^[\w.]+$/.test(ident)) {
    throw new Error(`SQL identifier is not valid: ${ident}`);
  }
  return `"${ident}"`;
}

/**
 * For each Grist type, we pick a good Sqlite SQL type name, and default value to use. Sqlite
 * columns are loosely typed, and the types named here are not all distinct in terms of
 * 'affinities', but they are helpful as comments.  Type names chosen from:
 *   https://www.sqlite.org/datatype3.html#affinity_name_examples
 */
interface SqlTypeInfo {
  sqlType: string;
  sqlDefault: string;
  gristDefault: CellValue;
}

const gristBaseTypeToSql: {[key in GristType]: SqlTypeInfo} = {
  Any:              {sqlType: 'BLOB',     sqlDefault: "NULL",   gristDefault: null    },
  Attachments:      {sqlType: 'TEXT',     sqlDefault: "NULL",   gristDefault: null    },
  Blob:             {sqlType: 'BLOB',     sqlDefault: "NULL",   gristDefault: null    },
  // Bool is only supported by SQLite as 0 and 1 values.
  Bool:             {sqlType: 'BOOLEAN',  sqlDefault: "0",      gristDefault: false   },
  Choice:           {sqlType: 'TEXT',     sqlDefault: "''",     gristDefault: ''      },
  ChoiceList:       {sqlType: 'TEXT',     sqlDefault: "NULL",   gristDefault: null    },
  Date:             {sqlType: 'DATE',     sqlDefault: "NULL",   gristDefault: null    },
  DateTime:         {sqlType: 'DATETIME', sqlDefault: "NULL",   gristDefault: null    },
  Id:               {sqlType: 'INTEGER',  sqlDefault: "0",      gristDefault: 0       },
  Int:              {sqlType: 'INTEGER',  sqlDefault: "0",      gristDefault: 0       },
  // Note that "1e999" is a way to store Infinity into SQLite.
  // See also http://sqlite.1065341.n5.nabble.com/Infinity-td55327.html.
  ManualSortPos:    {sqlType: 'NUMERIC',  sqlDefault: "1e999",  gristDefault: Number.POSITIVE_INFINITY       },
  Numeric:          {sqlType: 'NUMERIC',  sqlDefault: "0",      gristDefault: 0       },
  PositionNumber:   {sqlType: 'NUMERIC',  sqlDefault: "1e999",  gristDefault: Number.POSITIVE_INFINITY       },
  Ref:              {sqlType: 'INTEGER',  sqlDefault: "0",      gristDefault: 0       },
  RefList:          {sqlType: 'TEXT',     sqlDefault: "NULL",   gristDefault: null    },
  Text:             {sqlType: 'TEXT',     sqlDefault: "''",     gristDefault: ''      },
};

/**
 * Returns SqlTypeInfo for a given Grist type. Accepts full type (like "Ref:Table1") or base type
 * (like "Ref"). For unknown types, returns SqlTypeInfo of type "Any".
 */
export function getSqlTypeInfo(fullColType: string): SqlTypeInfo {
  const baseType = fullColType.split(':')[0];
  return gristBaseTypeToSql[baseType as GristType] || gristBaseTypeToSql.Any;
}
