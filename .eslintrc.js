// Copied from main grist repo.
module.exports = {
  // Basic settings for JS files.
  extends: ['eslint:recommended'],
  env: {
    node: true,
    es6: true,
    mocha: true,
  },
  // Set parser to support, e.g. import() function for dynamic imports (see
  // https://stackoverflow.com/a/47833471/328565 and https://stackoverflow.com/a/69557309/328565).
  parser: '@babel/eslint-parser',
  parserOptions: {
    ecmaVersion: 2018,
    requireConfigFile: false,
  },
  ignorePatterns: [
    // Add ignore patterns here.
  ],
  rules: {
    'no-unused-vars': ["error", {args: "none"}],
    'no-prototype-builtins': 'off',
    'no-trailing-spaces': 'warn',
    'comma-spacing': 'warn',
    'semi-spacing': 'warn',
  },

  // The Typescript-specific settings apply only to TS files. ESLint is expensive here, and has to
  // analyze dependencies for type-checking. (In an editor, it's much faster when used with
  // tsserver.)
  overrides: [{
    files: "**/*.ts",
    extends: [
      'eslint:recommended',
      'plugin:@typescript-eslint/recommended',
      'plugin:@typescript-eslint/recommended-requiring-type-checking',
    ],
    parser: "@typescript-eslint/parser",
    plugins: ["@typescript-eslint", "@stylistic"],
    parserOptions: {
      tsconfigRootDir: __dirname,
      project: ['./tsconfig.eslint.json'],
      sourceType: 'module',
      ecmaVersion: 2018,
    },
    env: {
      node: true,
      browser: true,
      es6: true,
      mocha: true,
    },
    globals: {
      Promise: true
    },
    rules: {
      // A lot of the options below would be helpful to keep on, but would need a lot of fix-ups.
      "@typescript-eslint/ban-types": 'off',
      "@typescript-eslint/explicit-member-accessibility": ["error", {overrides: {constructors: 'off'}}],
      "@typescript-eslint/explicit-module-boundary-types": 'off',
      // These settings mimic what we had before with tslint.
      "@typescript-eslint/member-ordering": ["warn", {default: [
        'public-static-field',
        'public-static-method',
        'protected-static-field', 'private-static-field', 'static-field',
        'protected-static-method', 'private-static-method', 'static-method',
        'public-field', 'protected-field', 'private-field', 'field',
        'public-constructor', 'protected-constructor', 'private-constructor', 'constructor',
        'public-method', 'protected-method', 'private-method', 'method',
      ]}],
      "@typescript-eslint/naming-convention": ["warn", {
        selector: "memberLike", filter: { match: false, regex: '(listenTo)' },
        modifiers: ["private"], format: ["camelCase"], leadingUnderscore: "require"
      }],
      "@typescript-eslint/no-empty-function": 'off',
      "@typescript-eslint/no-explicit-any": 'off',
      "@typescript-eslint/no-inferrable-types": 'off',
      "@typescript-eslint/no-misused-promises": ["error", {"checksVoidReturn": false}],
      "@typescript-eslint/no-namespace": 'off',
      "@typescript-eslint/no-non-null-assertion": 'off',
      "@typescript-eslint/no-shadow": ["warn", { ignoreTypeValueShadow: true }],
      "@typescript-eslint/no-this-alias": 'off',
      "@typescript-eslint/no-type-alias": ["warn", {
        "allowAliases": "always",
        "allowCallbacks": "always",
        "allowConditionalTypes": "always",
        "allowConstructors": "always",
        "allowLiterals": "in-unions-and-intersections",
        "allowMappedTypes": "always",
        "allowTupleTypes": "always",
        "allowGenerics": "always",
      }],
      "@typescript-eslint/no-unsafe-assignment": 'off',
      "@typescript-eslint/no-unsafe-argument": 'off',
      "@typescript-eslint/no-unsafe-call": 'off',
      "@typescript-eslint/no-unsafe-member-access": 'off',
      "@typescript-eslint/no-unsafe-return": 'off',
      "@typescript-eslint/no-unused-vars": ["error", { "vars": "all", "args": "none", "ignoreRestSiblings": false }],
      "@typescript-eslint/no-var-requires": 'off',
      "@typescript-eslint/prefer-regexp-exec": 'off',
      "@typescript-eslint/require-await": 'off',
      "@typescript-eslint/restrict-plus-operands": 'off',
      "@typescript-eslint/restrict-template-expressions": 'off',
      "@stylistic/type-annotation-spacing": 'warn',
      "@typescript-eslint/unbound-method": 'off',
      'no-undef': 'off',
      'no-prototype-builtins': 'off',
      'prefer-rest-params': 'off',
      'no-console': 'off',
      'no-shadow': 'off',
      'no-inner-declarations': 'off',
      'max-len': ['warn', {code: 120, ignoreUrls: true}],
      'sort-imports': ['warn', {ignoreDeclarationSort: true, ignoreCase: true, allowSeparatedGroups: true}],
      'no-trailing-spaces': 'warn',
      'no-unused-expressions': ["error", {allowShortCircuit: true, allowTernary: true}],

      'block-spacing': ['warn', 'always'],
      'comma-spacing': 'warn',
      'curly': ['warn', 'all'],
      'semi': ['warn', 'always'],
      'semi-spacing': 'warn',
    },
  }]
}
