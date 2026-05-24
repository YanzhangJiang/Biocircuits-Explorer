import globals from "globals";

export default [
  {
    files: ["public/js/**/*.js"],
    ignores: ["public/js/dist/**", "public/vendor/**"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      globals: {
        ...globals.browser,
        Plotly: "readonly",
        JSZip: "readonly",
        BiocircuitsExplorerWorkspaceShell: "writable",
        ROPWorkspaceShell: "writable",
      },
    },
    rules: {
      // Keep this config permissive: the goal here is catching obvious bugs
      // (undef vars, dead imports), not enforcing style. Tighten only after
      // existing code is cleaned up — turning on opinionated rules now would
      // produce hundreds of warnings and bury actionable findings.
      "no-undef": "error",
      "no-unused-vars": ["warn", { "args": "none", "varsIgnorePattern": "^_" }],
      "no-redeclare": "error",
      "no-dupe-keys": "error",
      "no-dupe-args": "error",
      "no-unreachable": "warn",
      "no-empty": ["warn", { "allowEmptyCatch": true }],
    },
  },
];
