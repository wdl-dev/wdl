import js from "@eslint/js";
import globals from "globals";

export default [
  js.configs.recommended,
  {
    ignores: [
      "**/node_modules/**",
      "**/.deploy-dist/**",
      ".claude/worktrees/**",
      // Fixtures/examples stay out; system-workers is production infra and is linted.
      "examples/**",
      "test-workers/**",
      "shared/vendor/**",
    ],
  },
  {
    files: ["**/*.js", "**/*.mjs", "**/*.cjs"],
    languageOptions: {
      ecmaVersion: 2025,
      sourceType: "module",
      globals: {
        ...globals.node,
        ...globals.browser,
        // workerd-specific
        WorkerEntrypoint: "readonly",
        WebSocketPair: "readonly",
      },
    },
    rules: {
      "no-unused-vars": ["error", { argsIgnorePattern: "^_", varsIgnorePattern: "^_" }],
      "no-empty": ["error", { allowEmptyCatch: true }],
      "eqeqeq": ["error", "smart"],
      "no-implicit-coercion": "error",
      "no-throw-literal": "error",
      "no-var": "error",
      "prefer-const": "error",
    },
  },
  {
    files: ["tests/**/*.js", "tests/**/*.mjs", "tests/**/*.cjs"],
    languageOptions: {
      globals: { ...globals.node },
    },
  },
  {
    files: [
      "auth/**/*.js",
      "control/**/*.js",
      "gateway/**/*.js",
      "runtime/**/*.js",
      "d1-runtime/**/*.js",
      "do-runtime/**/*.js",
      "shared/**/*.js",
      "system-workers/**/*.js",
    ],
    rules: {
      "no-restricted-globals": [
        "error",
        { name: "Buffer", message: "Use Web APIs or an explicit typed globalThis boundary." },
        { name: "process", message: "Use an explicit typed globalThis boundary." },
        { name: "global", message: "Use globalThis through an explicit typed boundary." },
      ],
    },
  },
];
