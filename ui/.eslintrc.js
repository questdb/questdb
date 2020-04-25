const path = require("path")

module.exports = {
  env: {
    browser: true,
    es6: true,
  },
  extends: [
    "standard",
    "eslint:recommended",
    "plugin:@typescript-eslint/eslint-recommended",
    "plugin:@typescript-eslint/recommended",
    "plugin:@typescript-eslint/recommended-requiring-type-checking",
    "plugin:prettier/recommended",
    "prettier",
    "prettier/@typescript-eslint",
    "prettier/babel",
    "prettier/standard",
  ],
  globals: { ace: true },
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaFeatures: {},
    ecmaVersion: 2018,
    project: path.resolve(__dirname, "./tsconfig.json"),
    tsconfigRootDir: __dirname,
    sourceType: "module",
  },
  plugins: ["@typescript-eslint", "babel", "prettier", "standard"],
  rules: {
    "@typescript-eslint/prefer-interface": "off",
    "@typescript-eslint/no-empty-function": "off",
    "@typescript-eslint/explicit-function-return-type": "off",
    "@typescript-eslint/no-this-alias": "off",
    "quote-props": ["error", "as-needed"],
    "object-shorthand": ["error", "always"],
    "no-var": ["error"],
    "no-console": [
      "warn",
      {
        allow: ["warn", "error"],
      },
    ],
    "no-unused-vars": "off",
    "prettier/prettier": ["error"],
  },
}
