module.exports = {
  processors: ["stylelint-processor-styled-components"],
  extends: [
    "stylelint-config-recommended",
    "stylelint-config-prettier",
    "stylelint-config-styled-components",
  ],
  rules: {
    "no-descending-specificity": null,
  },
}
