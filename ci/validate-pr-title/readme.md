This folder contains configuration files which are used to run validation rules on Github pull requests titles.

It is done by running [Danger JS](https://danger.systems/js/) tool in [github action](../../.github/workflows/danger.yml).

In addition, the validation rules are tested. Tests can be executed with node, by running `node ./validate.test.js`
