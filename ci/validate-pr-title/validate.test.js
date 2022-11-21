const assert = require("node:assert").strict;
const { validate, allowedTypes, allowedSubTypes } = require("./validate");

const testValid = (title) =>
  assert.doesNotThrow(() =>
    validate({
      title,
      onError: () => {
        throw `should accept "${title}"`;
      },
    })
  );

const testInvalid = (title) =>
  assert.throws(
    () => validate({ title, onError }),
    `should NOT accept "${title}"`
  );

allowedTypes.forEach((type) => {
  allowedSubTypes.forEach((subType) => {
    testValid(
      `${type}(${subType}): foo`,
      `should accept "${type}(${subType}): foo"`
    );
  });
});

testValid("build: 6.6");
testValid("build: hello world");
testInvalid("build");

testValid(`build: house`);
testInvalid(`build(house)`);

testInvalid(`foo: bar`);
testInvalid(`update(bar): baz`);
testInvalid(`ui: updating stuff`);
