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

// onError has to be a real callback here. Passing a bare `onError` identifier makes
// this assertion pass on the ReferenceError that raises instead of on the title
// being rejected, which lets every negative case below succeed against a validator
// that accepts everything.
const testInvalid = (title) =>
  assert.throws(
    () =>
      validate({
        title,
        onError: () => {
          throw new Error(`rejected "${title}"`);
        },
      }),
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
