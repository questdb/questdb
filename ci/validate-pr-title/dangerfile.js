const { danger, fail } = require("danger");
const { validate } = require("./validate");

validate({ title: danger.github.pr.title, onError: fail });
