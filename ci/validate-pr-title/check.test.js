"use strict";

// Exercises check.js against a stubbed GitHub API. The point is the reporting
// behaviour rather than the rules themselves, which validate.test.js covers: that a
// rejection is explained exactly once, that fixing the title takes the explanation
// away again, that the status lands on the commit the pull request is actually
// showing, and that a verdict is published even when the run comes apart.

const assert = require("node:assert").strict;
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

process.env.GITHUB_REPOSITORY = "questdb/questdb";
process.env.GITHUB_API_URL = "https://api.github.com";
process.env.GITHUB_TOKEN = "stub-token";

const { run, MARKER, LEGACY_MARKER } = require("./check");

const VALID = "feat(sql): add a thing";
const INVALID = "just some words";

// Records every call and answers from a small routing table, so a test can assert
// on what the job asked GitHub to do rather than on how it phrased it.
function stubApi({ comments = [], title = VALID, fail = null }) {
  const calls = [];
  global.fetch = async (url, options) => {
    const method = options.method;
    const route = String(url).replace("https://api.github.com", "");
    calls.push({ method, route, body: options.body ? JSON.parse(options.body) : null });

    const answer = (status, payload) => ({
      ok: status < 400,
      status,
      json: async () => payload,
      text: async () => JSON.stringify(payload),
    });

    if (fail && fail(method, route)) {
      return answer(500, { message: "stub failure" });
    }
    if (method === "GET" && /\/issues\/\d+\/comments/.test(route)) {
      return answer(200, route.includes("page=1") ? comments : []);
    }
    if (method === "GET" && /\/pulls\/\d+$/.test(route)) {
      return answer(200, { title });
    }
    if (method === "DELETE") {
      return answer(204, null);
    }
    return answer(201, { id: 4242 });
  };
  return calls;
}

function pullRequestEvent(title) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pr-title-"));
  const file = path.join(dir, "event.json");
  fs.writeFileSync(
    file,
    JSON.stringify({ pull_request: { number: 7595, title, head: { sha: "headsha" } } })
  );
  process.env.GITHUB_EVENT_NAME = "pull_request";
  process.env.GITHUB_EVENT_PATH = file;
  // What GITHUB_SHA is on a pull_request event: the throwaway merge commit, which
  // is not where the status belongs.
  process.env.GITHUB_SHA = "mergesha";
}

function mergeGroupEvent() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pr-title-"));
  const file = path.join(dir, "event.json");
  fs.writeFileSync(
    file,
    JSON.stringify({
      merge_group: {
        head_ref: "refs/heads/gh-readonly-queue/master/pr-7595-abc123",
        head_sha: "queuesha",
      },
    })
  );
  process.env.GITHUB_EVENT_NAME = "merge_group";
  process.env.GITHUB_EVENT_PATH = file;
  process.env.GITHUB_SHA = "queuesha";
}

const statusOf = (calls) => calls.find((call) => call.route.includes("/statuses/"));
const commentCalls = (calls) =>
  calls.filter((call) => call.method !== "GET" && call.route.includes("comments"));

async function test(name, body) {
  process.exitCode = 0;
  await body();
  console.log(`ok - ${name}`);
}

async function main() {
  await test("an invalid title fails the status and explains itself once", async () => {
    pullRequestEvent(INVALID);
    const calls = stubApi({});
    await run();

    const status = statusOf(calls);
    assert.equal(status.route, "/repos/questdb/questdb/statuses/headsha");
    assert.equal(status.body.state, "failure");
    // Spelled out rather than compared against CONTEXT: this is the string the
    // master ruleset requires, so a rename has to fail here and send whoever made
    // it to the ruleset, instead of quietly agreeing with itself.
    assert.equal(status.body.context, "PR title");

    const posted = commentCalls(calls);
    assert.equal(posted.length, 1);
    assert.equal(posted[0].method, "POST");
    assert.ok(posted[0].body.body.includes(MARKER));
    assert.ok(posted[0].body.body.includes(INVALID));
    assert.equal(process.exitCode, 1);
  });

  await test("a repeat run on the same bad title does not stack a second comment", async () => {
    pullRequestEvent(INVALID);
    const { commentBody } = require("./check");
    const existing = { id: 11, body: commentBody(INVALID, rejectionReason(INVALID)) };
    const calls = stubApi({ comments: [existing] });
    await run();

    assert.equal(commentCalls(calls).length, 0, "identical comment must be left alone");
    assert.equal(statusOf(calls).body.state, "failure");
  });

  await test("fixing the title deletes the comment and turns the status green", async () => {
    pullRequestEvent(VALID);
    const calls = stubApi({ comments: [{ id: 11, body: `${MARKER}\nold complaint` }] });
    await run();

    const removed = commentCalls(calls);
    assert.equal(removed.length, 1);
    assert.equal(removed[0].method, "DELETE");
    assert.equal(removed[0].route, "/repos/questdb/questdb/issues/comments/11");
    assert.equal(statusOf(calls).body.state, "success");
    assert.equal(process.exitCode, 0);
  });

  // Returning a single match sheds one comment per run and keeps the rest, so a
  // corrected title still reads as rejected. Both duplicate shapes are covered:
  // two of ours, and one of ours beside a leftover Danger comment.
  await test("fixing the title clears every duplicate comment, not just the first", async () => {
    pullRequestEvent(VALID);
    const calls = stubApi({
      comments: [
        { id: 11, body: `${MARKER}\nold complaint` },
        { id: 12, body: `${MARKER}\na second copy` },
      ],
    });
    await run();

    const removed = commentCalls(calls);
    assert.deepEqual(
      removed.map((call) => `${call.method} ${call.route.split("/").pop()}`),
      ["DELETE 11", "DELETE 12"]
    );
    assert.equal(statusOf(calls).body.state, "success");
  });

  await test("fixing the title clears ours and the Danger comment beside it", async () => {
    pullRequestEvent(VALID);
    const calls = stubApi({
      comments: [
        { id: 11, body: `${MARKER}\nold complaint` },
        { id: 77, body: `<!--\n  ${LEGACY_MARKER}\n-->\n<table>...</table>` },
      ],
    });
    await run();

    assert.deepEqual(
      commentCalls(calls).map((call) => `${call.method} ${call.route.split("/").pop()}`),
      ["DELETE 11", "DELETE 77"]
    );
    assert.equal(statusOf(calls).body.state, "success");
  });

  await test("a duplicate is cleared while the surviving comment is reworded", async () => {
    pullRequestEvent(INVALID);
    const calls = stubApi({
      comments: [
        { id: 11, body: `${MARKER}\nstale wording` },
        { id: 12, body: `${MARKER}\nanother copy` },
        { id: 77, body: `<!--\n  ${LEGACY_MARKER}\n-->\n<table>...</table>` },
      ],
    });
    await run();

    const touched = commentCalls(calls);
    assert.deepEqual(
      touched.map((call) => `${call.method} ${call.route.split("/").pop()}`),
      ["PATCH 11", "DELETE 12", "DELETE 77"],
      "one comment carries the explanation, every other copy goes away"
    );
    assert.ok(touched[0].body.body.includes(INVALID));
    assert.equal(statusOf(calls).body.state, "failure");
  });

  await test("a clean title with nothing to clean up touches no comment", async () => {
    pullRequestEvent(VALID);
    const calls = stubApi({});
    await run();
    assert.equal(commentCalls(calls).length, 0);
    assert.equal(statusOf(calls).body.state, "success");
  });

  // Every other legacy case builds its fixture from LEGACY_MARKER imported out of
  // check.js, so both sides move together and none of them would notice if the
  // constant stopped matching what Danger actually wrote. This one hardcodes the
  // real thing, copied from the comment questdb-butler left on #7558.
  await test("the legacy marker matches a comment Danger really wrote", async () => {
    pullRequestEvent(VALID);
    const realDangerComment = [
      "",
      "<!--",
      "  1 failure:  Please update the...",
      "  0 warning: ",
      "  ",
      "  ",
      "  DangerID: danger-id-Danger;",
      "-->",
      "",
      "<table>",
      "  <thead>",
    ].join("\n");
    const calls = stubApi({ comments: [{ id: 5408116926, body: realDangerComment }] });
    await run();

    const removed = commentCalls(calls);
    assert.equal(removed.length, 1, "a real Danger comment has to be recognised");
    assert.equal(removed[0].method, "DELETE");
    assert.equal(statusOf(calls).body.state, "success");
  });

  await test("a leftover Danger comment is cleared once the title is fixed", async () => {
    pullRequestEvent(VALID);
    const calls = stubApi({
      comments: [{ id: 77, body: `<!--\n  1 failure\n  ${LEGACY_MARKER}\n-->\n<table>...</table>` }],
    });
    await run();

    const removed = commentCalls(calls);
    assert.equal(removed.length, 1);
    assert.equal(removed[0].method, "DELETE");
    assert.equal(removed[0].route, "/repos/questdb/questdb/issues/comments/77");
    assert.equal(statusOf(calls).body.state, "success");
  });

  await test("a leftover Danger comment is replaced, not edited, while the title is bad", async () => {
    pullRequestEvent(INVALID);
    const calls = stubApi({
      comments: [{ id: 77, body: `<!--\n  ${LEGACY_MARKER}\n-->\n<table>...</table>` }],
    });
    await run();

    const touched = commentCalls(calls);
    assert.deepEqual(
      touched.map((call) => call.method),
      ["DELETE", "POST"],
      "another author's comment cannot be edited, so it is removed and rewritten"
    );
    assert.ok(touched[1].body.body.includes(MARKER));
    assert.equal(statusOf(calls).body.state, "failure");
  });

  await test("a merge group reads the queued title and reports by status only", async () => {
    mergeGroupEvent();
    const calls = stubApi({ title: INVALID });
    await run();

    assert.ok(calls.some((call) => call.route === "/repos/questdb/questdb/pulls/7595"));
    assert.equal(commentCalls(calls).length, 0, "a merge group must not comment");
    assert.equal(statusOf(calls).route, "/repos/questdb/questdb/statuses/queuesha");
    assert.equal(statusOf(calls).body.state, "failure");
    assert.equal(process.exitCode, 1);
  });

  await test("a comment that cannot be written does not fail a valid title", async () => {
    pullRequestEvent(VALID);
    const calls = stubApi({
      comments: [{ id: 11, body: `${MARKER}\nold complaint` }],
      fail: (method) => method === "DELETE",
    });
    await run();

    assert.equal(statusOf(calls).body.state, "success", "the status is the gate, not the comment");
    assert.equal(process.exitCode, 0);
  });

  await test("an unreadable merge group ref still publishes a failure", async () => {
    mergeGroupEvent();
    process.env.GITHUB_EVENT_PATH = (() => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pr-title-"));
      const file = path.join(dir, "event.json");
      fs.writeFileSync(file, JSON.stringify({ merge_group: { head_ref: "refs/heads/nonsense" } }));
      return file;
    })();
    const calls = stubApi({});
    await run();

    assert.equal(statusOf(calls).body.state, "failure");
    assert.equal(process.exitCode, 1);
  });

  // The scenarios deliberately leave process.exitCode at 1 behind them, since that
  // is what the job under test sets on a rejection.
  process.exitCode = 0;
  console.log("\nall check.js scenarios passed");
}

// The exact rejection text, so the "no duplicate comment" case can build the body
// the job would have written on the previous run.
function rejectionReason(title) {
  let reason = "";
  require("./validate").validate({ title, onError: (message) => (reason = message) });
  return reason;
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
