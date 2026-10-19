#!/usr/bin/env bash
#
# Initializes git submodules with authentication when any credential is
# available, and with bounded retry when none is. GitHub throttles anonymous
# git-over-HTTPS from the CI agents' shared egress IPs with intermittent
# 401s that git reports as:
#   fatal: could not read Username for 'https://github.com': terminal prompts disabled
# even though every submodule repo is public.
#
# ci/git-auth-env.sh resolves the credential (GH_TOKEN, or the credential
# the checkout task persisted with persistCredentials: true) and exports it
# through GIT_CONFIG_{COUNT,KEY_0,VALUE_0}, which every spawned submodule
# clone/fetch inherits (the same mechanism the checkout task uses for its
# own submodule handling). Nothing is written to disk and nothing appears
# on a command line or in xtrace output (neither script uses `set -x`).
# With no credential at all the clone stays anonymous and relies on the
# retry: GitHub's anonymous throttle windows are short, so ~4 minutes of
# backoff outlasts a burst without stalling a healthy build, whereas git's
# own instant second attempt lands inside the same window and dies.
#
# Each attempt clones shallow (--depth 1) first: CI never needs submodule
# history, only the pinned tree. Several submodules pin a SHA that is not a
# branch tip; git then fetches that SHA directly, which github.com serves
# for any ref-reachable commit (uploadpack.allowReachableSHA1InWant) -- the
# same mechanism .gitmodules' existing shallow = true entries and every
# exact-SHA CI fetch already rely on. Because the git protocol does not
# guarantee that for arbitrary servers, a failed shallow pass falls back to
# a full-history pass within the same attempt before any backoff.
#
# Usage: bash ci/submodule-update.sh [--recursive] <submodule-path>...

set -eu

recursive=""
if [[ "${1:-}" == "--recursive" ]]; then
  recursive="--recursive"
  shift
fi
if [[ $# -eq 0 ]]; then
  echo "usage: bash ci/submodule-update.sh [--recursive] <submodule-path>..." >&2
  exit 2
fi

eval "$(bash "$(dirname "$0")/git-auth-env.sh")"

update_args=(--init)
if [[ -n "$recursive" ]]; then
  update_args+=(--recursive)
fi

# A failed `git submodule update` is safe to rerun: git removes the target
# directory of a failed clone, and submodules that already completed are
# skipped on the next pass.
for delay in 0 15 30 60 120; do
  if [[ "$delay" -gt 0 ]]; then
    echo "git submodule update failed; retrying in ${delay}s" >&2
    sleep "$delay"
  fi
  if git submodule update "${update_args[@]}" --depth 1 "$@"; then
    exit 0
  fi
  echo "shallow submodule update failed; trying full history" >&2
  if git submodule update "${update_args[@]}" "$@"; then
    exit 0
  fi
done
echo "git submodule update failed after all retries" >&2
exit 1
