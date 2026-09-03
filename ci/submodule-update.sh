#!/usr/bin/env bash
#
# Initializes git submodules with authentication when any credential is
# available, and with bounded retry when none is. GitHub throttles anonymous
# git-over-HTTPS from the CI agents' shared egress IPs with intermittent
# 401s that git reports as:
#   fatal: could not read Username for 'https://github.com': terminal prompts disabled
# even though every submodule repo is public.
#
# Credential sources, tried in order:
#
#   1. GH_TOKEN, when it holds a real token. Azure leaves an undefined
#      pipeline variable as its literal "$(VAR)" macro text, and fork PR
#      builds never receive secret variables at all (Azure withholds them
#      by design), so only a token-shaped value is used.
#   2. The credential the checkout task persisted (persistCredentials: true
#      on the job's checkout). The agent scopes it to the parent repo URL
#      (http.https://github.com/questdb/questdb.extraheader); the submodules
#      live elsewhere on the same host, so this script re-applies it to all
#      of github.com for the child clones. Unlike GH_TOKEN, this credential
#      exists on fork PR builds too, because the checkout task itself needs
#      it to fetch the merge ref.
#   3. Nothing: clone anonymously, but retry with backoff. GitHub's
#      anonymous throttle windows are short; ~4 minutes of backoff outlasts
#      a burst without stalling a healthy build, whereas git's own instant
#      second attempt lands inside the same window and dies.
#
# The credential travels through GIT_CONFIG_{COUNT,KEY_0,VALUE_0}, which
# every spawned submodule clone/fetch inherits (the same mechanism the
# checkout task uses for its own submodule handling). Nothing is written to
# disk and nothing appears on a command line or in xtrace output (this
# script deliberately does not `set -x`).
#
# The update deliberately fetches full history (no --depth): several
# submodules pin a SHA that is not a branch tip, and a shallow fetch of an
# unadvertised object is not guaranteed to be served.
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

auth_header=""
if [[ ${GH_TOKEN:-} =~ ^[A-Za-z0-9_-]+$ ]]; then
  # x-access-token works as the basic-auth username for both PATs and GitHub
  # App installation tokens. macOS base64 has no -w flag; tr strips the
  # trailing newline portably.
  auth_header="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$GH_TOKEN" | base64 | tr -d '\n')"
  echo "Authenticating submodule fetch with GH_TOKEN"
else
  origin_url=$(git config --get remote.origin.url 2>/dev/null || true)
  if [[ -n "$origin_url" ]]; then
    for url in "$origin_url" "${origin_url%.git}" "$origin_url.git"; do
      if auth_header=$(git config --get "http.$url.extraheader" 2>/dev/null) \
          && [[ -n "$auth_header" ]]; then
        echo "Reusing the checkout task's persisted credential for submodule fetch"
        break
      fi
      auth_header=""
    done
  fi
fi

if [[ -n "$auth_header" ]]; then
  export GIT_CONFIG_COUNT=1
  export GIT_CONFIG_KEY_0="http.https://github.com/.extraheader"
  export GIT_CONFIG_VALUE_0="$auth_header"
else
  echo "No token and no persisted checkout credential; cloning submodules anonymously"
fi

# A failed `git submodule update` is safe to rerun: git removes the target
# directory of a failed clone, and submodules that already completed are
# skipped on the next pass.
for delay in 0 15 30 60 120; do
  if [[ "$delay" -gt 0 ]]; then
    echo "git submodule update failed; retrying in ${delay}s" >&2
    sleep "$delay"
  fi
  # shellcheck disable=SC2086  # $recursive is empty or a single flag
  if git submodule update --init $recursive "$@"; then
    exit 0
  fi
done
echo "git submodule update failed after all retries" >&2
exit 1
