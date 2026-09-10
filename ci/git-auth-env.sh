#!/usr/bin/env bash
#
# Resolves a GitHub credential for child git processes and prints `export`
# lines for GIT_CONFIG_{COUNT,KEY_0,VALUE_0}, which every git spawned from
# the calling shell inherits -- including git run by tools like CMake
# FetchContent, not just direct git commands. Eval the output:
#
#   set +x  # keep the header out of xtrace
#   eval "$(bash ci/git-auth-env.sh)"
#   set -x
#
# GitHub throttles anonymous git-over-HTTPS from the CI agents' shared
# egress IPs with intermittent 401s that git reports as:
#   fatal: could not read Username for 'https://github.com': terminal prompts disabled
# even though the repos are public.
#
# Credential sources, tried in order:
#
#   1. GH_TOKEN, when it holds a real token. Azure leaves an undefined
#      pipeline variable as its literal "$(VAR)" macro text, and fork PR
#      builds never receive secret variables at all (Azure withholds them
#      by design), so only a token-shaped value is used.
#   2. The credential the checkout task persisted (persistCredentials: true
#      on the job's checkout). The agent scopes it to the parent repo URL
#      (http.https://github.com/questdb/questdb.extraheader); other repos
#      live elsewhere on the same host, so this re-applies it to all of
#      github.com. Unlike GH_TOKEN, this credential exists on fork PR
#      builds too, because the checkout task itself needs it to fetch the
#      merge ref.
#
# Prints nothing when neither source exists, so anonymous callers stay
# anonymous. Progress messages go to stderr; stdout carries only the
# export lines, and the credential value never appears in build logs.

set -eu

auth_header=""
if [[ ${GH_TOKEN:-} =~ ^[A-Za-z0-9_-]+$ ]]; then
  # x-access-token works as the basic-auth username for both PATs and GitHub
  # App installation tokens. macOS base64 has no -w flag; tr strips the
  # trailing newline portably.
  auth_header="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$GH_TOKEN" | base64 | tr -d '\n')"
  echo "Authenticating github.com fetches with GH_TOKEN" >&2
else
  origin_url=$(git config --get remote.origin.url 2>/dev/null || true)
  if [[ -n "$origin_url" ]]; then
    for url in "$origin_url" "${origin_url%.git}" "$origin_url.git"; do
      if auth_header=$(git config --get "http.$url.extraheader" 2>/dev/null) \
          && [[ -n "$auth_header" ]]; then
        echo "Reusing the checkout task's persisted credential for github.com fetches" >&2
        break
      fi
      auth_header=""
    done
  fi
fi

if [[ -n "$auth_header" ]]; then
  printf 'export GIT_CONFIG_COUNT=1\n'
  printf 'export GIT_CONFIG_KEY_0=%q\n' "http.https://github.com/.extraheader"
  printf 'export GIT_CONFIG_VALUE_0=%q\n' "$auth_header"
else
  echo "No token and no persisted checkout credential; github.com fetches stay anonymous" >&2
fi
