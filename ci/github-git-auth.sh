# Source this (". ci/github-git-auth.sh") in any CI step that runs git against
# github.com, with GH_TOKEN mapped into the step's environment:
#
#   env:
#     GH_TOKEN: $(GIT_GITHUB_TOKEN)
#
# It exports GIT_CONFIG_* so that git sends an Authorization header on the FIRST
# request. git inherits those variables into every clone and fetch subprocess it
# spawns, submodules included, and nothing is written to the agent's git config.
#
# Why not `gh auth setup-git`: that installs a credential *helper*, and git only
# consults a helper after the server answers 401. GitHub's throttle often does
# not answer 401 -- it returns an in-protocol error over a successful response:
#
#   fatal: remote error: GitHub is temporarily limiting some unauthenticated
#   downloads to protect the stability of the platform. Please retry later or
#   authenticate.
#
# git never asks for credentials there, so the helper never runs and the token is
# never used. Sending the header up front covers both throttle responses.
#
# The checkout task keeps no credentials (persistCredentials defaults to false),
# so without this a submodule clone reaches github.com anonymously even though
# every repo involved is public.
#
# Azure leaves an undefined variable as its literal macro text, so a value that
# is not token-shaped yields an empty header. git treats an empty
# http.extraHeader as "no header", so the clones stay anonymous, exactly as they
# are today.

# shellcheck shell=bash
__gga_xtrace=$(set +o | grep xtrace)
set +x
__gga_header=""
if [[ ${GH_TOKEN:-} =~ ^[A-Za-z0-9_-]+$ ]]; then
  # Azure masks the raw token, but not its base64 form -- register that too
  # before it can reach the log.
  __gga_basic=$(printf 'x-access-token:%s' "$GH_TOKEN" | base64 | tr -d '\n')
  echo "##vso[task.setsecret]$__gga_basic"
  __gga_header="AUTHORIZATION: basic $__gga_basic"
  unset __gga_basic
  echo "github.com git requests will be authenticated"
else
  echo "No usable token; github.com git requests stay anonymous"
fi
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0="http.https://github.com/.extraheader"
export GIT_CONFIG_VALUE_0="$__gga_header"
unset __gga_header
eval "$__gga_xtrace"
unset __gga_xtrace
