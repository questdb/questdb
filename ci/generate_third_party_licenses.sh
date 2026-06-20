#!/usr/bin/env bash
#
# Regenerate THIRD_PARTY_LICENSES.txt from the Rust dependency tree.
#
# This file is shipped in every distribution's legal/ directory (see the
# rt-*/no-jre assembly descriptors) and embedded in questdb.jar's META-INF, so
# it must reflect the Rust crates actually linked into libquestdbr. It used to
# be refreshed and committed by the rebuild_rust.yml workflow, which was removed
# when the native libraries stopped being committed; the release and Docker
# builds therefore regenerate it from source instead. The committed copy stays
# in the tree as a fallback for local/manual builds that skip this step.
#
# Requires cargo on PATH (cargo-deny shells out to `cargo metadata`); installs a
# prebuilt, arch-matched cargo-deny pinned to $CARGO_DENY_VERSION if one is not
# already available. The build jobs (package-linux, core/Dockerfile) set that
# variable; run this before `mvn package`.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
out_file="${repo_root}/THIRD_PARTY_LICENSES.txt"

if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo is required (cargo-deny runs 'cargo metadata') but was not found on PATH" >&2
    exit 1
fi

if ! command -v cargo-deny >/dev/null 2>&1; then
    if [ -z "${CARGO_DENY_VERSION:-}" ]; then
        echo "CARGO_DENY_VERSION is not set; the build job must pin the cargo-deny version" >&2
        exit 1
    fi

    case "$(uname -m)" in
        x86_64 | amd64) deny_arch="x86_64" ;;
        aarch64 | arm64) deny_arch="aarch64" ;;
        *) echo "Unsupported architecture for cargo-deny: $(uname -m)" >&2; exit 1 ;;
    esac

    archive="cargo-deny-${CARGO_DENY_VERSION}-${deny_arch}-unknown-linux-musl.tar.gz"
    echo "Installing cargo-deny ${CARGO_DENY_VERSION} (${deny_arch})"
    tmp_dir="$(mktemp -d)"
    curl -fsSL "https://github.com/EmbarkStudios/cargo-deny/releases/download/${CARGO_DENY_VERSION}/${archive}" \
        | tar -xz --strip-components=1 -C "${tmp_dir}"
    install -m 0755 "${tmp_dir}/cargo-deny" /usr/local/bin/cargo-deny
    rm -rf "${tmp_dir}"
fi

echo "Regenerating ${out_file}"
cd "${repo_root}/core/rust/qdbr"

# Build into a temp file alongside the target and only move it into place once
# cargo-deny has succeeded. The committed copy is the fallback for local/manual
# builds, so a mid-run failure (e.g. a registry fetch hiccup aborting under
# set -e) must not leave it truncated to just the header lines.
tmp_out="$(mktemp "${out_file}.XXXXXX")"
trap 'rm -f "${tmp_out}"' EXIT
{
    echo "Third-party licenses for Rust dependencies of QuestDB"
    echo "======================================================"
    echo ""
    echo "Generated on $(date -u +%Y-%m-%d) by cargo-deny"
    echo ""
    cargo deny list --layout license
} > "${tmp_out}"
mv "${tmp_out}" "${out_file}"
trap - EXIT

echo "Wrote ${out_file}"
