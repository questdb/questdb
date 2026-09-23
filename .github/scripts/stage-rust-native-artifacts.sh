#!/usr/bin/env bash
#
# Copyright (c) 2014-2019 Appsicle
# Copyright (c) 2019-2026 QuestDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly script_dir
repo_dir="$(cd "${script_dir}/../.." && pwd)"
readonly repo_dir
readonly raw_root="${1:-${repo_dir}/core/target/downloaded-rust-artifacts}"
readonly staged_root="${2:-${repo_dir}/core/target/native-libs}"
readonly staged_bin_dir="${staged_root}/io/questdb/bin"

readonly -a raw_directories=(
    rust-linux-x64
    rust-linux-arm64
    rust-macos-arm64
    rust-windows
)
readonly -a raw_files=(
    libquestdbr.so
    libquestdbr.so
    libquestdbr.dylib
    questdbr.dll
)
readonly -a staged_files=(
    io/questdb/bin/linux-x86-64/libquestdbr.so
    io/questdb/bin/linux-aarch64/libquestdbr.so
    io/questdb/bin/darwin-aarch64/libquestdbr.dylib
    io/questdb/bin/windows-x86-64/questdbr.dll
)

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

assert_exact_raw_root() {
    local actual
    local expected

    [[ -d "${raw_root}" && ! -L "${raw_root}" ]] || fail "raw artifact root is not a directory: ${raw_root}"
    actual="$(find "${raw_root}" -mindepth 1 -maxdepth 1 -printf '%f\n' | LC_ALL=C sort)"
    expected="$(printf '%s\n' "${raw_directories[@]}" | LC_ALL=C sort)"
    [[ "${actual}" == "${expected}" ]] || fail "raw artifact root must contain exactly the four named Rust artifacts"
}

assert_raw_input() {
    local raw_directory="$1"
    local raw_file="$2"
    local actual

    [[ -d "${raw_root}/${raw_directory}" && ! -L "${raw_root}/${raw_directory}" ]] || fail "missing raw artifact directory: ${raw_directory}"
    actual="$(find "${raw_root}/${raw_directory}" -mindepth 1 -printf '%P:%y\n' | LC_ALL=C sort)"
    [[ "${actual}" == "${raw_file}:f" ]] || fail "${raw_directory} must contain exactly ${raw_file}"
    [[ -s "${raw_root}/${raw_directory}/${raw_file}" ]] || fail "raw artifact is empty: ${raw_directory}/${raw_file}"
}

assert_exact_staged_tree() {
    local actual
    local expected
    local staged_file

    actual="$(find "${staged_bin_dir}" -type f -printf '%P\n' | LC_ALL=C sort)"
    expected="$(printf '%s\n' \
        linux-x86-64/libquestdbr.so \
        linux-aarch64/libquestdbr.so \
        darwin-aarch64/libquestdbr.dylib \
        windows-x86-64/questdbr.dll | LC_ALL=C sort)"
    [[ "${actual}" == "${expected}" ]] || fail "staged tree must contain exactly the four supported Rust libraries"

    for staged_file in "${staged_files[@]}"; do
        [[ -f "${staged_root}/${staged_file}" && ! -L "${staged_root}/${staged_file}" && -s "${staged_root}/${staged_file}" ]] || fail "staged artifact is not a non-empty regular file: ${staged_file}"
    done
}

assert_exact_raw_root
for index in "${!raw_directories[@]}"; do
    assert_raw_input "${raw_directories[${index}]}" "${raw_files[${index}]}"
done

rm -rf "${staged_bin_dir}"
for index in "${!raw_directories[@]}"; do
    install -D -m 0644 \
        "${raw_root}/${raw_directories[${index}]}/${raw_files[${index}]}" \
        "${staged_root}/${staged_files[${index}]}"
done

assert_exact_staged_tree
find "${staged_bin_dir}" -type f -printf '%P ' -exec sha256sum {} \; | LC_ALL=C sort
