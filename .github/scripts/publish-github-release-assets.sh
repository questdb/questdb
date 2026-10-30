#!/usr/bin/env bash
#
# Copyright (c) 2014-2019 Appsicle
# Copyright (c) 2019-2026 QuestDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

if [[ "$#" -ne 3 ]]; then
    echo "usage: $0 <tag-name> <linux-archive-directory> <windows-archive-directory>" >&2
    exit 2
fi

tag_name="$1"
linux_directory="$2"
windows_directory="$3"

shopt -s nullglob
archives=("${linux_directory}"/*.tar.gz "${windows_directory}"/*.tar.gz)
if [[ "${#archives[@]}" -eq 0 ]]; then
    echo "no release archives were supplied" >&2
    exit 1
fi

for archive in "${archives[@]}"; do
    asset_name="$(basename "${archive}")"
    expected_sha256="$(sha256sum "${archive}" | awk '{print $1}')"
    if gh release view "${tag_name}" --json assets --jq '.assets[].name' | grep -Fxq "${asset_name}"; then
        mkdir -p existing
        gh release download "${tag_name}" --pattern "${asset_name}" --dir existing --clobber
        actual_sha256="$(sha256sum "existing/${asset_name}" | awk '{print $1}')"
        if [[ "${actual_sha256}" != "${expected_sha256}" ]]; then
            echo "release asset checksum mismatch: ${asset_name}" >&2
            exit 1
        fi
        echo "reusing checksum-equal release asset: ${asset_name}"
    else
        gh release upload "${tag_name}" "${archive}#${asset_name}"
    fi
done

gh release edit "${tag_name}" --draft=false --latest
