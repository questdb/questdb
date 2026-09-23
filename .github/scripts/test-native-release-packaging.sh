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
readonly stage_script="${script_dir}/stage-rust-native-artifacts.sh"
readonly jar_verifier="${script_dir}/verify-rust-native-jar.sh"
readonly runtime_verifier="${script_dir}/verify-rust-native-runtime-archive.sh"
temp_dir="$(mktemp -d)"
readonly temp_dir

cleanup() {
    rm -rf "${temp_dir}"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_failure() {
    local name="$1"
    shift

    if "$@" >"${temp_dir}/${name}.out" 2>&1; then
        cat "${temp_dir}/${name}.out" >&2
        fail "${name} unexpectedly succeeded"
    fi
}

assert_file_count() {
    local root="$1"
    local expected_count="$2"
    local actual_count

    actual_count="$(find "${root}" -type f | wc -l | tr -d ' ')"
    [[ "${actual_count}" == "${expected_count}" ]] || fail "expected ${expected_count} files below ${root}, got ${actual_count}"
}

create_raw_inputs() {
    local root="$1"

    mkdir -p "${root}/rust-linux-x64" \
        "${root}/rust-linux-arm64" \
        "${root}/rust-macos-arm64" \
        "${root}/rust-windows"
    printf 'linux-x64\n' > "${root}/rust-linux-x64/libquestdbr.so"
    printf 'linux-arm64\n' > "${root}/rust-linux-arm64/libquestdbr.so"
    printf 'macos-arm64\n' > "${root}/rust-macos-arm64/libquestdbr.dylib"
    printf 'windows-x64\n' > "${root}/rust-windows/questdbr.dll"
}

create_jar() {
    local jar_path="$1"
    local mode="$2"
    local staged_root="$3"

    python3 - "${jar_path}" "${mode}" "${staged_root}" <<'PY'
import pathlib
import sys
import warnings
import zipfile

warnings.filterwarnings("ignore", category=UserWarning, module="zipfile")

jar_path = pathlib.Path(sys.argv[1])
mode = sys.argv[2]
staged_root = pathlib.Path(sys.argv[3])
expected = [
    "io/questdb/bin/linux-x86-64/libquestdbr.so",
    "io/questdb/bin/linux-aarch64/libquestdbr.so",
    "io/questdb/bin/darwin-aarch64/libquestdbr.dylib",
    "io/questdb/bin/windows-x86-64/questdbr.dll",
]

with zipfile.ZipFile(jar_path, "w") as archive:
    for path in expected:
        if mode == "missing" and path.endswith("windows-x86-64/questdbr.dll"):
            continue
        data = (staged_root / path).read_bytes()
        if mode == "empty" and path.endswith("linux-x86-64/libquestdbr.so"):
            data = b""
        archive.writestr(path, data)
    if mode == "renamed":
        archive.writestr(
            "io/questdb/bin/linux-x86-64/libquestdbr-renamed.so",
            (staged_root / expected[0]).read_bytes(),
        )
    if mode == "extra":
        archive.writestr("io/questdb/bin/darwin-x86-64/libquestdbr.dylib", b"intel")
    if mode == "duplicate":
        archive.writestr(expected[0], (staged_root / expected[0]).read_bytes())
PY
}

create_runtime_archive() {
    local archive_path="$1"
    local platform="$2"
    local mode="$3"
    local staged_root="$4"

    python3 - "${archive_path}" "${platform}" "${mode}" "${staged_root}" <<'PY'
import io
import pathlib
import sys
import tarfile

archive_path = pathlib.Path(sys.argv[1])
platform = sys.argv[2]
mode = sys.argv[3]
staged_root = pathlib.Path(sys.argv[4])
source_by_platform = {
    "linux-x86-64": "io/questdb/bin/linux-x86-64/libquestdbr.so",
    "windows-x86-64": "io/questdb/bin/windows-x86-64/questdbr.dll",
}
source = source_by_platform[platform]
name = "questdb-test/lib/" + pathlib.PurePosixPath(source).name
payload = (staged_root / source).read_bytes()
if mode == "empty":
    payload = b""
if mode == "wrong-name":
    name = "questdb-test/lib/libquestdbr-wrong.so"
with tarfile.open(archive_path, "w:gz") as archive:
    info = tarfile.TarInfo(name)
    info.size = len(payload)
    archive.addfile(info, io.BytesIO(payload))
    if mode == "duplicate":
        duplicate = tarfile.TarInfo("questdb-test/lib/" + pathlib.PurePosixPath(source).name)
        duplicate.size = len(payload)
        archive.addfile(duplicate, io.BytesIO(payload))
PY
}

[[ -x "${stage_script}" ]] || fail "staging script is missing or not executable"
[[ -x "${jar_verifier}" ]] || fail "jar verifier is missing or not executable"
[[ -x "${runtime_verifier}" ]] || fail "runtime verifier is missing or not executable"

valid_raw="${temp_dir}/raw-valid"
valid_stage="${temp_dir}/staged"
create_raw_inputs "${valid_raw}"
mkdir -p "${valid_stage}/io/questdb/bin/darwin-x86-64"
printf 'stale-intel\n' > "${valid_stage}/io/questdb/bin/darwin-x86-64/libquestdbr.dylib"
"${stage_script}" "${valid_raw}" "${valid_stage}" > "${temp_dir}/stage-valid.out"
[[ ! -e "${valid_stage}/io/questdb/bin/darwin-x86-64/libquestdbr.dylib" ]] || fail "stale Intel macOS native library survived staging"
assert_file_count "${valid_stage}/io/questdb/bin" 4

missing_raw="${temp_dir}/raw-missing"
create_raw_inputs "${missing_raw}"
rm "${missing_raw}/rust-windows/questdbr.dll"
assert_failure stage-missing "${stage_script}" "${missing_raw}" "${temp_dir}/stage-missing"

empty_raw="${temp_dir}/raw-empty"
create_raw_inputs "${empty_raw}"
: > "${empty_raw}/rust-linux-x64/libquestdbr.so"
assert_failure stage-empty "${stage_script}" "${empty_raw}" "${temp_dir}/stage-empty"

renamed_raw="${temp_dir}/raw-renamed"
create_raw_inputs "${renamed_raw}"
mv "${renamed_raw}/rust-macos-arm64/libquestdbr.dylib" "${renamed_raw}/rust-macos-arm64/libquestdbr-renamed.dylib"
assert_failure stage-renamed "${stage_script}" "${renamed_raw}" "${temp_dir}/stage-renamed"

extra_raw="${temp_dir}/raw-extra"
create_raw_inputs "${extra_raw}"
printf 'extra\n' > "${extra_raw}/rust-linux-x64/unexpected.so"
assert_failure stage-extra "${stage_script}" "${extra_raw}" "${temp_dir}/stage-extra"

for mode in valid missing renamed extra duplicate empty; do
    jar_path="${temp_dir}/${mode}.jar"
    create_jar "${jar_path}" "${mode}" "${valid_stage}"
    if [[ "${mode}" == valid ]]; then
        "${jar_verifier}" "${jar_path}" "${valid_stage}" > "${temp_dir}/jar-${mode}.out"
    else
        assert_failure "jar-${mode}" "${jar_verifier}" "${jar_path}" "${valid_stage}"
    fi
done

for mode in valid empty wrong-name duplicate; do
    archive_path="${temp_dir}/runtime-${mode}.tar.gz"
    create_runtime_archive "${archive_path}" linux-x86-64 "${mode}" "${valid_stage}"
    if [[ "${mode}" == valid ]]; then
        "${runtime_verifier}" "${archive_path}" linux-x86-64 "${valid_stage}" > "${temp_dir}/runtime-${mode}.out"
    else
        assert_failure "runtime-${mode}" "${runtime_verifier}" "${archive_path}" linux-x86-64 "${valid_stage}"
    fi
done

python3 - "${repo_dir}/core/pom.xml" "${repo_dir}/pom.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET

core_pom, root_pom = map(ET.parse, sys.argv[1:])
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}


def profile(tree, profile_id):
    for item in tree.findall(".//m:profile", namespace):
        if item.findtext("m:id", namespaces=namespace) == profile_id:
            return item
    raise SystemExit(f"missing profile {profile_id}")

properties = core_pom.find("m:properties", namespace)
if properties is None or properties.findtext("m:rust.native.artifacts.directory", namespaces=namespace) != "${project.build.directory}/native-libs":
    raise SystemExit("missing normalized native-artifact property")

normal = profile(core_pom, "build-rust-library")
if "process-resources" not in ET.tostring(normal, encoding="unicode"):
    raise SystemExit("normal build does not remove stale Rust natives at process-resources")

aggregate = profile(core_pom, "include-rust-native-artifacts")
aggregate_text = ET.tostring(aggregate, encoding="unicode")
for required in ("skipNative", "process-resources", "verify-rust-native-jar.sh"):
    if required not in aggregate_text:
        raise SystemExit(f"aggregate profile is missing {required}")
if "maven-clean-plugin" in aggregate_text:
    raise SystemExit("aggregate profile must not bind maven-clean-plugin")

central = profile(core_pom, "maven-central-release")
central_text = ET.tostring(central, encoding="unicode")
for required in ("requireActiveProfile", "include-rust-native-artifacts", "centralBaseUrl"):
    if required not in central_text:
        raise SystemExit(f"Central profile is missing {required}")

release_profiles = root_pom.findtext(".//m:plugin[m:artifactId='maven-release-plugin']/m:configuration/m:releaseProfiles", namespaces=namespace)
if release_profiles is None or "maven-central-release" in release_profiles:
    raise SystemExit("release:perform must not activate maven-central-release")
PY

python3 - "${repo_dir}/.github/workflows/github-binaries-release.yml" "${repo_dir}/pkg/ami/marketplace/packer.json" <<'PY'
import json
import pathlib
import sys

workflow_path = pathlib.Path(sys.argv[1])
packer_path = pathlib.Path(sys.argv[2])
workflow = workflow_path.read_text()

for required in (
    "execution_mode:",
    "default: package-only",
    "core/target/downloaded-rust-artifacts",
    "stage-rust-native-artifacts.sh",
    "include-rust-native-artifacts",
    "mvn -B -pl core -am",
    "verify-rust-native-jar.sh",
    "verify-rust-native-runtime-archive.sh",
    "Reject an aggregate build without -DskipNative",
    "Verify aggregate-to-normal transition",
    "Reject a forged Central aggregate marker",
    "rust-native-libs",
    "third-party-licenses",
    "LINUX_RELEASE_ATTEMPT:",
    "LINUX_EVIDENCE_ATTEMPT:",
    "WINDOWS_RELEASE_ATTEMPT:",
    "WINDOWS_EVIDENCE_ATTEMPT:",
    "publish-github:",
    "publish-ami:",
    "github.event_name == 'push'",
    "startsWith(github.ref, 'refs/tags/')",
    "FORCE_DEREGISTER=false",
    "FORCE_DELETE_SNAPSHOT=false",
):
    if required not in workflow:
        raise SystemExit(f"release workflow is missing {required}")

if "path: core/target/classes/io/questdb/bin" in workflow:
    raise SystemExit("workflow downloads raw Rust artifacts into target/classes")
if "mvn clean install -DskipTests -f java-questdb-client/pom.xml" in workflow:
    raise SystemExit("workflow clean-installs the client outside the root reactor")
if "gh release upload \"${tag_name}\" artifacts/*.gz" in workflow:
    raise SystemExit("workflow uses unchecked glob GitHub release uploads")
if "  release:\n" in workflow:
    raise SystemExit("workflow retains the combined release job")

for artifact_name in (
    "rust-linux-x64",
    "rust-linux-arm64",
    "rust-macos-arm64",
    "rust-windows",
    "rust-native-libs",
    "third-party-licenses",
    "release-linux",
    "release-windows",
):
    start = workflow.find(f"name: {artifact_name}")
    if start == -1:
        raise SystemExit(f"workflow does not upload {artifact_name}")
    next_step = workflow.find("      - ", start + 1)
    section = workflow[start:next_step if next_step != -1 else len(workflow)]
    if "overwrite: true" not in section:
        raise SystemExit(f"workflow upload {artifact_name} is not overwrite-safe")

packer = json.loads(packer_path.read_text())
builder = packer["builders"][0]
if packer["variables"].get("force_deregister") != "false" or packer["variables"].get("force_delete_snapshot") != "false":
    raise SystemExit("release Packer defaults must not deregister AMIs or delete snapshots")
if builder.get("force_deregister") != "{{user `force_deregister`}}" or builder.get("force_delete_snapshot") != "{{user `force_delete_snapshot`}}":
    raise SystemExit("Packer builder does not use the non-destructive release force flags")
PY

printf 'native release packaging script checks passed\n'
