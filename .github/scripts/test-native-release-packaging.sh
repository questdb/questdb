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
readonly central_bundle_verifier="${script_dir}/verify-central-bundle.py"
readonly license_generator="${repo_dir}/ci/generate_third_party_licenses.sh"
temp_dir="$(mktemp -d)"
readonly temp_dir
central_endpoint_pid=""

cleanup() {
    if [[ -n "${central_endpoint_pid}" ]]; then
        kill "${central_endpoint_pid}" 2>/dev/null || true
        wait "${central_endpoint_pid}" 2>/dev/null || true
    fi
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
        if mode == "tampered" and path.endswith("linux-aarch64/libquestdbr.so"):
            data = b"tampered-" + data
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
if mode == "tampered":
    payload = b"tampered-" + payload
if mode == "wrong-name":
    name = "questdb-test/lib/libquestdbr-wrong.so"
if mode == "wrong-path":
    name = "questdb-test/not-runtime/lib/" + pathlib.PurePosixPath(source).name
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
[[ -x "${central_bundle_verifier}" ]] || fail "Central bundle verifier is missing or not executable"
[[ -x "${license_generator}" ]] || fail "third-party license generator is missing or not executable"

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

for mode in valid missing renamed extra duplicate empty tampered; do
    jar_path="${temp_dir}/${mode}.jar"
    create_jar "${jar_path}" "${mode}" "${valid_stage}"
    if [[ "${mode}" == valid ]]; then
        "${jar_verifier}" "${jar_path}" "${valid_stage}" > "${temp_dir}/jar-${mode}.out"
    else
        assert_failure "jar-${mode}" "${jar_verifier}" "${jar_path}" "${valid_stage}"
    fi
done
grep -F 'checksum mismatch for Rust jar entry: io/questdb/bin/linux-aarch64/libquestdbr.so' "${temp_dir}/jar-tampered.out" > /dev/null \
    || fail "jar verifier did not report the tampered Rust library as a checksum mismatch"

for mode in valid empty wrong-name wrong-path duplicate tampered; do
    archive_path="${temp_dir}/runtime-${mode}.tar.gz"
    create_runtime_archive "${archive_path}" linux-x86-64 "${mode}" "${valid_stage}"
    if [[ "${mode}" == valid ]]; then
        "${runtime_verifier}" "${archive_path}" linux-x86-64 "${valid_stage}" > "${temp_dir}/runtime-${mode}.out"
    else
        assert_failure "runtime-${mode}" "${runtime_verifier}" "${archive_path}" linux-x86-64 "${valid_stage}"
    fi
done
grep -F 'runtime checksum mismatch for questdb-test/lib/libquestdbr.so' "${temp_dir}/runtime-tampered.out" > /dev/null \
    || fail "runtime verifier did not report the tampered Rust library as a checksum mismatch"

create_incomplete_central_bundle() {
    local bundle_path="$1"
    local is_duplicate="$2"

    python3 - "${bundle_path}" "${is_duplicate}" <<'PY'
import sys
import zipfile

bundle_path = sys.argv[1]
is_duplicate = sys.argv[2] == "duplicate"
version = "9.9.9"
base = f"org/questdb/questdb/{version}/"
artifacts = (
    f"questdb-{version}.pom",
    f"questdb-{version}.jar",
    f"questdb-{version}-sources.jar",
    f"questdb-{version}-javadoc.jar",
    f"questdb-{version}.zip",
)
with zipfile.ZipFile(bundle_path, "w") as bundle:
    for artifact in artifacts:
        bundle.writestr(base + artifact, b"fixture")
    if is_duplicate:
        bundle.writestr(base + artifacts[0], b"fixture")
PY
}

missing_sidecar_bundle="${temp_dir}/central-missing-sidecar.zip"
create_incomplete_central_bundle "${missing_sidecar_bundle}" false
assert_failure central-missing-sidecar \
    "${central_bundle_verifier}" "${missing_sidecar_bundle}" "${temp_dir}/valid.jar" "${valid_stage}" --version 9.9.9
grep -F 'Central bundle entries differ' "${temp_dir}/central-missing-sidecar.out" > /dev/null \
    || fail "Central verifier did not reject the bundle with missing sidecars through the allowlist check"

duplicate_central_bundle="${temp_dir}/central-duplicate.zip"
create_incomplete_central_bundle "${duplicate_central_bundle}" duplicate
assert_failure central-duplicate-entry \
    "${central_bundle_verifier}" "${duplicate_central_bundle}" "${temp_dir}/valid.jar" "${valid_stage}" --version 9.9.9
grep -F 'Central bundle entries differ' "${temp_dir}/central-duplicate-entry.out" > /dev/null \
    || fail "Central verifier did not reject the bundle with a duplicate entry through the allowlist check"

valid_central_bundle="${temp_dir}/central-valid.zip"
python3 - "${valid_central_bundle}" "${temp_dir}/valid.jar" <<'PY'
import sys
import zipfile

bundle_path, main_jar = sys.argv[1:]
version = "9.9.9"
base = f"org/questdb/questdb/{version}/"
artifacts = {
    f"questdb-{version}.pom": b"<project><version>9.9.9</version></project>",
    f"questdb-{version}.jar": open(main_jar, "rb").read(),
    f"questdb-{version}-sources.jar": b"sources",
    f"questdb-{version}-javadoc.jar": b"javadocs",
    f"questdb-{version}.zip": b"web-console",
}
with zipfile.ZipFile(bundle_path, "w") as bundle:
    for artifact, payload in artifacts.items():
        bundle.writestr(base + artifact, payload)
        for suffix in (".asc", ".md5", ".sha1", ".sha256", ".sha512"):
            bundle.writestr(base + artifact + suffix, suffix.encode())
PY
"${central_bundle_verifier}" "${valid_central_bundle}" "${temp_dir}/valid.jar" "${valid_stage}" --version 9.9.9 > "${temp_dir}/central-valid.out"

verify_cargo_deny_checksum_guard() {
    local fixture_root="${temp_dir}/cargo-deny-fixture"
    local fake_bin="${fixture_root}/bin"
    local archive_path="${fixture_root}/untrusted-cargo-deny.tar.gz"
    local install_marker="${fixture_root}/install-called"

    mkdir -p "${fixture_root}/ci" "${fixture_root}/core/rust/qdbr" "${fake_bin}"
    cp "${license_generator}" "${fixture_root}/ci/generate_third_party_licenses.sh"
    python3 - "${archive_path}" <<'PY'
import io
import sys
import tarfile

with tarfile.open(sys.argv[1], "w:gz") as archive:
    payload = b"#!/usr/bin/env bash\\nexit 0\\n"
    member = tarfile.TarInfo("cargo-deny-0.19.8/cargo-deny")
    member.mode = 0o755
    member.size = len(payload)
    archive.addfile(member, io.BytesIO(payload))
PY
    cat > "${fake_bin}/cargo" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
    cat > "${fake_bin}/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-fsSL" && "${2:-}" == "--output" ]]; then
    cp "${FAKE_CARGO_DENY_ARCHIVE:?}" "$3"
else
    cat "${FAKE_CARGO_DENY_ARCHIVE:?}"
fi
EOF
    cat > "${fake_bin}/install" <<'EOF'
#!/usr/bin/env bash
: > "${FAKE_CARGO_DENY_INSTALL_MARKER:?}"
EOF
    chmod +x "${fake_bin}/cargo" "${fake_bin}/curl" "${fake_bin}/install"

    assert_failure cargo-deny-corrupt-archive bash -c "PATH='${fake_bin}:/usr/bin:/bin' FAKE_CARGO_DENY_ARCHIVE='${archive_path}' FAKE_CARGO_DENY_INSTALL_MARKER='${install_marker}' CARGO_DENY_VERSION=0.19.8 '${fixture_root}/ci/generate_third_party_licenses.sh'"
    [[ ! -e "${install_marker}" ]] || fail "cargo-deny installer ran after a checksum mismatch"
}

verify_cargo_deny_checksum_guard

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
normal_text = ET.tostring(normal, encoding="unicode")
if "process-resources" not in normal_text:
    raise SystemExit("normal build does not remove stale Rust natives at process-resources")
if "remove-rust-cli-binaries" not in normal_text or "**/pm_*" not in normal_text:
    raise SystemExit("normal build does not remove the Rust CLI binaries after the Rust build")

jar_plugin = core_pom.find(".//m:build/m:plugins/m:plugin[m:artifactId='maven-jar-plugin']", namespace)
if jar_plugin is None or "io/questdb/bin/**/pm_*" not in ET.tostring(jar_plugin, encoding="unicode"):
    raise SystemExit("maven-jar-plugin does not exclude the Rust CLI binaries")

aggregate = profile(core_pom, "include-rust-native-artifacts")
aggregate_text = ET.tostring(aggregate, encoding="unicode")
for required in ("skipNative", "process-resources", "verify-rust-native-jar.sh"):
    if required not in aggregate_text:
        raise SystemExit(f"aggregate profile is missing {required}")
if "maven-clean-plugin" in aggregate_text:
    raise SystemExit("aggregate profile must not bind maven-clean-plugin")

central = profile(core_pom, "maven-central-release")
central_text = ET.tostring(central, encoding="unicode")
for required in ("requireActiveProfile", "include-rust-native-artifacts", "centralBaseUrl", "requireReleaseDeps", "onlyWhenRelease"):
    if required not in central_text:
        raise SystemExit(f"Central profile is missing {required}")

release_profiles = root_pom.findtext(".//m:plugin[m:artifactId='maven-release-plugin']/m:configuration/m:releaseProfiles", namespaces=namespace)
if release_profiles is None or "maven-central-release" in release_profiles:
    raise SystemExit("release:perform must not activate maven-central-release")
if "release-preparation-safety" not in release_profiles:
    raise SystemExit("release lifecycle must reject external SNAPSHOT dependencies before tagging")
PY

python3 - "${repo_dir}/.github/workflows/github-binaries-release.yml" "${repo_dir}/pkg/ami/marketplace/packer.json" "${repo_dir}/pkg/ami/marketplace/Makefile" "${repo_dir}/.github/workflows/release_website.yml" <<'PY'
import json
import pathlib
import sys

import yaml

workflow_path = pathlib.Path(sys.argv[1])
packer_path = pathlib.Path(sys.argv[2])
makefile_path = pathlib.Path(sys.argv[3])
website_workflow_path = pathlib.Path(sys.argv[4])
workflow = workflow_path.read_text()
workflow_document = yaml.load(workflow, Loader=yaml.BaseLoader)
if not isinstance(workflow_document, dict):
    raise SystemExit("release workflow is not a mapping")
jobs = workflow_document.get("jobs")
if not isinstance(jobs, dict):
    raise SystemExit("release workflow has no jobs mapping")

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
    "Reject Central deploy without the aggregate profile",
    "Trigger the questdb.io rebuild",
    "gh workflow run release_website.yml",
    "/usr/bin/packer version",
    "PACKER_BIN=/usr/bin/packer",
    "test-native-release-packaging.sh",
    "RUN_MAVEN_LIFECYCLE_TESTS=1",
    "rust-native-libs",
    "third-party-licenses",
    "LINUX_RELEASE_ATTEMPT:",
    "LINUX_EVIDENCE_ATTEMPT:",
    "WINDOWS_RELEASE_ATTEMPT:",
    "WINDOWS_EVIDENCE_ATTEMPT:",
    "rust_versions",
    "publish-github:",
    "publish-ami:",
    "github.event_name == 'push'",
    "startsWith(github.ref, 'refs/tags/')",
    "publish-github-release-assets.sh",
    "publish-ami-release.sh",
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

for job_name in ("publish-github", "publish-ami"):
    job = jobs.get(job_name)
    if not isinstance(job, dict):
        raise SystemExit(f"release workflow has no {job_name} job")
    condition = job.get("if")
    if not isinstance(condition, str) or "github.event_name == 'push'" not in condition or "startsWith(github.ref, 'refs/tags/')" not in condition:
        raise SystemExit(f"{job_name} does not have the exact tag-push publication guard")

github_steps = jobs["publish-github"].get("steps", [])
if not isinstance(github_steps, list):
    raise SystemExit("GitHub publication job has no steps")
github_checkout_index = next((index for index, step in enumerate(github_steps) if isinstance(step, dict) and step.get("uses") == "actions/checkout@v5"), None)
github_helper_index = next((index for index, step in enumerate(github_steps) if isinstance(step, dict) and "publish-github-release-assets.sh" in str(step.get("run", ""))), None)
if github_checkout_index is None or github_helper_index is None or github_checkout_index >= github_helper_index:
    raise SystemExit("GitHub publication must check out the helper before invoking it")
github_website_index = next((index for index, step in enumerate(github_steps) if isinstance(step, dict) and "gh workflow run release_website.yml" in str(step.get("run", ""))), None)
if github_website_index is None or github_website_index <= github_helper_index:
    raise SystemExit("GitHub publication must dispatch the questdb.io rebuild after publishing the release assets")
github_permissions = jobs["publish-github"].get("permissions")
if not isinstance(github_permissions, dict) or github_permissions.get("contents") != "write" or github_permissions.get("actions") != "write":
    raise SystemExit("GitHub publication needs contents: write and actions: write to publish assets and dispatch the website rebuild")

website_workflow = yaml.load(website_workflow_path.read_text(), Loader=yaml.BaseLoader)
website_triggers = website_workflow.get("on") if isinstance(website_workflow, dict) else None
if not isinstance(website_triggers, dict) or "workflow_dispatch" not in website_triggers:
    raise SystemExit("release_website.yml has no workflow_dispatch trigger, so the Actions-token release publication cannot start it")

ami_job = jobs["publish-ami"]
ami_env = ami_job.get("env")
if not isinstance(ami_env, dict) or set(("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION")) - set(ami_env):
    raise SystemExit("AMI preflight does not receive job-scoped AWS credentials and a region")
if "PACKER_VERSION=1.10.0" not in workflow or '"packer=${PACKER_VERSION}-1"' not in workflow:
    raise SystemExit("release workflow does not pin the Packer package version")

windows_steps = jobs.get("package-windows", {}).get("steps", [])
provenance_step = next((step for step in windows_steps if isinstance(step, dict) and step.get("name") == "Write package provenance"), None)
if not isinstance(provenance_step, dict) or provenance_step.get("env", {}).get("RAW_LINUX_X64_ATTEMPT") != "${{ needs.package-linux.outputs.raw-linux-x64-producer-attempt }}":
    raise SystemExit("provenance must retain the raw Linux producer attempt")

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

makefile = makefile_path.read_text()
if "PACKER_AMAZON_PLUGIN_VERSION ?= 1.3.9" not in makefile or "plugins install github.com/hashicorp/amazon $(PACKER_AMAZON_PLUGIN_VERSION)" not in makefile:
    raise SystemExit("Packer Amazon plugin version is not pinned")
PY

verify_github_publication_recovery() {
    local fixture_root="${temp_dir}/github-publication-fixture"
    local fake_bin="${fixture_root}/bin"
    local source_root="${fixture_root}/source"
    local job_workspace="${fixture_root}/job-workspace"
    local release_artifacts="${fixture_root}/release-artifacts"
    local call_log="${fixture_root}/calls.log"

    mkdir -p "${release_artifacts}/linux" "${release_artifacts}/windows" "${fixture_root}/assets" "${fake_bin}" "${source_root}/.github/scripts"
    printf 'linux archive\n' > "${release_artifacts}/linux/questdb-linux.tar.gz"
    printf 'windows archive\n' > "${release_artifacts}/windows/questdb-windows.tar.gz"
    cp "${release_artifacts}/linux/questdb-linux.tar.gz" "${fixture_root}/assets/"
    cp "${release_artifacts}/windows/questdb-windows.tar.gz" "${fixture_root}/assets/"
    cp "${script_dir}/publish-github-release-assets.sh" "${source_root}/.github/scripts/"
    cat > "${fake_bin}/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
    "release view")
        if [[ "${GH_FIXTURE_MODE:?}" != "absent" ]]; then
            printf '%s\n' questdb-linux.tar.gz questdb-windows.tar.gz
        fi
        ;;
    "release download")
        pattern=""
        destination=""
        while [[ "$#" -gt 0 ]]; do
            case "$1" in
                --pattern) pattern="$2"; shift 2 ;;
                --dir) destination="$2"; shift 2 ;;
                *) shift ;;
            esac
        done
        mkdir -p "${destination}"
        if [[ "${GH_FIXTURE_MODE:?}" == "mismatch" ]]; then
            printf 'mismatched archive\n' > "${destination}/${pattern}"
        else
            cp "${GH_FIXTURE_ARCHIVES:?}/${pattern}" "${destination}/${pattern}"
        fi
        ;;
    "release upload") printf 'upload %s\n' "$4" >> "${GH_FIXTURE_CALL_LOG:?}" ;;
    "release edit") printf 'edit\n' >> "${GH_FIXTURE_CALL_LOG:?}" ;;
    *) echo "unexpected gh invocation: $*" >&2; exit 1 ;;
esac
EOF
    chmod +x "${fake_bin}/gh"

    run_github_fixture() {
        local mode="$1"

        rm -rf "${job_workspace}"
        mkdir -p "${job_workspace}/artifacts/linux" "${job_workspace}/artifacts/windows"
        cp "${release_artifacts}/linux/questdb-linux.tar.gz" "${job_workspace}/artifacts/linux/"
        cp "${release_artifacts}/windows/questdb-windows.tar.gz" "${job_workspace}/artifacts/windows/"
        : > "${call_log}"
        assert_failure github-helper-without-checkout bash -c "cd '${job_workspace}' && .github/scripts/publish-github-release-assets.sh 9.9.9 artifacts/linux artifacts/windows"
        cp -a "${source_root}/." "${job_workspace}/"
        if [[ "${mode}" == "mismatch" ]]; then
            assert_failure github-asset-mismatch bash -c "cd '${job_workspace}' && PATH='${fake_bin}:/usr/bin:/bin' GH_FIXTURE_MODE='${mode}' GH_FIXTURE_ARCHIVES='${fixture_root}/assets' GH_FIXTURE_CALL_LOG='${call_log}' .github/scripts/publish-github-release-assets.sh 9.9.9 artifacts/linux artifacts/windows"
        else
            (
                cd "${job_workspace}"
                PATH="${fake_bin}:/usr/bin:/bin" \
                    GH_FIXTURE_MODE="${mode}" \
                    GH_FIXTURE_ARCHIVES="${fixture_root}/assets" \
                    GH_FIXTURE_CALL_LOG="${call_log}" \
                    .github/scripts/publish-github-release-assets.sh 9.9.9 artifacts/linux artifacts/windows
            )
        fi
    }

    run_github_fixture absent
    [[ "$(grep -c '^upload ' "${call_log}")" == 2 ]] || fail "absent GitHub assets were not uploaded"
    [[ "$(grep -c '^edit$' "${call_log}")" == 1 ]] || fail "GitHub release was not finalized after uploads"

    run_github_fixture equal
    [[ "$(grep -c '^upload ' "${call_log}" || true)" == 0 ]] || fail "checksum-equal GitHub assets were unexpectedly uploaded"
    [[ "$(grep -c '^edit$' "${call_log}" || true)" == 1 ]] || fail "checksum-equal GitHub release was not finalized exactly once"

    run_github_fixture mismatch
    [[ ! -s "${call_log}" ]] || fail "mismatched GitHub asset reached an external side effect"
}

verify_ami_publication_recovery() {
    local fixture_root="${temp_dir}/ami-publication-fixture"
    local fake_bin="${fixture_root}/bin"
    local call_log="${fixture_root}/calls.log"
    local regions

    mkdir -p "${fake_bin}"
    cat > "${fake_bin}/aws" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
: "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}" "${AWS_DEFAULT_REGION:?}"
printf 'aws %s\n' "$*" >> "${AMI_FIXTURE_CALL_LOG:?}"
if [[ "$2" == "describe-regions" ]]; then
    printf 'eu-west-1\tus-west-2\n'
    exit 0
fi
if [[ "$2" == "describe-images" ]]; then
    region=""
    while [[ "$#" -gt 0 ]]; do
        if [[ "$1" == "--region" ]]; then
            region="$2"
            break
        fi
        shift
    done
    if [[ "${AMI_FIXTURE_MODE:?}" == "duplicate" && "${region}" == "us-west-2" ]]; then
        printf '1\n'
    else
        printf '0\n'
    fi
    exit 0
fi
echo "unexpected aws invocation: $*" >&2
exit 1
EOF
    cat > "${fake_bin}/make" <<'EOF'
#!/usr/bin/env bash
printf 'make %s\n' "$*" >> "${AMI_FIXTURE_CALL_LOG:?}"
EOF
    chmod +x "${fake_bin}/aws" "${fake_bin}/make"

    : > "${call_log}"
    assert_failure ami-duplicate-preflight bash -c "PATH='${fake_bin}:/usr/bin:/bin' AWS_ACCESS_KEY_ID=fixture AWS_SECRET_ACCESS_KEY=fixture AWS_DEFAULT_REGION=eu-west-1 AMI_FIXTURE_MODE=duplicate AMI_FIXTURE_CALL_LOG='${call_log}' '${script_dir}/publish-ami-release.sh' preflight 9.9.9"
    [[ ! -s "${call_log}" || -z "$(grep '^make ' "${call_log}" || true)" ]] || fail "duplicate AMI preflight invoked Packer through make"
    grep -F -- '--region eu-west-1' "${call_log}" > /dev/null || fail "AMI preflight did not inspect the source region"
    grep -F -- '--region us-west-2' "${call_log}" > /dev/null || fail "AMI preflight did not inspect a non-default destination region"

    : > "${call_log}"
    regions="$(PATH="${fake_bin}:/usr/bin:/bin" AWS_ACCESS_KEY_ID=fixture AWS_SECRET_ACCESS_KEY=fixture AWS_DEFAULT_REGION=eu-west-1 AMI_FIXTURE_MODE=absent AMI_FIXTURE_CALL_LOG="${call_log}" "${script_dir}/publish-ami-release.sh" preflight 9.9.9)"
    [[ "${regions}" == "eu-west-1,us-west-2" ]] || fail "AMI preflight returned unexpected regions: ${regions}"
    PATH="${fake_bin}:/usr/bin:/bin" AWS_ACCESS_KEY_ID=fixture AWS_SECRET_ACCESS_KEY=fixture AWS_DEFAULT_REGION=eu-west-1 AMI_FIXTURE_MODE=absent AMI_FIXTURE_CALL_LOG="${call_log}" "${script_dir}/publish-ami-release.sh" publish 9.9.9 "${regions}"
    grep -F 'make install_aws_plugin' "${call_log}" > /dev/null || fail "AMI publication did not install the pinned plugin"
    grep -F 'make build_release AMI_REGIONS=eu-west-1,us-west-2 QUESTDB_VERSION=9.9.9 FORCE_DEREGISTER=false FORCE_DELETE_SNAPSHOT=false' "${call_log}" > /dev/null \
        || fail "AMI publication did not use non-destructive Packer arguments"
    grep -F 'make install_aws_plugin packer=packer' "${call_log}" > /dev/null \
        || fail "AMI publication did not fall back to the PATH Packer without PACKER_BIN"

    : > "${call_log}"
    PATH="${fake_bin}:/usr/bin:/bin" AWS_ACCESS_KEY_ID=fixture AWS_SECRET_ACCESS_KEY=fixture AWS_DEFAULT_REGION=eu-west-1 AMI_FIXTURE_MODE=absent AMI_FIXTURE_CALL_LOG="${call_log}" PACKER_BIN=/usr/bin/packer-fixture "${script_dir}/publish-ami-release.sh" publish 9.9.9 "${regions}"
    grep -F 'make install_aws_plugin packer=/usr/bin/packer-fixture' "${call_log}" > /dev/null \
        || fail "AMI publication did not install the plugin with the pinned Packer binary"
    grep -F 'make build_release AMI_REGIONS=eu-west-1,us-west-2 QUESTDB_VERSION=9.9.9 FORCE_DEREGISTER=false FORCE_DELETE_SNAPSHOT=false packer=/usr/bin/packer-fixture' "${call_log}" > /dev/null \
        || fail "AMI publication did not build with the pinned Packer binary"
}

verify_github_publication_recovery
verify_ami_publication_recovery

run_release_lifecycle_probes() {
    local lifecycle_root
    local release_root
    local endpoint_log
    local endpoint_port_file
    local endpoint_port
    local gpg_home
    local settings_xml
    local central_output
    local central_log
    local verified_jar
    local bundle
    local effective_pom
    local probe_root
    local probe_remote
    local probe_receives
    local probe_head_before
    local fixture_project_version
    local fixture_release_version
    local fixture_client_version
    local fixture_release_client_version
    local release_core_jar
    local probe_project_version
    local probe_snapshot_client_version

    if ! java -version 2>&1 | grep -Eq 'version "(2[5-9]|[3-9][0-9])\.'; then
        fail "RUN_MAVEN_LIFECYCLE_TESTS=1 requires JDK 25 or newer"
    fi

    lifecycle_root="${temp_dir}/lifecycle-root"
    rsync -a --delete \
        --exclude .git \
        --exclude target \
        --exclude docs/superpowers \
        "${repo_dir}/" "${lifecycle_root}/"

    cp -a "${valid_raw}" "${lifecycle_root}/raw-native-inputs"
    "${lifecycle_root}/.github/scripts/stage-rust-native-artifacts.sh" \
        "${lifecycle_root}/raw-native-inputs" \
        "${lifecycle_root}/core/target/native-libs" > "${temp_dir}/lifecycle-stage.out"

    mkdir -p "${lifecycle_root}/core/target/classes/io/questdb/bin/linux-x86-64" \
        "${lifecycle_root}/core/src/main/resources/io/questdb/bin/darwin-x86-64"
    printf 'stale-output\n' > "${lifecycle_root}/core/target/classes/io/questdb/bin/linux-x86-64/libquestdbr.so"
    printf 'stale-intel\n' > "${lifecycle_root}/core/src/main/resources/io/questdb/bin/darwin-x86-64/libquestdbr.dylib"

    (
        cd "${lifecycle_root}"
        mvn -B -pl core -am package \
            -DskipTests -Dmaven.test.skip=true -DskipNative \
            -P local-client,include-rust-native-artifacts
    ) > "${temp_dir}/aggregate-package.log" 2>&1

    local aggregate_rust_count
    aggregate_rust_count="$(find "${lifecycle_root}/core/target/classes/io/questdb/bin" -type f \( -name 'libquestdbr.so' -o -name 'libquestdbr.dylib' -o -name 'questdbr.dll' \) | wc -l | tr -d ' ')"
    [[ "${aggregate_rust_count}" == 4 ]] || fail "aggregate package did not contain exactly four Rust libraries"
    [[ ! -e "${lifecycle_root}/core/target/classes/io/questdb/bin/darwin-x86-64/libquestdbr.dylib" ]] || fail "aggregate package retained stale Intel macOS output"
    verified_jar="$(find "${lifecycle_root}/core/target" -maxdepth 1 -type f -name 'questdb-*.jar' ! -name '*-tests.jar' -print -quit)"
    [[ -n "${verified_jar}" ]] || fail "aggregate package did not produce a core jar"
    "${lifecycle_root}/.github/scripts/verify-rust-native-jar.sh" \
        "${verified_jar}" "${lifecycle_root}/core/target/native-libs" > "${temp_dir}/aggregate-jar.manifest"

    mkdir -p "${temp_dir}/cargo-sentinel"
    cat > "${temp_dir}/cargo-sentinel/cargo" <<'EOF'
#!/usr/bin/env bash
: > "${CARGO_SENTINEL_CALLED:?}"
exit 99
EOF
    chmod +x "${temp_dir}/cargo-sentinel/cargo"
    export CARGO_SENTINEL_CALLED="${temp_dir}/cargo-sentinel.called"
    assert_failure aggregate-without-skip bash -c "cd '${lifecycle_root}' && PATH='${temp_dir}/cargo-sentinel':\"\$PATH\" mvn -B -pl core -am compile -P local-client,include-rust-native-artifacts"
    [[ ! -e "${CARGO_SENTINEL_CALLED}" ]] || fail "aggregate validation invoked Cargo without -DskipNative"
    unset CARGO_SENTINEL_CALLED

    (
        cd "${lifecycle_root}"
        mvn -B -pl core -am compile -DskipTests -Dmaven.test.skip=true -P local-client
    ) > "${temp_dir}/normal-compile.log" 2>&1
    local normal_rust_count
    normal_rust_count="$(find "${lifecycle_root}/core/target/classes/io/questdb/bin" -type f \( -name 'libquestdbr.so' -o -name 'libquestdbr.dylib' -o -name 'questdbr.dll' \) | wc -l | tr -d ' ')"
    [[ "${normal_rust_count}" == 1 ]] || fail "normal compile did not transition to exactly one host Rust library"
    [[ -z "$(find "${lifecycle_root}/core/target/classes/io/questdb/bin" -type f -name 'pm_*')" ]] \
        || fail "normal compile left Rust CLI binaries (pm_*) in the packaged classpath"

    assert_failure central-without-aggregate bash -c "cd '${lifecycle_root}' && mvn -B -pl core -am validate -DskipNative -P maven-central-release"
    grep -F 'Profile "include-rust-native-artifacts" is not activated.' "${temp_dir}/central-without-aggregate.out" > /dev/null \
        || fail "Central deploy without the aggregate profile did not fail through RequireActiveProfile"

    read -r fixture_project_version fixture_client_version < <(python3 - "${lifecycle_root}/pom.xml" "${lifecycle_root}/core/pom.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET

namespace = {'m': 'http://maven.apache.org/POM/4.0.0'}
root = ET.parse(sys.argv[1]).getroot()
core = ET.parse(sys.argv[2]).getroot()
project_version = root.findtext('m:version', namespaces=namespace)
client_version = core.findtext('.//m:questdb.client.version', namespaces=namespace)
if not project_version or not client_version:
    raise SystemExit('fixture could not read the project or client version')
print(project_version, client_version)
PY
)
    fixture_release_version="${fixture_project_version%-SNAPSHOT}"
    fixture_release_client_version="$(curl -fsSL https://repo1.maven.org/maven2/org/questdb/questdb-client/maven-metadata.xml | python3 -c 'import sys, xml.etree.ElementTree as ET; version = ET.parse(sys.stdin).findtext("./versioning/release"); assert version and "SNAPSHOT" not in version; print(version)')"
    [[ -n "${fixture_release_version}" && -n "${fixture_release_client_version}" ]] \
        || fail "fixture could not derive release versions"

    release_root="${temp_dir}/central-release-root"
    cp -a "${lifecycle_root}" "${release_root}"
    python3 - "${release_root}/pom.xml" "${release_root}/core/pom.xml" "${fixture_project_version}" "${fixture_release_version}" "${fixture_client_version}" "${fixture_release_client_version}" <<'PY'
from pathlib import Path
import sys

root_pom, core_pom, project_version, release_version, client_version, release_client_version = sys.argv[1:]
for path in map(Path, (root_pom, core_pom)):
    text = path.read_text()
    text = text.replace(project_version, release_version)
    text = text.replace(client_version, release_client_version)
    if 'SNAPSHOT' in text:
        raise SystemExit(f'fixture left a SNAPSHOT value in {path}')
    path.write_text(text)
PY

    gpg_home="${temp_dir}/gnupg"
    mkdir -m 700 "${gpg_home}"
    gpg --batch --homedir "${gpg_home}" --pinentry-mode loopback --passphrase '' \
        --quick-generate-key 'Native release fixture <fixture@example.invalid>' rsa2048 sign 0 > "${temp_dir}/gpg-key.log" 2>&1
    settings_xml="${temp_dir}/settings.xml"
    cat > "${settings_xml}" <<'EOF'
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0">
  <servers>
    <server>
      <id>central</id>
      <username>fixture-user</username>
      <password>fixture-password</password>
    </server>
  </servers>
</settings>
EOF

    endpoint_log="${temp_dir}/central-requests.log"
    endpoint_port_file="${temp_dir}/central-port"
    python3 - "${endpoint_log}" "${endpoint_port_file}" <<'PY' &
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import sys

request_log = Path(sys.argv[1])
port_file = Path(sys.argv[2])

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        request_log.write_text(request_log.read_text() + f'GET {self.path}\n' if request_log.exists() else f'GET {self.path}\n')
        self.send_response(503)
        self.end_headers()
    def do_POST(self):
        request_log.write_text(request_log.read_text() + f'POST {self.path}\n' if request_log.exists() else f'POST {self.path}\n')
        self.send_response(503)
        self.end_headers()
    def log_message(self, *_):
        pass

server = HTTPServer(('127.0.0.1', 0), Handler)
port_file.write_text(str(server.server_port))
server.serve_forever()
PY
    central_endpoint_pid=$!
    for _ in $(seq 1 20); do
        [[ -s "${endpoint_port_file}" ]] && break
        sleep 1
    done
    endpoint_port="$(cat "${endpoint_port_file}")"
    central_output="${temp_dir}/central-output"
    central_log="${temp_dir}/central-deploy.log"
    (
        cd "${release_root}"
        GNUPGHOME="${gpg_home}" mvn -B -s "${settings_xml}" -pl core -am deploy \
            -DskipTests -Dmaven.test.skip=true -DskipNative -DskipPublishing=true \
            -Dcentral.base.url="http://127.0.0.1:${endpoint_port}" \
            -DoutputDirectory="${central_output}" \
            -P build-web-console,include-rust-native-artifacts,maven-central-release,release-preparation-safety
    ) > "${central_log}" 2>&1
    kill "${central_endpoint_pid}"
    wait "${central_endpoint_pid}" 2>/dev/null || true
    central_endpoint_pid=""
    [[ ! -s "${endpoint_log}" ]] || fail "safe Central fixture sent a request to its fail-closed endpoint"
    grep -Eiq 'skip.*publish|publish.*skip' "${central_log}" || fail "Central plugin did not report skipPublishing"
    bundle="$(find "${central_output}" -type f -name 'central-bundle.zip' -print -quit)"
    [[ -n "${bundle}" ]] || fail "Central plugin did not produce its local bundle"
    release_core_jar="${release_root}/core/target/questdb-${fixture_release_version}.jar"
    [[ -n "${release_core_jar}" ]] || fail "Central fixture did not produce a core jar"
    "${release_root}/.github/scripts/verify-central-bundle.py" \
        "${bundle}" "${release_core_jar}" "${release_root}/core/target/native-libs" \
        --version "${fixture_release_version}"
    local verifier_line central_line
    verifier_line="$(grep -n 'io/questdb/bin/windows-x86-64/questdbr.dll' "${central_log}" | head -1 | cut -d: -f1 || true)"
    central_line="$(grep -Ein 'skip.*publish|publish.*skip' "${central_log}" | tail -1 | cut -d: -f1 || true)"
    [[ -n "${verifier_line}" && -n "${central_line}" && "${verifier_line}" -lt "${central_line}" ]] \
        || fail "aggregate jar verification did not precede Central skipPublishing"

    effective_pom="${temp_dir}/release-effective-pom.xml"
    (
        cd "${lifecycle_root}"
        mvn -B -N help:effective-pom -Doutput="${effective_pom}"
    ) > "${temp_dir}/release-effective-pom.log" 2>&1
    python3 - "${effective_pom}" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
for plugin in root.findall('.//m:plugin', ns):
    if plugin.findtext('m:artifactId', namespaces=ns) != 'maven-release-plugin':
        continue
    config = plugin.find('m:configuration', ns)
    if config is None:
        continue
    text = ET.tostring(config, encoding='unicode')
    if 'maven-central-release' in text or 'build-web-console' not in text or 'release-preparation-safety' not in text:
        raise SystemExit('release-plugin profile configuration is unsafe')
    if config.findtext('m:preparationProfiles', namespaces=ns) != 'release-preparation-safety':
        raise SystemExit('release:prepare does not activate its external SNAPSHOT dependency guard')
    if any(element in text for element in ('<preparationGoals>', '<pushChanges>', '<resume>')):
        raise SystemExit('fixture requires release-plugin clean verify, pushChanges=true, and resume=true defaults')
    print('effective release plugin keeps Central inactive; preparation defaults remain clean verify, pushChanges=true, resume=true')
    break
else:
    raise SystemExit('configured maven-release-plugin missing from effective POM')
PY

    probe_root="${temp_dir}/release-prepare-probe"
    rsync -a --delete \
        --exclude .git \
        --exclude target \
        --exclude docs/superpowers \
        "${repo_dir}/" "${probe_root}/"
    probe_project_version="${fixture_release_version}-fixture-SNAPSHOT"
    probe_snapshot_client_version="${fixture_release_client_version}-fixture-SNAPSHOT"
    probe_remote="${temp_dir}/release-prepare-remote.git"
    probe_receives="${temp_dir}/release-prepare-receives.log"
    git init --bare "${probe_remote}" > /dev/null
    git --git-dir "${probe_remote}" symbolic-ref HEAD refs/heads/master
    cat > "${probe_remote}/hooks/pre-receive" <<EOF
#!/usr/bin/env bash
cat >> '${probe_receives}'
EOF
    chmod +x "${probe_remote}/hooks/pre-receive"
    # Point the root POM's SCM at the audited local remote before either probe
    # runs, so neither the negative nor the safe release:prepare can reach GitHub.
    python3 - "${probe_root}" "${probe_remote}" "${fixture_project_version}" "${probe_project_version}" "${fixture_client_version}" "${fixture_release_client_version}" <<'PY'
from pathlib import Path
import sys

repository, remote, project_version, probe_project_version, client_version, release_client_version = sys.argv[1:]
root = Path(repository) / 'pom.xml'
root_text = root.read_text()
root_text = root_text.replace('scm:git:https://github.com/questdb/questdb.git', f'scm:git:file://{remote}')
root_text = root_text.replace('https://github.com/questdb/questdb', f'file://{remote}')
root.write_text(root_text)
for path in Path(repository).rglob('pom.xml'):
    if 'java-questdb-client' in path.parts:
        continue
    text = path.read_text()
    text = text.replace(project_version, probe_project_version)
    text = text.replace(client_version, release_client_version)
    path.write_text(text)
PY
    (
        cd "${probe_root}"
        git init -b master > /dev/null
        git config user.name 'Native release fixture'
        git config user.email fixture@example.invalid
        git add .
        git commit -m 'fixture base' > /dev/null
        git remote add origin "${probe_remote}"
        git push origin master > /dev/null
    )
    : > "${probe_receives}"
    python3 - "${probe_root}/core/pom.xml" "${fixture_release_client_version}" "${probe_snapshot_client_version}" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
release_client_version, snapshot_client_version = sys.argv[2:]
path.write_text(path.read_text().replace(release_client_version, snapshot_client_version))
PY
    (
        cd "${probe_root}"
        git add core/pom.xml
        git commit -m 'inject external snapshot client' > /dev/null
    )
    : > "${probe_receives}"
    probe_head_before="$(git -C "${probe_root}" rev-parse HEAD)"
    assert_failure release-prepare-snapshot bash -c "cd '${probe_root}' && mvn -B -pl core -am release:prepare -DpreparationGoals=validate -DautoVersionSubmodules=true"
    grep -F "Can't release project due to non released dependencies" "${temp_dir}/release-prepare-snapshot.out" > /dev/null \
        || fail "release:prepare did not reject the external snapshot client"
    [[ "$(git -C "${probe_root}" rev-parse HEAD)" == "${probe_head_before}" ]] || fail "snapshot release:prepare created a local release commit"
    git -C "${probe_root}" diff --quiet || fail "snapshot release:prepare modified a tracked file"
    [[ -z "$(git -C "${probe_root}" tag -l)" ]] || fail "snapshot release:prepare created a local tag"
    [[ ! -s "${probe_receives}" ]] || fail "snapshot release:prepare pushed to the audited remote"
    git -C "${probe_root}" clean -fd > /dev/null
    printf 'snapshot release:prepare negative probe passed\n'

    python3 - "${probe_root}" "${probe_snapshot_client_version}" "${fixture_release_client_version}" <<'PY'
from pathlib import Path
import sys

repository, snapshot_client_version, release_client_version = sys.argv[1:]
for path in Path(repository).rglob('pom.xml'):
    if 'java-questdb-client' in path.parts:
        continue
    path.write_text(path.read_text().replace(snapshot_client_version, release_client_version))
PY
    if ! (
        cd "${probe_root}"
        git add -u
        git commit -m 'make release-plugin probe releasable' > /dev/null
        mvn -B release:prepare -DpreparationGoals=validate -DautoVersionSubmodules=true > "${temp_dir}/release-prepare-safe.log" 2>&1
        mvn -B release:perform -Dgoals=validate -DlocalCheckout=false > "${temp_dir}/release-perform-safe.log" 2>&1
    ); then
        cat "${temp_dir}/release-prepare-safe.log" >&2 || true
        cat "${temp_dir}/release-perform-safe.log" >&2 || true
        fail "safe release prepare/perform probe failed"
    fi
    grep -F 'BUILD SUCCESS' "${temp_dir}/release-perform-safe.log" > /dev/null \
        || fail "safe release:perform probe did not complete"
    if grep -Eq 'require-aggregated-rust-native-artifacts|Profile "include-rust-native-artifacts" is not activated' "${temp_dir}/release-perform-safe.log"; then
        fail "release:perform activated maven-central-release"
    fi
    [[ -n "$(git --git-dir "${probe_remote}" tag -l)" ]] || fail "safe release:prepare did not create a remote tag"

    printf 'Maven lifecycle, safe Central deploy, and release-plugin probes passed\n'
}

if [[ "${RUN_MAVEN_LIFECYCLE_TESTS:-0}" == "1" ]]; then
    run_release_lifecycle_probes
fi

printf 'native release packaging script checks passed\n'
