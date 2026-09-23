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
    "test-native-release-packaging.sh",
    "RUN_MAVEN_LIFECYCLE_TESTS=1",
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

    assert_failure forged-central-marker bash -c "cd '${lifecycle_root}' && mvn -B -pl core -am validate -DskipNative -Dis.rust.native.artifacts.aggregated=true -P maven-central-release"
    grep -F 'Profile "include-rust-native-artifacts" is not activated.' "${temp_dir}/forged-central-marker.out" > /dev/null \
        || fail "forged Central marker did not fail through RequireActiveProfile"

    release_root="${temp_dir}/central-release-root"
    cp -a "${lifecycle_root}" "${release_root}"
    python3 - "${release_root}/pom.xml" "${release_root}/core/pom.xml" <<'PY'
from pathlib import Path
import sys

for path in map(Path, sys.argv[1:]):
    text = path.read_text()
    text = text.replace('10.0.2-SNAPSHOT', '10.0.2')
    text = text.replace('1.3.10-SNAPSHOT', '1.3.8')
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
            -P build-web-console,include-rust-native-artifacts,maven-central-release
    ) > "${central_log}" 2>&1
    kill "${central_endpoint_pid}"
    wait "${central_endpoint_pid}" 2>/dev/null || true
    central_endpoint_pid=""
    [[ ! -s "${endpoint_log}" ]] || fail "safe Central fixture sent a request to its fail-closed endpoint"
    grep -Eiq 'skip.*publish|publish.*skip' "${central_log}" || fail "Central plugin did not report skipPublishing"
    bundle="$(find "${central_output}" -type f -name 'central-bundle.zip' -print -quit)"
    [[ -n "${bundle}" ]] || fail "Central plugin did not produce its local bundle"
    python3 - "${bundle}" "${release_root}/core/target/questdb-10.0.2.jar" "${release_root}/core/target/native-libs" <<'PY'
import hashlib
import io
import pathlib
import sys
import xml.etree.ElementTree as ET
import zipfile

bundle_path, main_jar, staged_root = map(pathlib.Path, sys.argv[1:])
version = '10.0.2'
base = f'org/questdb/questdb/{version}/'
artifacts = {
    f'questdb-{version}.pom',
    f'questdb-{version}.jar',
    f'questdb-{version}-sources.jar',
    f'questdb-{version}-javadoc.jar',
    f'questdb-{version}.zip',
}
sidecars = ('.asc', '.md5', '.sha1', '.sha256', '.sha512')
with zipfile.ZipFile(bundle_path) as bundle:
    files = [name for name in bundle.namelist() if not name.endswith('/')]
    if not files or any(not name.startswith(base) for name in files):
        raise SystemExit('Central bundle contains an unplanned coordinate')
    bases = set()
    for name in files:
        filename = name.removeprefix(base)
        artifact = next((item for item in artifacts if filename == item or filename.startswith(item + '.')), None)
        if artifact is None:
            raise SystemExit(f'Central bundle contains an unexpected artifact or sidecar: {name}')
        suffix = filename[len(artifact):]
        if suffix and suffix not in sidecars:
            raise SystemExit(f'Central bundle contains an unexpected sidecar: {name}')
        bases.add(artifact)
    if bases != artifacts:
        raise SystemExit(f'Central bundle allowlist mismatch: {bases}')
    if any('-tests.jar' in name for name in files):
        raise SystemExit('Central bundle contains a tests jar')
    pom = ET.fromstring(bundle.read(base + f'questdb-{version}.pom'))
    if any('SNAPSHOT' in (node.text or '') for node in pom.iter()):
        raise SystemExit('Central bundled POM contains a SNAPSHOT dependency')
    bundled_jar = bundle.read(base + f'questdb-{version}.jar')
    if bundled_jar != main_jar.read_bytes():
        raise SystemExit('Central bundled core jar differs from the verified core jar')
    expected = {
        'io/questdb/bin/linux-x86-64/libquestdbr.so',
        'io/questdb/bin/linux-aarch64/libquestdbr.so',
        'io/questdb/bin/darwin-aarch64/libquestdbr.dylib',
        'io/questdb/bin/windows-x86-64/questdbr.dll',
    }
    with zipfile.ZipFile(io.BytesIO(bundled_jar)) as jar:
        actual = [entry.filename for entry in jar.infolist() if entry.filename in expected]
        if set(actual) != expected or len(actual) != len(expected):
            raise SystemExit('Central bundled core jar does not contain exactly four Rust entries')
        for entry in actual:
            digest = hashlib.sha256(jar.read(entry)).hexdigest()
            source = hashlib.sha256((staged_root / entry).read_bytes()).hexdigest()
            if digest != source:
                raise SystemExit(f'Central bundled native checksum mismatch: {entry}')
print('Central bundle allowlist, POM, jar identity, and native checks passed')
PY
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
    if plugin.findtext('m:artifactId', namespaces=ns) == 'maven-release-plugin':
        config = plugin.find('m:configuration', ns)
        text = ET.tostring(config, encoding='unicode')
        if 'maven-central-release' in text or 'build-web-console' not in text:
            raise SystemExit('release:perform profile configuration is unsafe')
        if any(element in text for element in ('<preparationGoals>', '<pushChanges>', '<resume>')):
            raise SystemExit('fixture requires release-plugin clean verify, pushChanges=true, and resume=true defaults')
        print('effective release plugin keeps Central inactive; preparation defaults remain clean verify, pushChanges=true, resume=true')
        break
else:
    raise SystemExit('maven-release-plugin missing from effective POM')
PY

    probe_root="${temp_dir}/release-prepare-probe"
    rsync -a --delete \
        --exclude .git \
        --exclude target \
        --exclude docs/superpowers \
        "${repo_dir}/" "${probe_root}/"
    probe_remote="${temp_dir}/release-prepare-remote.git"
    probe_receives="${temp_dir}/release-prepare-receives.log"
    git init --bare "${probe_remote}" > /dev/null
    cat > "${probe_remote}/hooks/pre-receive" <<EOF
#!/usr/bin/env bash
cat >> '${probe_receives}'
EOF
    chmod +x "${probe_remote}/hooks/pre-receive"
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
    python3 - "${probe_root}/core/pom.xml" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
text = path.read_text().replace('1.3.10-SNAPSHOT', '1.3.999-SNAPSHOT')
path.write_text(text)
PY
    (
        cd "${probe_root}"
        git add core/pom.xml
        git commit -m 'inject external snapshot client' > /dev/null
    )
    : > "${probe_receives}"
    assert_failure release-prepare-snapshot bash -c "cd '${probe_root}' && mvn -B -pl core -am release:prepare -DpreparationGoals=validate -DautoVersionSubmodules=true"
    grep -qi 'snapshot' "${temp_dir}/release-prepare-snapshot.out" || fail "release:prepare did not reject the external snapshot client"
    [[ -z "$(git -C "${probe_root}" status --porcelain)" ]] || fail "snapshot release:prepare changed the local repository"
    [[ -z "$(git -C "${probe_root}" tag -l)" ]] || fail "snapshot release:prepare created a local tag"
    [[ ! -s "${probe_receives}" ]] || fail "snapshot release:prepare pushed to the audited remote"

    python3 - "${probe_root}/pom.xml" "${probe_root}/core/pom.xml" "${probe_remote}" <<'PY'
from pathlib import Path
import sys

root, core, remote = map(Path, sys.argv[1:])
root_text = root.read_text()
root_text = root_text.replace('scm:git:https://github.com/questdb/questdb.git', f'scm:git:file://{remote}')
root_text = root_text.replace('https://github.com/questdb/questdb', f'file://{remote}')
root.write_text(root_text)
core_text = core.read_text().replace('1.3.999-SNAPSHOT', '1.3.8')
core.write_text(core_text)
PY
    (
        cd "${probe_root}"
        git add pom.xml core/pom.xml
        git commit -m 'make release-plugin probe releasable' > /dev/null
        mvn -B release:prepare -DpreparationGoals=validate -DautoVersionSubmodules=true > "${temp_dir}/release-prepare-safe.log" 2>&1
        mvn -B release:perform -Dgoals=validate -DlocalCheckout=true > "${temp_dir}/release-perform-safe.log" 2>&1
    )
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
