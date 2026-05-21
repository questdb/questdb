#!/usr/bin/env bash
#
# Build QuestDB's pgwire fuzz targets and run one of them with Jazzer's
# standalone binary.

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ci/run-pgwire-fuzz.sh [options] [-- <jazzer/libFuzzer args>]

Options:
  --target NAME          parse | session | startup | cleartext | servermain |
                         differential, or a full class name.
                         Default: parse
  --corpus DIR          Corpus directory to use. Defaults to the target's checked-in corpus.
  --time SECONDS        Set -max_total_time. Default: 300
  --runs N              Set -runs instead of -max_total_time.
  --keep-going N        Set --keep_going. Default: 25
  --artifact-dir DIR    Crash artifact directory. Default: core/target/pgwire-fuzz/artifacts
  --jazzer PATH         Path to an existing jazzer executable. Can also use JAZZER=PATH.
  --download-jazzer     Download Jazzer into core/target/jazzer if --jazzer/JAZZER is absent.
  --jazzer-version VER  Jazzer release tag for --download-jazzer. Default: v0.30.0
  --jazzer-sha256 SHA   Expected archive SHA-256 for --download-jazzer. Required
                         for versions without a checksum pinned in this script.
  --postgres-host HOST  PostgreSQL oracle host for --target differential.
                         Can also use QDB_PGWIRE_POSTGRES_HOST.
  --postgres-port PORT  PostgreSQL oracle port for --target differential.
                         Can also use QDB_PGWIRE_POSTGRES_PORT. Default: 5432.
  --no-build            Do not run Maven test-compile/classpath generation.
  --dry-run             Print the command instead of executing it.
  -h, --help            Show this help.

Examples:
  JAZZER=/opt/jazzer/jazzer ci/run-pgwire-fuzz.sh --target cleartext --time 60
  ci/run-pgwire-fuzz.sh --download-jazzer --target session --runs 10000
USAGE
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target="parse"
corpus=""
max_total_time="300"
runs=""
keep_going="25"
artifact_dir="$repo_root/core/target/pgwire-fuzz/artifacts"
jazzer="${JAZZER:-}"
jazzer_version="${JAZZER_VERSION:-v0.30.0}"
jazzer_sha256="${JAZZER_SHA256:-}"
download_jazzer=false
build=true
dry_run=false
postgres_host="${QDB_PGWIRE_POSTGRES_HOST:-}"
postgres_port="${QDB_PGWIRE_POSTGRES_PORT:-}"
extra_args=()

require_arg() {
  if [[ $# -lt 2 || -z "$2" ]]; then
    echo "Missing value for $1" >&2
    usage >&2
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) require_arg "$@"; target="$2"; shift 2 ;;
    --corpus) require_arg "$@"; corpus="$2"; shift 2 ;;
    --time) require_arg "$@"; max_total_time="$2"; runs=""; shift 2 ;;
    --runs) require_arg "$@"; runs="$2"; shift 2 ;;
    --keep-going) require_arg "$@"; keep_going="$2"; shift 2 ;;
    --artifact-dir) require_arg "$@"; artifact_dir="$2"; shift 2 ;;
    --jazzer) require_arg "$@"; jazzer="$2"; shift 2 ;;
    --download-jazzer) download_jazzer=true; shift ;;
    --jazzer-version) require_arg "$@"; jazzer_version="$2"; shift 2 ;;
    --jazzer-sha256) require_arg "$@"; jazzer_sha256="$2"; shift 2 ;;
    --postgres-host) require_arg "$@"; postgres_host="$2"; shift 2 ;;
    --postgres-port) require_arg "$@"; postgres_port="$2"; shift 2 ;;
    --no-build) build=false; shift ;;
    --dry-run) dry_run=true; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; extra_args+=("$@"); break ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

target_class=""
default_corpus=""
case "$target" in
  parse)
    target_class="io.questdb.test.cutlass.pgwire.PGParseMessageFuzz"
    default_corpus="$repo_root/core/src/test/resources/pgwire-corpus"
    ;;
  session)
    target_class="io.questdb.test.cutlass.pgwire.PGSessionFuzz"
    default_corpus="$repo_root/core/src/test/resources/pgwire-session-corpus"
    ;;
  startup)
    target_class="io.questdb.test.cutlass.pgwire.PGStartupSessionFuzz"
    default_corpus="$repo_root/core/src/test/resources/pgwire-startup-corpus"
    ;;
  cleartext)
    target_class="io.questdb.test.cutlass.pgwire.PGCleartextAuthFuzz"
    default_corpus="$repo_root/core/src/test/resources/pgwire-cleartext-auth-corpus"
    ;;
  servermain)
    target_class="io.questdb.test.cutlass.pgwire.PGServerMainFuzz"
    default_corpus="$repo_root/core/src/test/resources/pgwire-servermain-corpus"
    ;;
  differential)
    target_class="io.questdb.test.cutlass.pgwire.PGDifferentialFuzz"
    default_corpus="$repo_root/core/src/test/resources/pgwire-differential-corpus"
    ;;
  io.questdb.test.cutlass.pgwire.*)
    target_class="$target"
    ;;
  *)
    echo "Unknown target: $target" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ -z "$corpus" ]]; then
  corpus="$default_corpus"
fi
if [[ -n "$corpus" && ! -d "$corpus" ]]; then
  echo "Corpus directory does not exist: $corpus" >&2
  exit 1
fi

jazzer_dir="$repo_root/core/target/jazzer/$jazzer_version"

detect_jazzer_asset() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os:$arch" in
    Linux:x86_64|Linux:amd64) echo "jazzer-linux-x86-64.tar.gz" ;;
    Linux:aarch64|Linux:arm64) echo "jazzer-linux-arm64.tar.gz" ;;
    Darwin:*) echo "jazzer-macos.tar.gz" ;;
    MINGW*:*) echo "jazzer-windows.tar.gz" ;;
    MSYS*:*) echo "jazzer-windows.tar.gz" ;;
    CYGWIN*:*) echo "jazzer-windows.tar.gz" ;;
    *) echo "Unsupported host for Jazzer download: $os $arch" >&2; return 1 ;;
  esac
}

find_jazzer_executable() {
  local found
  if [[ -x "$jazzer_dir/jazzer" ]]; then
    echo "$jazzer_dir/jazzer"
    return 0
  fi
  if [[ -x "$jazzer_dir/jazzer.exe" ]]; then
    echo "$jazzer_dir/jazzer.exe"
    return 0
  fi
  found="$(find "$jazzer_dir" -maxdepth 2 -type f \( -name jazzer -o -name jazzer.exe \) | head -n 1)"
  if [[ -n "$found" ]]; then
    chmod +x "$found" 2>/dev/null || true
    echo "$found"
    return 0
  fi
  return 1
}

expected_jazzer_sha256() {
  local asset="$1"
  if [[ -n "$jazzer_sha256" ]]; then
    echo "$jazzer_sha256"
    return 0
  fi
  if [[ "$jazzer_version" == "v0.30.0" ]]; then
    case "$asset" in
      jazzer-linux-arm64.tar.gz) echo "16636a4d3e98f1d3a7fcf59d4ab37f5be1d1ad6df9d94d5c3ca8de644838e369" ;;
      jazzer-linux-x86-64.tar.gz) echo "6eeaf0026d75599b07527d93b576593ce847cfca8b337a579971a5a9cdf792d0" ;;
      jazzer-macos.tar.gz) echo "6239e2b2b8d626c287fd8a50376315f00ef4310e3f29e96315cd6e60911563c8" ;;
      jazzer-windows.tar.gz) echo "959b04d6b773c901aa2a2e367b06841a346111eb76d3437dfb8b16b574091f61" ;;
      *) return 1 ;;
    esac
    return 0
  fi
  return 1
}

verify_sha256() {
  local actual expected path
  path="$1"
  expected="$2"
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$path" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$path" | awk '{print $1}')"
  else
    echo "No SHA-256 verifier found; install sha256sum or shasum." >&2
    exit 1
  fi
  if [[ "$actual" != "$expected" ]]; then
    echo "Jazzer archive checksum mismatch for $path" >&2
    echo "expected: $expected" >&2
    echo "actual:   $actual" >&2
    exit 1
  fi
}

if [[ -z "$jazzer" ]] && jazzer="$(find_jazzer_executable 2>/dev/null)"; then
  :
fi

if [[ -z "$jazzer" && "$download_jazzer" == true && "$dry_run" == true ]]; then
  jazzer="$jazzer_dir/jazzer"
fi

if [[ -z "$jazzer" && "$download_jazzer" == true ]]; then
  expected_sha256=""
  asset="$(detect_jazzer_asset)"
  if ! expected_sha256="$(expected_jazzer_sha256 "$asset")"; then
    echo "No pinned Jazzer checksum for $jazzer_version/$asset." >&2
    echo "Pass --jazzer-sha256 or JAZZER_SHA256 to download this version." >&2
    exit 1
  fi
  url="https://github.com/CodeIntelligenceTesting/jazzer/releases/download/$jazzer_version/$asset"
  mkdir -p "$jazzer_dir"
  echo "Downloading $url"
  curl -fL -sS "$url" -o "$jazzer_dir/$asset"
  verify_sha256 "$jazzer_dir/$asset" "$expected_sha256"
  tar -xzf "$jazzer_dir/$asset" -C "$jazzer_dir"
  jazzer="$(find_jazzer_executable)"
fi

if [[ -z "$jazzer" && "$dry_run" == true ]]; then
  jazzer="$jazzer_dir/jazzer"
fi

if [[ "$dry_run" != true && ( -z "$jazzer" || ! -x "$jazzer" ) ]]; then
  echo "Jazzer executable not found. Set JAZZER=/path/to/jazzer or pass --download-jazzer." >&2
  exit 1
fi

cp_file="$repo_root/core/target/pgwire-fuzz/test-classpath.txt"
if [[ "$build" == true ]]; then
  mvn -q -pl core -DskipTests compile
  mvn -q -pl core -DskipTests test-compile
  mkdir -p "$(dirname "$cp_file")"
  mvn -q -pl core -DincludeScope=test -Dmdep.outputFile="$cp_file" org.apache.maven.plugins:maven-dependency-plugin:3.7.1:build-classpath
fi

if [[ ! -f "$cp_file" ]]; then
  echo "Missing classpath file: $cp_file. Run without --no-build first." >&2
  exit 1
fi

classpath="$repo_root/core/target/test-classes:$repo_root/core/target/classes:$(cat "$cp_file")"
mkdir -p "$artifact_dir"

if [[ -n "$corpus" ]]; then
  materialized_corpus="$repo_root/core/target/pgwire-fuzz/corpus/$target"
  java -cp "$classpath" io.questdb.test.cutlass.pgwire.PGFuzzCorpusMaterializer "$corpus" "$materialized_corpus"
  corpus="$materialized_corpus"
fi

core_argline="-ea -Dfile.encoding=UTF-8 -XX:+UseParallelGC --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED"
jvm_args="$core_argline -XX:ErrorFile=$artifact_dir/hs_err_%p.log -Dquestdb.pgwire.fuzz.root=$repo_root/core/target/pgwire-fuzz/root"
if [[ -n "$postgres_host" ]]; then
  jvm_args="$jvm_args -Dquestdb.pgwire.postgres.host=$postgres_host"
fi
if [[ -n "$postgres_port" ]]; then
  jvm_args="$jvm_args -Dquestdb.pgwire.postgres.port=$postgres_port"
fi

cmd=(
  "$jazzer"
  "--target_class=$target_class"
  "--cp=$classpath"
  "--jvm_args=$jvm_args"
  "-artifact_prefix=$artifact_dir/"
  "--reproducer_path=$artifact_dir"
  "--keep_going=$keep_going"
)

if [[ -n "$corpus" ]]; then
  cmd+=("$corpus")
fi

if [[ -n "$runs" ]]; then
  cmd+=("-runs=$runs")
else
  cmd+=("-max_total_time=$max_total_time")
fi

cmd+=("${extra_args[@]}")

if [[ "$dry_run" == true ]]; then
  printf '%q ' "${cmd[@]}"
  printf '\n'
  exit 0
fi

exec "${cmd[@]}"
