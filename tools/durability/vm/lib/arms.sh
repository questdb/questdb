# lib/arms.sh — the arm vocabulary, in one place.
# Source this file; do not execute it.
#
# An "arm" is a branch of the experiment, as in a clinical trial: the same invariant reached by
# a different route into the engine.
#
#   reference  the engine embedded in the writer process. Sees C and Wm in-process.
#   qwp        a real server and a real WebSocket client. The server tracks and recovers.
#   qwp-sf     as qwp, plus the client requests the local durable-ack tier, holds un-acked rows
#              in a store-and-forward buffer on the crashed device, and replays them.
#   product    qwp-sf with the server artifact swapped: the release tarball
#              (questdb-<ver>-no-jre-bin.tar.gz), unpacked in the guest and started by the real
#              questdb.sh launcher, which runs QuestDB as a named JPMS module
#              (-p questdb.jar -m io.questdb/io.questdb.ServerMain). Every other arm runs the
#              engine on the classpath in the unnamed module, so nothing else here can see a
#              packaging, entry-point or module-configuration regression.
#
# product is qwp-sf plus exactly one change, so that a product-vs-qwp-sf divergence is
# attributable to packaging rather than to a different test.
#
# Every definition here is shared by all callers. A second call site holding its own copy of an
# arm's liveness pattern, progress file or verify flags is how the vocabulary drifts, and a
# drifted guard either fails closed on a healthy run or never fires at all.

# arm_live_pattern ARM -> the pgrep -f pattern that matches that arm's workload process.
#
# The bracket is load-bearing and must survive any edit. `pgrep -f` matches whole command lines,
# and the ssh command running the check carries the class name in its own cmdline, so a plain
# pattern matches itself and reports the workload alive after it has exited. `[C]rash...`
# matches the real process but not the literal text searching for it.
arm_live_pattern() {
    case "$1" in
        qwp|qwp-sf|product) echo "[Q]wpCrashIngestClient" ;;
        *)                  echo "[C]rashIngestWriter" ;;
    esac
}

# arm_sf_capable MODE -> 0 if the LOCAL durable-ack tier can exist at all in this commit mode.
#
# Adaptive only, and that is a property of the product rather than an assumption: WalWriter's
# commit path guards the whole durable-ack bookkeeping with
#
#     if (commitMode == CommitMode.ADAPTIVE) { ... seqTxnTracker.setLocalDurableSeqTxn(seqTxn); }
#
# so under SYNC the commit is fdatasync'd but localDurableSeqTxn never advances,
# LocalDurableAckRegistry returns -1, and the server emits no STATUS_LOCAL_DURABLE_ACK frames.
# Checked up front: otherwise the mismatch surfaces deep into a run as the client's "the channel
# is dead" refusal, which names the symptom and blames the wrong layer.
arm_sf_capable() {
    case "$(echo "$1" | tr 'A-Z' 'a-z')" in
        adaptive) return 0 ;;
        *) return 1 ;;
    esac
}

# arm_is_known ARM -> 0 if this is an arm the harness can run. An arm belongs here only once it
# can produce a verdict: reporting a stub as known lets a caller start a cell that cannot.
arm_is_known() {
    case "$1" in
        reference|qwp|qwp-sf|product) return 0 ;;
        *) return 1 ;;
    esac
}

# arm_progress_file ARM -> the watermark file that arm's workload writes, relative to the DB root.
#
# The reference arm's writer owns the engine and writes _progress. The qwp arms run the client
# as a separate process from the server, so it writes its own _qwp_progress carrying the same
# C / Wm pair read back from the server's wal_tables(). A caller that anchors on the wrong one
# waits for a file that never appears.
arm_progress_file() {
    case "$1" in
        qwp|qwp-sf|product) echo "_qwp_progress" ;;
        *)                  echo "_progress" ;;
    esac
}

# arm_verify_flags ARM -> the flags guest/verify.sh needs for that arm.
#
# verify.sh's --arm names which oracle to run, not which workload produced the data, and the
# only implemented oracle is `reference`. The qwp arms are expressed as that oracle plus
# modifiers (--qwp / --qwp-sf). Forwarding a workload arm straight through instead reaches
# verify.sh as an unknown arm, after a full record, cut and reboot cycle has already been spent.
#
# MODE is the second argument and is not optional in spirit, because the product arm's oracle
# depends on it. It defaults to adaptive only so an old call site fails on the flags rather than
# on an unbound variable.
arm_verify_flags() {
    local mode="${2:-adaptive}"
    case "$1" in
        qwp)    echo "--arm=reference --qwp=true --qwp-sf=false --sf-replay=${QDB_SF_REPLAY:-false}" ;;
        qwp-sf) echo "--arm=reference --qwp=true --qwp-sf=true --sf-replay=${QDB_SF_REPLAY:-compare}" ;;
        # product differs from qwp-sf by one flag, and that flag is the arm: --server=product
        # makes the recovery pass and the replay server the shipped artifact. Drop it and the run
        # silently degrades into a qwp-sf run wearing a product label.
        #
        # Outside adaptive the sf half cannot exist (see arm_sf_capable), so the arm drops to the
        # plain-qwp contract and says so through its flags. The artifact claim is untouched, but
        # nothing downstream may report ack-channel coverage the product cannot provide in this
        # mode, and asking for --qwp-sf=true here would fail the run on the absence of a
        # guarantee never on offer.
        product)
            if arm_sf_capable "$mode"; then
                echo "--arm=reference --qwp=true --qwp-sf=true --sf-replay=${QDB_SF_REPLAY:-compare} --server=product"
            else
                echo "--arm=reference --qwp=true --qwp-sf=false --sf-replay=false --server=product"
            fi
            ;;
        *)      echo "--arm=reference --qwp=false --qwp-sf=false" ;;
    esac
}

# arm_qwp_tier ARM MODE -> the durable-ack tier that arm requests by default.
#
# The arm name has to imply the tier, or qwp-sf launches with durable ack disabled: a label
# promising a guarantee the configuration never requested. An explicit QDB_QWP_DURABLE_ACK still
# wins, because that is how the defanged negative control turns the channel off on purpose.
arm_qwp_tier() {
    local arm="$1" mode="${2:-adaptive}"
    case "$arm" in
        qwp-sf)  echo "${QDB_QWP_DURABLE_ACK:-local}" ;;
        product) if arm_sf_capable "$mode"; then echo "${QDB_QWP_DURABLE_ACK:-local}"; else echo off; fi ;;
        *)       echo "${QDB_QWP_DURABLE_ACK:-off}" ;;
    esac
}

# harness_workload_env ARM MODE -> the environment guest/run-workload.sh needs, as one string.
#
# ssh does not carry the caller's environment, so every knob has to be named explicitly on the
# remote command line. A knob left out is not an error, it is a default: a run asked for as
# enterprise then runs OSS, and a defanged negative control runs undefanged and reports the
# green it was built to make impossible. Built once here so no call site can omit one.
harness_workload_env() {
    local arm="$1" mode="${2:-adaptive}"
    echo "QDB_WAL_TABLE=$(harness_wal_table "$mode")" \
         "QDB_SCHEMA_PROFILE=${QDB_SCHEMA_PROFILE:-bitmap}" \
         "QDB_SIBLING_TABLE=${QDB_SIBLING_TABLE:-false}" \
         "QDB_DDL_EVERY_ROWS=${QDB_DDL_EVERY_ROWS:--1}" \
         "QDB_MAT_VIEW=${QDB_MAT_VIEW:-false}" \
         "QDB_REBASE_AT_ROWS=${QDB_REBASE_AT_ROWS:--1}" \
         "QDB_WITNESS_FSYNC=${QDB_WITNESS_FSYNC:-true}" \
         "QDB_QWP_DURABLE_ACK=$(arm_qwp_tier "$arm" "$mode")" \
         "QDB_QWP_DEFANG_ACK=${QDB_QWP_DEFANG_ACK:-0}" \
         "QDB_QWP_BATCH=${QDB_QWP_BATCH:-1000}" \
         "QDB_EDITION=${QDB_EDITION:-oss}" \
         "QDB_QWP_SF_DIR=${QDB_QWP_SF_DIR:-/mnt/qdb/sf}" \
         "QDB_QWP_SF_DURABILITY=${QDB_QWP_SF_DURABILITY:-periodic}"
}

# Every key the two builders share must carry the SAME value, because the recording and the replay
# are separate JVMs reading the same settings. An override that reaches only the verifier makes the
# replay client look in a directory the workload never wrote to, and "the client replayed NOTHING"
# is reported as a durability failure of the product. test/t11 pins the pairing.

# harness_assert_config -> 0 if the environment this run was asked for is coherent, 64 if not.
#
# Called before anything boots. Both checks below are otherwise swallowed by a command
# substitution: harness_wal_table's refusal becomes an empty QDB_WAL_TABLE= that silently reverts
# to the mode-derived default, and a defanged label with an intact tier runs a fully armed sweep
# under a "must fail" banner.
harness_assert_config() {  # [MODE]
    local mode="${1:-adaptive}" rc=0
    harness_wal_table "$mode" >/dev/null || rc=64
    if [ "${QDB_QWP_DEFANG_ACK:-0}" = "1" ]; then
        case "$(arm_qwp_tier "${QDB_ARM:-reference}" "$mode")" in
            *local*)
                echo "harness: QDB_QWP_DEFANG_ACK=1 but the durable-ack tier is still local." >&2
                echo "  The negative control would run fully armed while every report labels it" >&2
                echo "  required-to-fail. Set QDB_QWP_DURABLE_ACK=off to actually defang it." >&2
                rc=64 ;;
        esac
    fi
    return "$rc"
}

# harness_wal_table MODE -> whether this run uses a WAL table, as `true`/`false`.
#
# By default the mode picks the table kind: SYNC/NOSYNC select a bypass-WAL table, adaptive
# selects a WAL table. QDB_WAL_TABLE overrides that, which is what makes "WAL table, no
# durability barrier" expressible -- the barrier control for the WAL path.
#
# Defined here, beside the two functions that use it, because the writer and the verifier must
# receive the same value.
#
# Deliberately not expressed via arm_sf_capable, even though both currently reduce to "is the
# mode adaptive". They answer different questions, and if the durable-ack tier is ever granted
# under SYNC a shared predicate would silently flip every SYNC run onto a WAL table.
harness_wal_table() {
    case "$(echo "${QDB_WAL_TABLE:-}" | tr 'A-Z' 'a-z')" in
        true)  echo true ;;
        false) echo false ;;
        "")    case "$(echo "${1:-adaptive}" | tr 'A-Z' 'a-z')" in
                   adaptive) echo true ;;
                   *)        echo false ;;
               esac ;;
        *)     echo "harness_wal_table: QDB_WAL_TABLE must be true or false (got '$QDB_WAL_TABLE')" >&2
               return 64 ;;
    esac
}

# harness_verify_cmd ARM MODE WINDOW EPOCH -> the complete guest verify.sh command.
#
# Every call site uses this rather than assembling its own. A hand-copied verify invocation that
# misses a flag grades its boundaries with a different oracle than the run claims -- which, in
# the sweep's densify pass, means the neighbours of a failure are graded differently from the
# failure they are meant to bracket.
harness_verify_cmd() {
    local arm="$1" mode="${2:-adaptive}" window="${3:-0}" epoch="${4:-1000}"
    # QDB_WAL_TABLE travels to the verifier as well, and it is the same value the workload got.
    # CrashVerifier re-reads every -D in its own JVM, so passing it to only one of the two is
    # lost silently.
    echo "env QDB_PRODUCT_RECOVERY_PASS=${QDB_PRODUCT_RECOVERY_PASS:-true}" \
         "QDB_WAL_TABLE=$(harness_wal_table "$mode")" \
         "QDB_QWP_DURABLE_ACK=$(arm_qwp_tier "$arm" "$mode")" \
         "QDB_QWP_SF_DIR=${QDB_QWP_SF_DIR:-/mnt/qdb/sf}" \
         "QDB_QWP_SF_DURABILITY=${QDB_QWP_SF_DURABILITY:-periodic}" \
         "bash /opt/vmcrash/guest/verify.sh $(arm_verify_flags "$arm" "$mode")" \
         "--mode=$mode --window-us=$window --epoch-ms=$epoch" \
         "--sibling=${QDB_SIBLING_TABLE:-false}" \
         "--recover-as=${QDB_RECOVER_AS:-}" \
         "--profile=${QDB_SCHEMA_PROFILE:-bitmap}" \
         "--mat-view=${QDB_MAT_VIEW:-false}" \
         "--rebase=$([ "${QDB_REBASE_AT_ROWS:--1}" -gt 0 ] && echo true || echo false)"
}
