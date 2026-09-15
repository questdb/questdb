# lib/verdict.sh — the one shared verdict vocabulary.
# Source this file; do not execute it.
#
# Both arms print exactly one verdict line, and every consumer classifies it
# through this function. Keeping the vocabulary in one place is what makes a
# DIVERGENCE between the two arms meaningful rather than a wording artifact.
#
#   DURABLE             every committed txn survived (W=0, or no loss at W>0)
#   RPO_OK              every ACKED txn survived; loss confined to (Wm, C]
#   DURABILITY_FAILURE  an acked txn was lost, or a suspend never cleared
#   SILENT_CORRUPTION   wrong value, gap, or torn commit boundary — the worst
#   LOUD_FAILURE        the engine refused to open or query, loudly
#   PREFLIGHT_OK        the cut demonstrably drops un-fsync'd data
#   PREFLIGHT_FAILED    the cut is not cutting — every later verdict is vacuous
#   NO_COMMIT           the cut landed before ANYTHING was committed. A legitimate
#                       but UNINFORMATIVE sample -- counted as a pass so it does
#                       not fail the run, but tracked separately so it cannot
#                       inflate the durable count.
#   RPO_UNVERIFIED      the run COMPLETED and the data-loss gap was MEASURED, but
#                       the RPO bar could not be enforced because the client-side
#                       Wm was unavailable. Reported with the measured gap, never
#                       silently treated as a pass. Measuring the gap is the point
#                       -- refusing to run measures nothing.
#   UNPARSEABLE         no recognised verdict; treated as failure, disks kept

verdict_classify() {  # LINE -> one token on stdout
    local line="$1"
    case "$line" in
        PREFLIGHT_OK*)        echo PREFLIGHT_OK ;;
        PREFLIGHT_FAILED*)    echo PREFLIGHT_FAILED ;;
        DURABLE*)             echo DURABLE ;;
        RPO_OK*)              echo RPO_OK ;;
        DURABILITY_FAILURE*)  echo DURABILITY_FAILURE ;;
        SILENT_CORRUPTION*)   echo SILENT_CORRUPTION ;;
        LOUD_FAILURE*)        echo LOUD_FAILURE ;;
        # CrashVerifier's wording on the non-WAL path. Same meaning: the
        # committed history reconciled.
        NO_COMMIT*)           echo NO_COMMIT ;;
        RPO_UNVERIFIED*)      echo RPO_UNVERIFIED ;;
        CONSISTENT*)          echo DURABLE ;;
        *)                    echo UNPARSEABLE ;;
    esac
}

# verdict_line BLOB -> the ONE verdict line out of a multi-line oracle output.
#
# guest/verify.sh prints its evidence as DETAIL / DETAIL-ERR lines and the verdict LAST. A
# caller that classifies the whole blob therefore matches the FIRST line, which is evidence,
# and gets UNPARSEABLE for a perfectly good run.
#
# THAT WAS NOT HYPOTHETICAL. power-cut-vm.sh classified the raw blob, so after verify.sh gained
# its DETAIL output EVERY live-cut run classified as UNPARSEABLE internally: each one took the
# failure path, kept its ~300-700 MB of disks, and exited 1 -- while its CALLERS, which took
# `tail -1`, correctly reported DURABLE. One session of run-matrix.sh left 27 run directories
# and 28 GB behind, and check-host.sh gates on >= 200 GB free, so a nightly would have wedged
# itself and reported an infrastructure problem.
#
# Filtering DETAIL is not the same as `tail -1`: DETAIL-ERR lines can legitimately follow the
# verdict. Both are handled here, once, instead of in three call sites with three opinions.
verdict_line() {  # BLOB -> the verdict line (empty if there is none)
    printf '%s\n' "$1" | grep -vE '^DETAIL' | grep -vE '^[[:space:]]*$' | tail -1
}

# A verdict that means "the run passed". Anything else keeps the disks.
verdict_is_pass() {  # TOKEN -> exit 0 if pass
    case "$1" in
        DURABLE|RPO_OK|PREFLIGHT_OK|NO_COMMIT) return 0 ;;
        *) return 1 ;;
    esac
}
