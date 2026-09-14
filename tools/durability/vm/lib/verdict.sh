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

# A verdict that means "the run passed". Anything else keeps the disks.
verdict_is_pass() {  # TOKEN -> exit 0 if pass
    case "$1" in
        DURABLE|RPO_OK|PREFLIGHT_OK|NO_COMMIT) return 0 ;;
        *) return 1 ;;
    esac
}
