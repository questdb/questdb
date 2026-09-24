# lib/verdict.sh — the one shared verdict vocabulary.
# Source this file; do not execute it.
#
# Every arm prints exactly one verdict line, and every consumer classifies it through
# verdict_classify. One vocabulary in one place is what makes a divergence between two arms a
# real difference rather than a wording artifact.
#
#   DURABLE             every committed txn survived (W=0, or no loss at W>0)
#   RPO_OK              every ACKED txn survived; loss confined to (Wm, C]
#   PRECONDITION_NOT_MET
#                       this boundary did not reach the mat-view repair: no captured durability
#                       gap, no actual base rollback, or no persisted view ahead of the recovered
#                       base. A boundary-level skip; the sweep fails as NOT_EVALUATED if every
#                       sampled boundary has this result.
#   DURABILITY_FAILURE  an acked txn was lost, or a suspend never cleared
#   SILENT_CORRUPTION   wrong value, gap, or torn commit boundary — the worst
#   LOUD_FAILURE        the engine refused to open or query, loudly. A PRODUCT finding: the
#                       cases where nothing was measured carry NOT_EVALUATED instead, so this
#                       token means only "the product refused".
#   NOT_EVALUATED       the oracle never reached a verdict, so this boundary says nothing
#                       about the product either way. A rig fault, and a non-pass: a boundary
#                       that cannot be evaluated is a finding, not something to skip past.
#                       Distinct from NO_COMMIT, which is a VALID sample that measured
#                       nothing; this is an INVALID one.
#   NO_COMMIT           the cut landed before ANYTHING was committed. A legitimate but
#                       uninformative sample: counted as a pass so it does not fail the run,
#                       tracked separately so it cannot inflate the durable count.
#   MOUNT_FAILED        the filesystem would not mount at this boundary. A PRODUCT finding,
#                       not a rig fault: an ext4 that will not mount after a power cut is
#                       exactly the damage this instrument exists to catch.
#   UNPARSEABLE         no recognised verdict; treated as failure, disks kept

verdict_classify() {  # LINE -> one token on stdout
    local line="$1"
    case "$line" in
        DURABLE*)             echo DURABLE ;;
        RPO_OK*)              echo RPO_OK ;;
        PRECONDITION_NOT_MET*) echo PRECONDITION_NOT_MET ;;
        DURABILITY_FAILURE*)  echo DURABILITY_FAILURE ;;
        SILENT_CORRUPTION*)   echo SILENT_CORRUPTION ;;
        LOUD_FAILURE*)        echo LOUD_FAILURE ;;
        # guest/verify.sh emits both "NOT_EVALUATED: ..." and "NOT_EVALUATED qwp-sf: ...", so
        # the anchor must not assume the colon.
        NOT_EVALUATED*)       echo NOT_EVALUATED ;;
        NO_COMMIT*)           echo NO_COMMIT ;;
        # Prefix-anchored like every case above, so a line that merely mentions the token
        # inside a longer message is not absorbed by it.
        MOUNT_FAILED*)        echo MOUNT_FAILED ;;
        # CrashVerifier's wording on the non-WAL path. Same meaning: the committed history
        # reconciled.
        CONSISTENT*)          echo DURABLE ;;
        *)                    echo UNPARSEABLE ;;
    esac
}

# verdict_line BLOB -> the ONE verdict line out of a multi-line oracle output.
#
# guest/verify.sh prints its evidence as DETAIL / DETAIL-ERR lines and the verdict last, so a
# caller that classifies the whole blob matches the FIRST line -- evidence -- and gets
# UNPARSEABLE for a perfectly good run. Filtering DETAIL is not the same as `tail -1`, because
# DETAIL-ERR lines can legitimately follow the verdict. Both are handled here, once, rather
# than in three call sites with three opinions.
verdict_line() {  # BLOB -> the verdict line (empty if there is none)
    printf '%s\n' "$1" | grep -vE '^DETAIL' | grep -vE '^[[:space:]]*$' | tail -1
}

# A verdict that means "the run passed". Anything else keeps the disks.
verdict_is_pass() {  # TOKEN -> exit 0 if pass
    case "$1" in
        DURABLE|RPO_OK|PRECONDITION_NOT_MET|NO_COMMIT) return 0 ;;
        *) return 1 ;;
    esac
}

# Did the INSTRUMENT break, or did the PRODUCT fail?
#
# For a durability gate this decides who gets paged: "the rig broke, nothing was measured" is a
# different alarm from "an acked transaction was lost". lib/junit.sh maps this predicate onto
# <error> vs <failure>, and it lives here beside verdict_is_pass so the two cannot drift about
# what a token means.
#
# NOT a pass/fail decision. Every token below is already a non-pass; verdict_is_pass remains the
# only authority on that. This routes an already-failing boundary to the right alarm.
#
# LOUD_FAILURE and MOUNT_FAILED are deliberately absent: the product refusing to open, and a
# filesystem that will not mount, are both product findings. What keeps LOUD_FAILURE unambiguous
# is that guest/verify.sh emits NOT_EVALUATED at the exits where nothing was measured.
verdict_is_instrument_fault() {  # TOKEN -> exit 0 if the RIG broke rather than the product
    case "$1" in
        # No recognised verdict: the oracle's output could not be read, so this boundary
        # measured nothing about the product either way.
        UNPARSEABLE) return 0 ;;
        # The oracle never reached a verdict -- a JVM the agent killed, a distribution that was
        # never staged, numbers that did not parse. Paging the product owner for that is a false
        # alarm, and a weekly false alarm is how the real one stops being believed.
        NOT_EVALUATED) return 0 ;;
        *) return 1 ;;
    esac
}
