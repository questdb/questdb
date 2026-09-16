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
#   LOUD_FAILURE        the engine refused to open or query, loudly. A PRODUCT finding:
#                       since issues/21 the not-evaluated cases carry NOT_EVALUATED, so this
#                       token means only "the product refused" -- for a reason, rather than
#                       for want of a distinction.
#   NOT_EVALUATED       the oracle never reached a verdict, so this boundary says NOTHING
#                       about the product either way. A rig fault, and a non-pass: a boundary
#                       that cannot be evaluated is a finding, not something to skip past.
#                       Distinct from NO_COMMIT, which is a VALID sample that measured
#                       nothing; this is an INVALID one.
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
#   MOUNT_FAILED        the filesystem would not mount at this boundary. A PRODUCT
#                       finding, not a rig fault: an ext4 that will not mount after
#                       a power cut is exactly the damage this instrument exists to
#                       catch.
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
        # issues/21. Prefix-anchored like every case above; guest/verify.sh emits both
        # "NOT_EVALUATED: ..." and "NOT_EVALUATED qwp-sf: ...", so the anchor must not
        # assume the colon.
        NOT_EVALUATED*)       echo NOT_EVALUATED ;;
        # CrashVerifier's wording on the non-WAL path. Same meaning: the
        # committed history reconciled.
        NO_COMMIT*)           echo NO_COMMIT ;;
        RPO_UNVERIFIED*)      echo RPO_UNVERIFIED ;;
        # MOUNT_FAILED USED TO HIDE INSIDE UNPARSEABLE, and those are opposite alarms:
        # "the filesystem is wrecked" against "we could not read the oracle's output".
        # run-flush-sweep.sh already special-cased the RAW STRING while this function knew
        # nothing about it -- two sources of truth for one concept, which is the drift pattern
        # that has cost this effort four separate bugs. The token lives here now, and the sweep
        # compares against it.
        #
        # Prefix-anchored like every case above, so run-st8-probe.sh's
        # "ST8_READBACK a=MOUNT_FAILED b=MOUNT_FAILED" is NOT absorbed by it.
        MOUNT_FAILED*)        echo MOUNT_FAILED ;;
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

# Did the INSTRUMENT break, or did the PRODUCT fail?
#
# Not a cosmetic split. For a durability gate this is the distinction that decides who gets
# paged: "the rig broke, nothing was measured" is a different alarm from "an acked transaction
# was lost". JUnit XML already carries it as <error> vs <failure>, and lib/junit.sh maps this
# predicate onto exactly that. It lives HERE, next to verdict_is_pass, so the two files cannot
# drift about what a token means.
#
# NOT a pass/fail decision. Every token below is still a non-pass -- verdict_is_pass is
# unchanged and remains the only authority on that. This only routes an already-failing
# boundary to the right alarm.
#
# WHY LOUD_FAILURE IS **NOT** HERE, because someone will re-litigate this:
# it USED to span both meanings, and the stopgap was to give it the LOUDER alarm -- an
# instrument fault shown as a product failure gets investigated, whereas a product failure
# shown as a rig glitch gets ignored. issues/21 removed the ambiguity at the source instead:
# guest/verify.sh now emits NOT_EVALUATED at the exits where nothing was measured, so
# LOUD_FAILURE is left meaning only "the product refused, loudly".
#
# What still emits LOUD_FAILURE, checked call site by call site:
#   guest/verify.sh:88   "$JAR missing or empty after the cut -- shipped artifacts were not
#                        durable"
#   guest/verify.sh:186/191/223/261/288  the shipped server would not start or stop, or the
#                        server that recovered was not the shipped artifact
#   CrashVerifier:305/612  CairoException on open/query (detected torn state)
#   CrashVerifier:529    "matview is EMPTY after recovery"
# Every one of those is the product refusing. The host-side scripts (power-cut-vm.sh,
# run-flush-sweep.sh, run-st8-probe.sh, run-sf-replay.sh, lib/qemu.sh) also print
# LOUD_FAILURE for their own setup faults, but those abort the run before any boundary is
# classified and never reach junit_case, so they cannot mis-page anyone.
#
# MOUNT_FAILED is likewise NOT here: see the token's note above. It is a product finding.
verdict_is_instrument_fault() {  # TOKEN -> exit 0 if the RIG broke rather than the product
    case "$1" in
        # No recognised verdict: the oracle's output could not be read, so this boundary
        # measured nothing about the product either way.
        UNPARSEABLE) return 0 ;;
        # The cut is not cutting. Every verdict that follows it is vacuous, which is a
        # statement about the instrument and never about the product.
        PREFLIGHT_FAILED) return 0 ;;
        # INCONCLUSIVE, NOT EXCULPATORY: the gap was measured but the RPO bar could not be
        # enforced, so the product was neither convicted nor cleared. It stays a non-pass --
        # do not "simplify" this into a pass; t09 pins that.
        RPO_UNVERIFIED) return 0 ;;
        # THE POINT OF issues/21. The oracle never reached a verdict -- a JVM the agent killed,
        # a distribution that was never staged, numbers that did not parse. Nothing was
        # measured, so paging the product owner is a false alarm; do that weekly and the real
        # data-loss alarm stops being believed too.
        NOT_EVALUATED) return 0 ;;
        *) return 1 ;;
    esac
}
