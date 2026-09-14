#!/usr/bin/env bash
# run-matrix.sh — every cell, aggregated, with the self-checks that keep the
# suite honest.
#
# Each cell is a separate cut cycle on fresh disks. Where both arms run a cell,
# they run as two independent cycles and are never interleaved in one guest
# lifetime, so neither can perturb the other's device state.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/verdict.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=lib/verdict.sh
source "$HERE/lib/verdict.sh"
# shellcheck source=lib/preflight.sh
source "$HERE/lib/preflight.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
LOG="$STATE_DIR/runs.log"
mkdir -p "$STATE_DIR"
STAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
fails=0

# mode:window_us:arms
#
# The product arm runs W=0 cells only. The client-side LOCAL durable-ack
# frontier is WIP, so at W>0 the product arm cannot observe Wm and therefore
# cannot enforce the RPO bar — running it there would print a verdict it never
# actually checked. The reference arm reads the frontier in-process and covers
# W>0 today.
CELLS=(
    "adaptive:0:reference product"
    "adaptive:50000:reference"
    "SYNC:0:reference product"
    "NOSYNC:0:reference product"
)

echo "QuestDB VM crash matrix — $STAMP"

# THE STANDING GATE. Prove the cut still drops un-flushed device writes before
# trusting a single durability verdict below it. test/t04 proves this check can
# fail; without that, a green matrix would mean nothing.
echo "  preflight ..."
pf=$(run_preflight_cycle real "$HERE")
echo "$STAMP preflight verdict=$pf" >> "$LOG"
if [ "$pf" != "PREFLIGHT_OK" ]; then
    echo "ABORT: preflight returned $pf — the cut is not cutting, so no verdict below it is meaningful" >&2
    echo "$STAMP ABORT preflight=$pf" >> "$LOG"
    exit 2
fi
echo "  preflight -> $pf"
for cell in "${CELLS[@]}"; do
    mode="${cell%%:*}"; rest="${cell#*:}"; w="${rest%%:*}"; arms="${rest#*:}"
    declare -A got=()

    for arm in $arms; do
        line=$(bash "$HERE/power-cut-vm.sh" --arm="$arm" --mode="$mode" --window-us="$w" 2>/dev/null | tail -1)
        v=$(verdict_classify "$line")
        got[$arm]="$v"
        echo "$STAMP cell=$mode/W=$w arm=$arm verdict=$v line=$line" >> "$LOG"
        printf '  %-9s W=%-6s [%-9s] -> %s\n' "$mode" "$w" "$arm" "$v"
    done

    # NOSYNC is reported, NOT gated.
    #
    # It was originally a self-check on the theory that a no-sync mode must lose
    # data across a power cut, so a DURABLE verdict would prove the cut had
    # stopped cutting. MEASURED, that theory is wrong for this harness: NOSYNC
    # reproducibly loses NOTHING (count==watermark exactly, at 312k / 432k / 974k
    # rows across runs, and unchanged under ext4 data=writeback, which rules out
    # a foreign flush from the harness's own _progress fsync).
    #
    # The reason is structural. These ingests run for tens of seconds and the
    # guest kernel writes dirty pages back continuously throughout, so by the
    # time drop_writes is armed almost everything -- including the _txn commit
    # pointer -- is already legitimately on disk. The cut can only discard what
    # has not been written back YET, which under sustained load is a thin tail.
    #
    # So a DURABLE NOSYNC verdict says nothing about whether the cut works. The
    # per-run preflight above is the guard that actually discriminates, and
    # test/t04 proves it can fail. Gating on NOSYNC would only produce a red
    # suite that says nothing.
    if [ "$mode" = "NOSYNC" ]; then
        continue
    fi

    # A disagreement between the arms is a finding about the layer between them
    # — the wire, the ack plumbing, the server lifecycle — not a flake to retry.
    # Only meaningful where BOTH arms ran; a single-arm cell has nothing to
    # diverge from and must not be compared against an unset value.
    if [ -n "${got[reference]:-}" ] && [ -n "${got[product]:-}" ] \
       && [ "${got[reference]}" != "${got[product]}" ]; then
        echo "  DIVERGENCE $mode W=$w reference=${got[reference]} product=${got[product]}"
        echo "$STAMP DIVERGENCE cell=$mode/W=$w reference=${got[reference]} product=${got[product]}" >> "$LOG"
        fails=$((fails + 1))
        continue
    fi

    for arm in $arms; do
        verdict_is_pass "${got[$arm]}" || fails=$((fails + 1))
    done
done

echo "matrix complete: $fails failure(s); log at $LOG"
[ "$fails" -eq 0 ]
