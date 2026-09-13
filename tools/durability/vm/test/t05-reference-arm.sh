#!/usr/bin/env bash
# t05 — the reference arm end to end, on a real kernel and a real block layer.
#
#   preflight     MUST be PREFLIGHT_OK. This is the gate: it proves the cut still
#                 discards un-flushed DEVICE writes. test/t04 proves this check
#                 can fail, which is what makes a green result here mean anything.
#   adaptive W=0  MUST be DURABLE. Zero-loss configuration: every committed txn
#                 survives the cut.
#   SYNC          MUST be DURABLE. Pre-adaptive regression guard.
#   NOSYNC        REPORTED, NOT GATED. See below.
#
# WHY NOSYNC IS NOT A GATE.
# The obvious self-check -- "a no-sync mode must lose data across a power cut, so
# a DURABLE verdict proves the cut stopped cutting" -- is WRONG for this harness,
# and was measured to be wrong rather than argued away:
#
#   data=ordered   count=974000 watermark=974000   no loss
#   data=ordered   count=312000 watermark=312000   no loss
#   data=writeback count=432000 watermark=432000   no loss
#
# The data=writeback arm rules out a foreign flush from the harness's own
# _progress fsync (under ext4 data=ordered an fsync forces a journal commit that
# writes back other inodes' data; data=writeback removes that coupling, and the
# result did not change).
#
# The real reason is structural: the cut can only discard writes that have not
# been written back YET. These ingests run for tens of seconds and the guest
# kernel flushes dirty pages continuously, so by the time drop_writes is armed
# almost everything -- including the _txn commit pointer -- is already
# legitimately on disk. The cut removes a thin tail.
#
# That is also the honest limit of this harness: it crashes at ONE wall-clock
# moment, after most data is already safe. The JVM sweeps
# (AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint) crash at EVERY
# durability op and remain the stronger instrument. This answers a different
# question -- does adaptive survive a real cut on real hardware -- and a NOSYNC
# verdict contributes nothing to it either way.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=../lib/verdict.sh
source "$HERE/lib/verdict.sh"
# shellcheck source=../lib/preflight.sh
source "$HERE/lib/preflight.sh"

# `|| true` on the cells: power-cut-vm.sh signals a bad verdict through its EXIT
# CODE as well as its output, and under `set -e` a non-zero exit would abort this
# script before it could print why. stderr is deliberately NOT merged -- the
# controller prints its "run state kept" note there, and merging would let
# `tail -1` capture the note and discard the verdict above it.

pf=$(run_preflight_cycle real "$HERE")
[ "$pf" = "PREFLIGHT_OK" ] || {
    echo "FAIL t05: preflight gave $pf — the cut is not cutting, so nothing below it is meaningful"
    exit 1
}

a=$(bash "$HERE/power-cut-vm.sh" --arm=reference --mode=adaptive --window-us=0 --epoch-ms=1000 | tail -1 || true)
va=$(verdict_classify "$a")
[ "$va" = "DURABLE" ] || { echo "FAIL t05: adaptive W=0 gave $va ($a)"; exit 1; }

s=$(bash "$HERE/power-cut-vm.sh" --arm=reference --mode=SYNC | tail -1 || true)
vs=$(verdict_classify "$s")
[ "$vs" = "DURABLE" ] || { echo "FAIL t05: SYNC gave $vs ($s)"; exit 1; }

n=$(bash "$HERE/power-cut-vm.sh" --arm=reference --mode=NOSYNC | tail -1 || true)
vn=$(verdict_classify "$n")

echo "PASS t05 (preflight=$pf adaptive-W0=$va sync=$vs) [nosync=$vn, reported not gated]"
