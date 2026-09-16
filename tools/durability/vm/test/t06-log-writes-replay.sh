#!/usr/bin/env bash
# t06 — dm-log-writes replay: validate the parser AND the boundary semantics.
#
# WHAT THIS PROVES
#   Replaying to flush N reconstructs the device as it stood at flush N, and
#   EXCLUDES everything written after it.
#
# That is exactly the property run-flush-sweep.sh depends on. If an earlier
# boundary leaked later writes, every enumerated crash point would be a blend of
# states that never existed on any real machine, and its verdicts would be
# fiction.
#
# HOW -- and why not the obvious way
#   Earlier versions tried to manufacture an "at the device but unflushed" write
#   and assert it was discarded. That kept testing the harness's ability to
#   create that state rather than the replay's correctness, and all three routes
#   failed for different reasons (each measured, not assumed):
#
#     * O_DIRECT -- ext4 forces a journal commit for the O_DIRECT path, and the
#       commit carries a FLUSH covering the write. Seen directly in the log with
#       marks bracketing it. commit=3600 does not help: not time-driven.
#     * crash immediately -- dm-log-writes queues entries to a kthread, so the
#       write is never recorded at all and its absence proves nothing.
#     * pause first -- a periodic journal commit flushes it.
#
#   So instead: write TWO files, each fsync'd, and use the replay itself as the
#   instrument. Find the first boundary at which each appears. If boundaries work,
#   `first` must appear strictly before `second`, and `second` must never appear
#   at a boundary where `first` is absent. No exotic state required, and it tests
#   the property the sweep relies on instead of a proxy for it.
#
# TWO RESET REGIMES, AND WHY THIS GUARD NEEDS BOTH
#   The device must be cleared before each replay -- dm-log-writes passes writes
#   THROUGH, so /dev/vdb still holds the final crashed state and a replay can add
#   writes but never revert them (lib/qemu.sh, replay_reset_cmd).
#
#   PASS A uses replay_reset_cmd() -- whatever run-flush-sweep.sh uses. That is not
#   a preference, it is the whole point of a guard: t06 certifies the property the
#   INSTRUMENT relies on, so it must certify it under the INSTRUMENT'S regime. When
#   this test wiped 64 MiB of a 4 GiB device and the sweep wiped nothing, a green
#   t06 said nothing whatever about a sweep result.
#
#   PASS B re-derives the same two answers after a FULL dd zero -- a different
#   mechanism, in a different layer, with no discard involved.
#
#   Pass B exists because of a COMMON-MODE FAULT that pass A structurally cannot
#   see. If the reset silently does nothing, the sweep is wrong and t06 is wrong in
#   the SAME direction, and t06 still goes green. replay_reset_assert() is the first
#   line of defence and runs below, but it samples ONE MiB at ONE offset: a PARTIAL
#   reset -- a discard honoured for some of the device and dropped for the rest --
#   passes that assert and still leaves residue where the filesystem will read it.
#   Two independent mechanisms agreeing on the boundary answer is the check that
#   actually covers it. The assert proves the reset fires; pass B proves the answer
#   does not depend on which reset fired.
#
#   Pass B is bounded to the four boundaries that PIN the two answers -- first_at,
#   first_at-1, second_at, second_at-1 -- not a second full scan. Those four are
#   sufficient: "first appears at N" is exactly "present at N, absent at N-1". A
#   full second scan would multiply a 4 GiB dd by every boundary in the log for no
#   extra information.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib/qemu.sh
source "$HERE/lib/qemu.sh"

# THE CROSS-CHECK VERDICT, as a pure function so it can be exercised without a VM.
# Reads "LABEL EXPECTED ACTUAL" triples on stdin; echoes OK, or one line per
# disagreement. Kept separate from the ssh plumbing for one reason: every other
# assertion in this file needs a booted guest to run at all, so a mistake in the
# comparison itself could only be found by burning a VM cycle. `--self-test`
# below drives it with fixtures in about a second.
t06_crosscheck() {
    local label expected actual bad=0
    while read -r label expected actual; do
        [ -z "$label" ] && continue
        if [ "$expected" != "$actual" ]; then
            echo "  MISMATCH $label: pass A said $expected, pass B said $actual"
            bad=1
        fi
    done
    [ "$bad" -eq 0 ] && echo OK
    return "$bad"
}

if [ "${1:-}" = "--self-test" ]; then
    # Fixtures, not a booted guest. `if ! out=$(...)` rather than `out=$(...) && ...`: the
    # second form's behaviour under `set -e` when the assignment fails is exactly the kind of
    # subtlety that makes a test quietly stop running half its assertions.
    rc=0
    ok=0;   if out=$(printf 'first@7 1 1\nfirst@6 0 0\n' | t06_crosscheck); then ok=1; fi
    [ "$ok" -eq 1 ] || { echo "FAIL self-test: agreeing regimes returned failure"; rc=1; }
    [ "$out" = OK ] || { echo "FAIL self-test: agreeing regimes did not report OK (got '$out')"; rc=1; }

    ok=0;   if out=$(printf 'first@7 1 0\n' | t06_crosscheck); then ok=1; fi
    [ "$ok" -eq 0 ] || { echo "FAIL self-test: a disagreement returned success"; rc=1; }
    case "$out" in *"MISMATCH first@7"*) ;; *) echo "FAIL self-test: disagreement not named (got '$out')"; rc=1 ;; esac

    # A good triple AFTER a bad one must not clear the verdict, and the named line must be
    # the offending one -- a reporter that names the last triple it saw is worse than none.
    ok=0;   if out=$(printf 'a 1 1\nb 0 1\nc 1 1\n' | t06_crosscheck); then ok=1; fi
    [ "$ok" -eq 0 ] || { echo "FAIL self-test: one bad triple among good ones returned success"; rc=1; }
    case "$out" in *"MISMATCH b"*) ;; *) echo "FAIL self-test: the bad triple was not the one named (got '$out')"; rc=1 ;; esac
    case "$out" in *"MISMATCH a"*|*"MISMATCH c"*) echo "FAIL self-test: a matching triple was reported as a mismatch (got '$out')"; rc=1 ;; esac

    [ "$rc" -eq 0 ] && echo "PASS t06 --self-test (cross-check logic)"
    exit "$rc"
fi

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"; KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/t06"; rm -rf "$RUN"; mkdir -p "$RUN"

qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
# ONE definition of the data device's size: it is the truncate argument AND pass B's dd
# count. Two copies would drift, and a dd count short of the device is exactly the weak
# wipe this revision removes (the old one cleared 64 MiB of 4 GiB, so group-1+ ext4
# metadata survived and the guard was weaker than it read).
DATA_MB=4096
truncate -s "${DATA_MB}M" "$RUN/data.raw"
truncate -s 4G "$RUN/log.raw"

P=$(vm_free_port)
# The RECORDING boot takes the default discard=ignore. An unmapping discard issued here --
# by mkfs, say -- would be recorded by dm-log-writes as a DISCARD entry and change what the
# replay later reconstructs. Only the replay boot below opts in.
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$RUN/log.raw"
vm_wait_ssh "$P" "$KEY" 240
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/
vm_ssh "$P" "$KEY" "sudo sync"

echo "--- build the log-writes stack: data=/dev/vdb log=/dev/vdc"
vm_ssh "$P" "$KEY" "sudo modprobe dm-log-writes && \
    S=\$(sudo blockdev --getsz /dev/vdb) && \
    sudo dmsetup create qdblog --table \"0 \$S log-writes /dev/vdb /dev/vdc\" && \
    sudo mkfs.ext4 -F -q /dev/mapper/qdblog && \
    sudo mkdir -p /mnt/qdb && sudo mount /dev/mapper/qdblog /mnt/qdb && \
    sudo chown ubuntu /mnt/qdb && echo stack-ready"

echo "--- write FIRST (fsync), then SECOND (fsync); each forces a flush"
vm_ssh "$P" "$KEY" "printf one > /mnt/qdb/first && sync /mnt/qdb/first && sudo sync"
vm_ssh "$P" "$KEY" "sleep 1"
vm_ssh "$P" "$KEY" "printf two > /mnt/qdb/second && sync /mnt/qdb/second && sudo sync"
vm_ssh "$P" "$KEY" "sleep 2"   # let the log kthread drain its queue

echo "--- CRASH (no umount: that would flush and blur the boundaries)"
vm_kill "$RUN"

echo "--- reboot on a fresh OS disk; data + log carry over untouched"
rm -f "$RUN/overlay.qcow2"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
P2=$(vm_free_port)
QDB_VM_DATA_DISCARD=unmap vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw"
vm_wait_ssh "$P2" "$KEY" 240
vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/

# The reset must be REAL before either pass means anything. QEMU's default discard=ignore
# still advertises discard and still returns success in 8 ms having reverted nothing, which
# would make both passes agree on a blended state -- the precise common-mode failure pass B
# exists to catch, arriving one layer lower than pass B can see.
RESET_CMD="$(replay_reset_cmd)"
echo "--- pass A reset regime: ${QDB_REPLAY_RESET:-blkdiscard} (the instrument's)"
replay_reset_assert "$P2" "$KEY" || { echo "FAIL t06: the device reset is not real; no boundary claim below can be trusted"; exit 1; }

nflush=$(vm_ssh "$P2" "$KEY" "sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --list | head -1" \
    | grep -oE '[0-9]+ flushes' | grep -oE '^[0-9]+')
echo "--- log holds $nflush flush boundaries; replaying each"
[ "${nflush:-0}" -ge 3 ] || { echo "FAIL t06: only ${nflush:-0} flushes recorded; cannot test a boundary"; exit 1; }

# ONE replay+mount+list, parameterised by the reset it runs first. Both passes go through
# here so they cannot drift in any way EXCEPT the reset -- which is the single variable this
# test is comparing. A second hand-written copy for pass B would reintroduce exactly the
# drift this session spent four commits removing.
replay_and_list() {  # BOUNDARY RESET_SHELL_CMD
    vm_ssh "$P2" "$KEY" "sudo umount /mnt/raw 2>/dev/null; \
        $2; \
        sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush $1 >/dev/null 2>&1; \
        sudo mkdir -p /mnt/raw; \
        if sudo mount /dev/vdb /mnt/raw 2>/dev/null; then ls /mnt/raw | tr '\n' ' '; else echo UNMOUNTABLE; fi"
}

first_at=0; second_at=0; leaked=0
for n in $(seq 1 "$nflush"); do
    ls_out=$(replay_and_list "$n" "$RESET_CMD")
    has_first=0; has_second=0
    case "$ls_out" in *first*) has_first=1 ;; esac
    case "$ls_out" in *second*) has_second=1 ;; esac
    printf '  flush %2d/%-2d -> %s\n' "$n" "$nflush" "$ls_out"

    # THE BOUNDARY VIOLATION: `second` cannot exist at a boundary where `first`
    # does not. That ordering is fixed by the order they were written and fsync'd.
    if [ "$has_second" -eq 1 ] && [ "$has_first" -eq 0 ]; then leaked=1; fi
    if [ "$first_at" -eq 0 ] && [ "$has_first" -eq 1 ]; then first_at=$n; fi
    if [ "$second_at" -eq 0 ] && [ "$has_second" -eq 1 ]; then second_at=$n; fi
done

echo "first appears at flush $first_at; second appears at flush $second_at"
[ "$leaked" -eq 0 ] || { echo "FAIL t06: 'second' present where 'first' was absent — replay is blending states"; exit 1; }
[ "$first_at" -gt 0 ] || { echo "FAIL t06: 'first' never appears — replay is dropping writes"; exit 1; }
[ "$second_at" -gt 0 ] || { echo "FAIL t06: 'second' never appears — the log tail is being truncated"; exit 1; }
[ "$first_at" -lt "$second_at" ] || {
    echo "FAIL t06: both files appear at the same boundary (flush $first_at)."
    echo "  The replay is not discriminating between boundaries, so every"
    echo "  enumerated crash point would report the same state."
    exit 1
}

# ---- PASS B: the same two answers, via a different reset mechanism -------------
# Only the four boundaries that PIN the answers. "first appears at N" IS "present at N and
# absent at N-1", so these four re-derive both answers in full. N-1 = 0 is skipped: there is
# no flush 0 to replay, and absence before the first boundary is not a claim this test makes.
echo "--- pass B: full ${DATA_MB} MiB dd zero, at the boundaries that pin the answers"
B_DD="sudo dd if=/dev/zero of=/dev/vdb bs=4M count=$((DATA_MB / 4)) status=none"
pins=""
add_pin() {  # BOUNDARY FILE EXPECTED
    [ "$1" -ge 1 ] || return 0
    ls_out=$(replay_and_list "$1" "$B_DD")
    got=0
    case "$ls_out" in *"$2"*) got=1 ;; esac
    printf '  flush %2d (%s) -> %s\n' "$1" "$2" "$ls_out"
    pins="$pins$2@$1 $3 $got"$'\n'
}
# 'second' matches 'first' nowhere, but 'first' IS a substring of nothing else in this
# filesystem either; the two names were chosen distinct for exactly this reason.
add_pin "$first_at"             first  1
add_pin "$(( first_at - 1 ))"   first  0
add_pin "$second_at"            second 1
add_pin "$(( second_at - 1 ))"  second 0
vm_kill "$RUN"

if ! cross=$(printf '%s' "$pins" | t06_crosscheck); then
    echo "$cross"
    echo "FAIL t06: the boundary answer DEPENDS ON THE RESET MECHANISM."
    echo "  Pass A used ${QDB_REPLAY_RESET:-blkdiscard}, pass B a full dd zero, and they disagree."
    echo "  One of the two is not clearing the device it claims to clear -- a partial discard"
    echo "  passes replay_reset_assert (it samples one MiB) and still leaves residue where the"
    echo "  filesystem reads it. Do NOT trust a sweep result until this is resolved."
    exit 1
fi

rm -rf "$RUN"
echo "PASS t06 (boundaries discriminate: first@$first_at < second@$second_at, nothing leaked backwards;"
echo "          and both reset regimes agree at the four pinning boundaries)"
