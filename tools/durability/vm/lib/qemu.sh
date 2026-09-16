# lib/qemu.sh — VM lifecycle primitives for the crash harness.
# Source this file; do not execute it.
#
# DURABILITY-CRITICAL: every disk is attached cache=none.
#
#   cache=writeback  — the HOST page cache holds the guest's un-flushed writes
#                      and SURVIVES the VMM being killed, because the host did
#                      not lose power. Data that should be lost stays durable.
#                      False green.
#   cache=directsync — every guest write becomes durable immediately, so even
#                      NOSYNC survives. False green.
#   cache=none       — O_DIRECT, no host page cache. The GUEST page cache dies
#                      with the VMM, which is what we want.
#
# cache=none is necessary but NOT sufficient: anything the guest kernel already
# wrote back on its own schedule has reached host storage and survives, whereas
# on real hardware it would sit in the disk's volatile write cache and die.
# That residual leniency is closed by dm-flakey drop_writes inside the guest
# (guest/arm-cut.sh), not here. See the spec, §2.

# Pick an unused loopback port for the SSH forward. Bound to 127.0.0.1 only:
# the harness never exposes a service on this host.
vm_free_port() {
    python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# vm_boot RUNDIR BOOTDISK DATADISK PORT [SEEDISO] [LOGDISK]
#   LOGDISK, when given, is attached as /dev/vdc and used as the dm-log-writes
#   log device. It is deliberately a SEPARATE disk: the log must record the data
#   device's traffic without becoming part of it.
# Boots daemonized; writes $RUNDIR/qemu.pid, $RUNDIR/console.log and
# $RUNDIR/cmdline. DATADISK may be "" (no data disk).
# vm_boot RUNDIR BOOT DATA SSHPORT [SEED] [LOGDISK] [QWPPORT]
#
# QWPPORT (optional) additionally forwards the guest's 9000 to that HOST port, so a client
# running OUTSIDE the VM can reach the server. That is the deployment the QWP arm models: the
# client is on a DIFFERENT MACHINE, so a power cut kills the server and the client survives to
# reconnect. Re-boot with the SAME QWPPORT and the client's reconnect policy finds the server
# again at the address it already has.
vm_boot() {
    local rundir="$1" boot="$2" data="$3" port="$4" seed="${5:-}" logdisk="${6:-}" qwpport="${7:-}"
    mkdir -p "$rundir"
    : > "$rundir/console.log"

    # Clear a STALE pidfile. Retained run dirs (kept on failure, by design) still
    # hold the pidfile of the VM that was killed there, and QEMU refuses to start
    # with "cannot create PID file: Cannot lock pid file" -- which surfaces as an
    # opaque SSH timeout 240s later rather than as the trivial cause it is.
    # Only remove it if no live process holds it.
    if [ -f "$rundir/qemu.pid" ]; then
        local stale
        stale="$(cat "$rundir/qemu.pid" 2>/dev/null)"
        if [ -z "$stale" ] || ! kill -0 "$stale" 2>/dev/null; then
            rm -f "$rundir/qemu.pid"
        else
            echo "ERROR: $rundir/qemu.pid is held by live pid $stale; refusing to boot over it" >&2
            return 1
        fi
    fi

    # Overridable ONLY so a deliberately-broken configuration can be demonstrated.
    #
    # REFUSE ANY OTHER VALUE. t04 no longer mutates this (it mutates drop_writes, which is
    # what actually discriminates), so nothing in the harness sets it any more -- yet the
    # variable stayed reachable, and the two values it accepts both produce a FALSE GREEN:
    #
    #   cache=writeback   un-flushed guest writes land in the HOST page cache, which
    #                     survives `kill -9` on the VMM because the host kept its power.
    #   cache=directsync  every guest write becomes durable at once, so even NOSYNC
    #                     survives.
    #
    # And the preflight CANNOT catch either: pf_ranged is discarded by dm-flakey inside the
    # guest and never reaches QEMU's cache layer, so it returns PREFLIGHT_OK under all three
    # modes. An operator setting this to speed CI up would get a green run with no guard
    # firing anywhere. So the guard has to live here, at the point of use.
    local dcache="${QDB_VM_DATA_CACHE:-none}"
    if [ "$dcache" != "none" ] && [ "${QDB_VM_ALLOW_UNSAFE_CACHE:-0}" != "1" ]; then
        echo "REFUSING: QDB_VM_DATA_CACHE=$dcache produces a FALSE GREEN and no guard detects it." >&2
        echo "  cache=none is required: the guest page cache must die with the VMM, and a guest" >&2
        echo "  fsync must mean 'the bytes reached host storage', not 'a host cache accepted them'." >&2
        echo "  Set QDB_VM_ALLOW_UNSAFE_CACHE=1 only to demonstrate the broken configuration." >&2
        return 64
    fi

    # aio=threads, not aio=native: native requires O_DIRECT and fails outright
    # under cache=writeback, which would make t04's second direction fail for
    # the wrong reason.
    local args=(
        -enable-kvm -cpu host -smp 8 -m 16G
        -drive "file=$boot,if=virtio,format=qcow2,cache=none,aio=threads"
        -netdev "user,id=n0,hostfwd=tcp:127.0.0.1:$port-:22${qwpport:+,hostfwd=tcp:127.0.0.1:$qwpport-:9000}"
        -device virtio-net-pci,netdev=n0
        -serial "file:$rundir/console.log"
        -display none
        -pidfile "$rundir/qemu.pid"
        -daemonize
    )
    # DISCARD, on the DATA disk only, and OFF by default.
    #
    # The replay path needs to RESET the data device between boundaries (see replay_reset_cmd
    # below). A 40 GiB dd costs ~40 s per boundary and is unaffordable at 1.9 s/point; an
    # unmapping discard costs 8 ms for the whole device, measured.
    #
    # Default "ignore" because the RECORDING boot must not be perturbed: with unmap, a discard
    # issued by mkfs or by the workload becomes a real hole AND is logged by dm-log-writes as a
    # DISCARD entry, which changes what the recording contains. Only the replay boot sets this.
    #
    # QEMU's own default is also "ignore", and that is a TRAP, not a safe default: the guest
    # still advertises discard support (discard_granularity=512, discard_max_bytes=2 GiB) and
    # blkdiscard still returns success in 8 ms -- having reverted nothing. A reset built on it
    # is a silent no-op, so replay_reset_cmd's caller MUST assert the reset really zeroes.
    local ddiscard="${QDB_VM_DATA_DISCARD:-ignore}"
    case "$ddiscard" in
        ignore|unmap) ;;
        *) echo "REFUSING: QDB_VM_DATA_DISCARD=$ddiscard is not one of ignore|unmap" >&2; return 64 ;;
    esac
    [ -n "$data" ] && args+=( -drive "file=$data,if=virtio,format=raw,cache=$dcache,aio=threads,discard=$ddiscard" )
    [ -n "$logdisk" ] && args+=( -drive "file=$logdisk,if=virtio,format=raw,cache=$dcache,aio=threads" )
    [ -n "$seed" ] && args+=( -drive "file=$seed,if=virtio,format=raw,media=cdrom,readonly=on" )

    printf '%q ' qemu-system-x86_64 "${args[@]}" > "$rundir/cmdline"
    echo >> "$rundir/cmdline"
    echo "$port" > "$rundir/ssh_port"
    qemu-system-x86_64 "${args[@]}"
}

# THE DEVICE RESET, and the assertion that it is real. ONE definition, shared by every replay
# call site: run-flush-sweep.sh's main loop and densify pass, t06, t07, t10, run-sf-replay.sh
# and run-st8-probe.sh. It began life as the sweep's alone, and a reset present at some call
# sites and absent at others is worse than none -- it makes the guard and the instrument
# disagree about what a boundary means, which is how t06 came to certify a regime the
# instrument never used.
#
# WHY A RESET IS NEEDED AT ALL. dm-log-writes is a PASS-THROUGH target: during the recording
# every write reaches /dev/vdb as well as the log. After the reboot the data device therefore
# still holds the FINAL crashed state, and replaying to boundary N re-applies the writes up to
# N but cannot revert the ones issued after it. Mounting then runs ext4 journal recovery, which
# writes -- so boundary N+1 also inherits boundary N's recovered state. Without a reset,
# "everything after the boundary is gone" -- the README's central claim -- is simply not true,
# and because every payload byte is a deterministic function of the row id, the leftover future
# bytes are VALID bytes: the corruption oracle cannot tell "this block was durable" from "this
# block still holds the end-of-run value". The error is in the direction of FALSE GREEN.
#
# run-st8-probe.sh:398-406 states this and zeroes with dd. t06 zeroes 64 MiB. The instrument
# itself zeroed nothing. This is that fix, at a price the instrument can afford.
#
# QDB_REPLAY_RESET: blkdiscard (default) | none.
#   none reproduces the pre-fix behaviour and exists for ONE reason -- the A/B that measures
#   what the reset changes. It is not a performance knob; a sweep run with none is not evidence
#   about the product, only about the instrument.
replay_reset_cmd() {
    case "${QDB_REPLAY_RESET:-blkdiscard}" in
        none)       echo "true" ;;
        # -f is NOT cosmetic. Every replay puts an ext4 signature back on the device, so from
        # the second boundary on, blkdiscard sees a filesystem it is about to destroy. util-linux
        # 2.39.3 (noble) warns and proceeds with rc=0 -- MEASURED, including that the device then
        # reads as zeros -- but that behaviour is a deprecation away from a hard refusal, and a
        # refusal would silently return the sweep to replaying onto the previous boundary's
        # state. -f states the intent the sweep actually has.
        blkdiscard) echo "sudo blkdiscard -f /dev/vdb" ;;
        *) echo "false  # REFUSING: QDB_REPLAY_RESET=${QDB_REPLAY_RESET} is not one of blkdiscard|none" ;;
    esac
}

# ASSERT THE RESET IS REAL, once, before the first replay. Not optional paranoia: with QEMU's
# default discard=ignore the guest still advertises discard and blkdiscard still returns 0 in
# 8 ms having reverted NOTHING (measured). That produces exactly the blended-state sweep this
# reset exists to prevent, while looking like it worked. Writes a pattern, discards, requires
# zeros back. Destroys the passed-through final state -- which is the point, and every later
# boundary resets anyway.
replay_reset_assert() {  # PORT KEY
    local port="$1" key="$2" out
    [ "${QDB_REPLAY_RESET:-blkdiscard}" = none ] && return 0
    out=$(vm_ssh "$port" "$key" "
        zero=\$(head -c 1048576 /dev/zero | md5sum | cut -d' ' -f1)
        # SEVERAL OFFSETS, SPREAD ACROSS THE DEVICE. One sample cannot tell a full discard from
        # a PARTIAL one: a backend that honoured only the first extent -- discard_max_bytes is
        # 2 GiB here -- would zero the probed MiB and pass a one-point check while the rest of
        # the device still carried the final recorded state. That is the worst of the three
        # outcomes, because it looks exactly like success.
        #
        # DERIVED FROM THE DEVICE, not hardcoded. The callers do not agree on size: the sweep
        # and t07/t10 use 40 GiB, t06 uses 4 GiB, run-st8-probe.sh uses 2 GiB. Hardcoded 32 GiB
        # offsets seek past the end of the small ones, dd fails, and the check reports
        # RESET_PARTIAL on a device that was in fact fully reset -- a false alarm that stops a
        # green run. Measured on t06 before this was derived.
        mib=\$(( \$(sudo blockdev --getsize64 /dev/vdb) / 1048576 ))
        [ \"\$mib\" -ge 8 ] || { echo \"RESET_UNTESTABLE device is only \${mib} MiB\"; exit 1; }
        offs=\"1 \$((mib/4)) \$((mib/2)) \$((mib*3/4)) \$((mib-2))\"
        dirty=0; clean=0
        for off in \$offs; do
            sudo dd if=/dev/urandom of=/dev/vdb bs=1M count=1 seek=\$off conv=fsync status=none
        done
        $(replay_reset_cmd) || { echo 'RESET_REFUSED'; exit 1; }
        for off in \$offs; do
            a=\$(sudo dd if=/dev/vdb bs=1M count=1 skip=\$off status=none | md5sum | cut -d' ' -f1)
            if [ \"\$a\" = \"\$zero\" ]; then clean=\$((clean+1)); else dirty=\$((dirty+1)); fi
        done
        if [ \$dirty -eq 0 ]; then echo RESET_REAL
        elif [ \$clean -eq 0 ]; then echo RESET_IGNORED
        else echo \"RESET_PARTIAL clean=\$clean dirty=\$dirty\"; fi" 2>&1)
    case "$out" in
        *RESET_REAL*) return 0 ;;
        *RESET_IGNORED*)
            echo "LOUD_FAILURE: blkdiscard returned success and reverted nothing." >&2
            echo "  The data drive was booted without discard=unmap, so the reset is a no-op and" >&2
            echo "  every boundary would be replayed onto the previous boundary's state." >&2
            echo "  Boot the replay VM with QDB_VM_DATA_DISCARD=unmap." >&2
            return 1 ;;
        *RESET_UNTESTABLE*)
            echo "LOUD_FAILURE: the data device is too small to sample: $out" >&2
            return 1 ;;
        *RESET_PARTIAL*)
            echo "LOUD_FAILURE: the device reset zeroed part of the device and left the rest." >&2
            echo "  $out" >&2
            echo "  A partial reset is the worst of the three outcomes: boundaries would replay" >&2
            echo "  onto a device that is clean where a check looked and stale everywhere else." >&2
            return 1 ;;
        *) echo "LOUD_FAILURE: device reset self-check did not report RESET_REAL: $out" >&2
           return 1 ;;
    esac
}

vm_ssh() {  # PORT KEY CMD...
    local port="$1" key="$2"
    shift 2
    ssh -q -i "$key" -p "$port" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=5 \
        -o LogLevel=ERROR \
        ubuntu@127.0.0.1 "$@"
}

vm_scp() {  # PORT KEY SRC DST
    scp -q -i "$2" -P "$1" \
        -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
        "$3" "ubuntu@127.0.0.1:$4"
}

vm_scp_dir() {  # PORT KEY SRCDIR DST
    scp -qr -i "$2" -P "$1" \
        -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
        "$3" "ubuntu@127.0.0.1:$4"
}

vm_wait_ssh() {  # PORT KEY TIMEOUT
    local port="$1" key="$2" timeout="${3:-180}" i=0
    while [ "$i" -lt "$timeout" ]; do
        if vm_ssh "$port" "$key" true 2>/dev/null; then
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    echo "ERROR: guest did not answer SSH on port $port within ${timeout}s" >&2
    return 1
}

# Block until TOKEN appears on the guest's serial console. This is the join
# between the two halves of the cut: the guest arms drop_writes and writes
# CUT-ARMED, the host sees it and kills the VMM.
vm_wait_console() {  # RUNDIR TOKEN TIMEOUT
    local rundir="$1" token="$2" timeout="${3:-120}" i=0
    local ticks=$((timeout * 10))
    while [ "$i" -lt "$ticks" ]; do
        if grep -q "$token" "$rundir/console.log" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
        i=$((i + 1))
    done
    echo "ERROR: token '$token' never appeared on the console within ${timeout}s" >&2
    return 1
}

# THE POWER CUT, host half. kill -9 on the VMM: the guest kernel and its page
# cache die instantly, with no shutdown and no writeback.
#
# ORDERING IS LOAD-BEARING: this must only ever run AFTER the guest has armed
# drop_writes. Reverse the order and a write can reach durability during the
# join window, which is the exact failure this design exists to exclude.
vm_kill() {  # RUNDIR
    local pid
    pid="$(cat "$1/qemu.pid" 2>/dev/null)" || return 0
    [ -n "$pid" ] || return 0
    kill -9 "$pid" 2>/dev/null
    while kill -0 "$pid" 2>/dev/null; do sleep 0.05; done
    rm -f "$1/qemu.pid"
}

# Wait for a CLEAN exit (used after `poweroff` during image build), escalating
# to kill -9 if the guest will not go. Never used as the cut.
vm_wait_gone() {  # RUNDIR TIMEOUT
    local pid i=0
    pid="$(cat "$1/qemu.pid" 2>/dev/null)" || return 0
    [ -n "$pid" ] || return 0
    while kill -0 "$pid" 2>/dev/null && [ "$i" -lt "${2:-60}" ]; do
        sleep 1
        i=$((i + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null
    fi
    rm -f "$1/qemu.pid"
    return 0
}
