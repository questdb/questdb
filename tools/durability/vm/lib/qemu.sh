# lib/qemu.sh — VM lifecycle primitives for the crash harness.
# Source this file; do not execute it.
#
# Every disk is attached cache=none, and that is durability-critical:
#
#   cache=writeback  — the host page cache holds the guest's un-flushed writes and survives the
#                      VMM being killed, because the host did not lose power. False green.
#   cache=directsync — every guest write becomes durable immediately, so even NOSYNC survives.
#                      False green.
#   cache=none       — O_DIRECT, no host page cache, so the guest page cache dies with the VMM.

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

# vm_boot RUNDIR BOOT DATA SSHPORT [SEED] [LOGDISK] [QWPPORT]
#
# Boots daemonized; writes $RUNDIR/qemu.pid, $RUNDIR/console.log and $RUNDIR/cmdline. DATA may
# be "" for no data disk.
#
# LOGDISK is attached as /dev/vdc and holds the dm-log-writes log. It must be a separate disk:
# the log records the data device's traffic and cannot be part of it.
#
# QWPPORT forwards the guest's 9000 to that host port, so a client running outside the VM can
# reach the server. That is the deployment the QWP arm models -- the client is on a different
# machine, survives the cut, and after a reboot with the same QWPPORT its own reconnect policy
# finds the server at the address it already has.
vm_boot() {
    local rundir="$1" boot="$2" data="$3" port="$4" seed="${5:-}" logdisk="${6:-}" qwpport="${7:-}"
    mkdir -p "$rundir"
    : > "$rundir/console.log"

    # Clear a stale pidfile. A run dir kept on failure still holds the pidfile of the VM killed
    # there, and QEMU then refuses to start with a pid-file lock error that surfaces as an
    # opaque SSH timeout minutes later. Only remove it when no live process holds it.
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

    # Overridable only so a deliberately broken configuration can be demonstrated. Both other
    # values produce a false green that nothing downstream can detect, so the refusal lives here
    # at the point of use.
    local dcache="${QDB_VM_DATA_CACHE:-none}"
    if [ "$dcache" != "none" ] && [ "${QDB_VM_ALLOW_UNSAFE_CACHE:-0}" != "1" ]; then
        echo "REFUSING: QDB_VM_DATA_CACHE=$dcache produces a FALSE GREEN and no guard detects it." >&2
        echo "  cache=none is required: the guest page cache must die with the VMM, and a guest" >&2
        echo "  fsync must mean 'the bytes reached host storage', not 'a host cache accepted them'." >&2
        echo "  Set QDB_VM_ALLOW_UNSAFE_CACHE=1 only to demonstrate the broken configuration." >&2
        return 64
    fi

    # aio=threads, not aio=native: native requires O_DIRECT and fails outright under
    # cache=writeback, which the unsafe-cache escape hatch above must still be able to boot.
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
    # Discard, on the data disk only, and off by default. The replay path resets the data device
    # between boundaries (replay_reset_cmd below) and needs an unmapping discard to do that at a
    # price a per-boundary loop can afford.
    #
    # The recording boot must not enable it: with unmap, a discard issued by mkfs or by the
    # workload punches a real hole and dm-log-writes records a DISCARD entry, which changes what
    # the recording contains. Only the replay boot sets this.
    #
    # QEMU's default "ignore" is a trap rather than a safe default: the guest still advertises
    # discard support and blkdiscard still returns success having reverted nothing, so a reset
    # built on it is a silent no-op. That is what replay_reset_assert exists to catch.
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

# The device reset between replayed boundaries. One definition, shared by every replay call
# site: a reset present at some of them and absent at others makes the guard and the instrument
# disagree about what a boundary means.
#
# dm-log-writes is a pass-through target, so during the recording every write reaches /dev/vdb
# as well as the log. After the reboot the data device still holds the final crashed state, and
# replaying to boundary N re-applies the writes up to N without reverting the ones issued after
# it; mounting then runs ext4 journal recovery, which writes, so boundary N+1 inherits boundary
# N's recovered state too. Without a reset, "everything after the boundary is gone" is not true.
# Every payload byte is a deterministic function of the row id, so the leftover future bytes are
# valid bytes: the oracle cannot tell a block that was durable from one still holding the
# end-of-run value, and the error runs towards false green.
#
# QDB_REPLAY_RESET: blkdiscard (default) | none. `none` exists for the A/B that measures what
# the reset changes, and is not a performance knob -- a sweep run with none is evidence about
# the instrument, not about the product.
replay_reset_cmd() {
    case "${QDB_REPLAY_RESET:-blkdiscard}" in
        none)       echo "true" ;;
        # -f is not cosmetic. Every replay puts an ext4 signature back on the device, so from the
        # second boundary on blkdiscard sees a filesystem it is about to destroy. It warns and
        # proceeds today, but a refusal would silently return the sweep to replaying onto the
        # previous boundary's state.
        blkdiscard) echo "sudo blkdiscard -f /dev/vdb" ;;
        *) echo "false  # REFUSING: QDB_REPLAY_RESET=${QDB_REPLAY_RESET} is not one of blkdiscard|none" ;;
    esac
}

# Assert the reset is real, once, before the first replay. With QEMU's default discard=ignore
# the guest still advertises discard and blkdiscard still returns success having reverted
# nothing, which produces the blended-state sweep the reset exists to prevent while looking like
# it worked. Writes a pattern, discards, and requires zeros back. This destroys the
# passed-through final state, which is the point.
replay_reset_assert() {  # PORT KEY
    local port="$1" key="$2" out
    [ "${QDB_REPLAY_RESET:-blkdiscard}" = none ] && return 0
    out=$(vm_ssh "$port" "$key" "
        zero=\$(head -c 1048576 /dev/zero | md5sum | cut -d' ' -f1)
        # Several offsets, spread across the device. One sample cannot tell a full discard from a
        # partial one: a backend that honoured only the first extent would zero the probed MiB
        # and pass a one-point check while the rest still carried the final recorded state, which
        # is the worst outcome because it looks exactly like success.
        #
        # The offsets are derived from the device rather than hardcoded, because call sites use
        # different device sizes: an offset past the end makes dd fail and reports RESET_PARTIAL
        # on a device that was in fact fully reset.
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

# Kill the VM on every exit path, not just the ones someone remembered. A leaked VM is not
# merely a stray process: it holds its qemu.pid, reap-state.sh refuses any directory whose pid
# is alive, so the run dir becomes permanently unreapable and surfaces later at check-host.sh's
# free-space gate looking like an infrastructure outage.
#
# This kills only. It does not remove the run dir, because keeping the disks on failure is
# deliberate elsewhere in this harness. vm_kill is idempotent, so an explicit vm_kill on the
# success path stays correct.
VM_KILL_ON_EXIT_DIR=""
vm_kill_on_exit() {  # RUNDIR
    VM_KILL_ON_EXIT_DIR="$1"
    trap '[ -n "$VM_KILL_ON_EXIT_DIR" ] && vm_kill "$VM_KILL_ON_EXIT_DIR"' EXIT INT TERM
}

# kill -9 on the VMM: the guest kernel and its page cache die instantly, with no shutdown and no
# writeback. This is the power cut, host half.
vm_kill() {  # RUNDIR
    local pid
    pid="$(cat "$1/qemu.pid" 2>/dev/null)" || return 0
    [ -n "$pid" ] || return 0
    kill -9 "$pid" 2>/dev/null
    while kill -0 "$pid" 2>/dev/null; do sleep 0.05; done
    rm -f "$1/qemu.pid"
}

# Wait for a clean exit (used after `poweroff` during image build), escalating to kill -9 if the
# guest will not go. Never used as the cut.
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
