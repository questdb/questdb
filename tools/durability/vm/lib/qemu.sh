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
vm_boot() {
    local rundir="$1" boot="$2" data="$3" port="$4" seed="${5:-}" logdisk="${6:-}"
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

    # Overridable ONLY so test/t04-preflight.sh can prove the preflight guard
    # actually fires under a knowingly-broken configuration. Never override in
    # a real run.
    local dcache="${QDB_VM_DATA_CACHE:-none}"

    # aio=threads, not aio=native: native requires O_DIRECT and fails outright
    # under cache=writeback, which would make t04's second direction fail for
    # the wrong reason.
    local args=(
        -enable-kvm -cpu host -smp 8 -m 16G
        -drive "file=$boot,if=virtio,format=qcow2,cache=none,aio=threads"
        -netdev "user,id=n0,hostfwd=tcp:127.0.0.1:$port-:22"
        -device virtio-net-pci,netdev=n0
        -serial "file:$rundir/console.log"
        -display none
        -pidfile "$rundir/qemu.pid"
        -daemonize
    )
    [ -n "$data" ] && args+=( -drive "file=$data,if=virtio,format=raw,cache=$dcache,aio=threads" )
    [ -n "$logdisk" ] && args+=( -drive "file=$logdisk,if=virtio,format=raw,cache=$dcache,aio=threads" )
    [ -n "$seed" ] && args+=( -drive "file=$seed,if=virtio,format=raw,media=cdrom,readonly=on" )

    printf '%q ' qemu-system-x86_64 "${args[@]}" > "$rundir/cmdline"
    echo >> "$rundir/cmdline"
    echo "$port" > "$rundir/ssh_port"
    qemu-system-x86_64 "${args[@]}"
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
