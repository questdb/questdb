#!/usr/bin/env bash
# check-host.sh — verify this host can run the VM crash harness.
#
# Exits 0 if ready, non-zero naming the first missing prerequisite. Every other script here
# calls it first, so a half-configured host fails with a specific message rather than a
# confusing downstream error.
#
# Nothing here needs root: everything requiring privilege happens inside the guest.
set -uo pipefail

fail() { echo "NOT READY: $*" >&2; exit 1; }

# 1. KVM, usable without sudo, via an ACL granting the operator rw on /dev/kvm or kvm group
#    membership.
[ -c /dev/kvm ] || fail "/dev/kvm is absent — is this bare metal with virtualisation enabled in firmware?"
{ [ -r /dev/kvm ] && [ -w /dev/kvm ]; } || \
    fail "/dev/kvm is not readable+writable by $(id -un) — need an ACL grant (setfacl -m u:$(id -un):rw /dev/kvm) or kvm group membership"

# 2. Hardware virtualisation. Without it QEMU falls back to emulation, which is slow enough to
#    change the timing of every crash point.
grep -qE '(vmx|svm)' /proc/cpuinfo || fail "CPU exposes neither vmx nor svm — hardware virtualisation unavailable"

# 2b. The disk cache mode. Any value but none produces a false green that no other guard can
#     detect (see lib/qemu.sh). Refused again at boot time; repeated here so a run stops before
#     it creates disks and boots a VM.
if [ -n "${QDB_VM_DATA_CACHE:-}" ] && [ "${QDB_VM_DATA_CACHE}" != "none" ] \
   && [ "${QDB_VM_ALLOW_UNSAFE_CACHE:-0}" != "1" ]; then
    fail "QDB_VM_DATA_CACHE=${QDB_VM_DATA_CACHE} produces a false green (see lib/qemu.sh); unset it, or set QDB_VM_ALLOW_UNSAFE_CACHE=1 to demonstrate the broken configuration"
fi

# 3. Tooling. genisoimage builds the cloud-init seed, so cloud-image-utils is not required.
for c in qemu-system-x86_64 qemu-img ssh scp ssh-keygen genisoimage curl; do
    command -v "$c" >/dev/null 2>&1 || \
        fail "missing command: $c  (install: sudo apt install qemu-system-x86 qemu-utils genisoimage)"
done

# 4. Run state with real headroom: 40G data + 20G overlay per run, and a failed run keeps its
#    disks for inspection, so budget for several.
STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
mkdir -p "$STATE_DIR" || fail "cannot create state dir $STATE_DIR"
avail_gb=$(df -BG --output=avail "$STATE_DIR" 2>/dev/null | tail -1 | tr -dc '0-9')
[ "${avail_gb:-0}" -ge 200 ] || fail "only ${avail_gb:-0}G free at $STATE_DIR; need >= 200G"

# 5. Host-safety tripwire, informational only: the harness needs no loop device at all (the
#    guest's data disk is raw /dev/vdb), and on a shared box a host loop device may be a live
#    database's filesystem. Printed because a future edit reaching for losetup would destroy
#    real data.
if command -v losetup >/dev/null 2>&1; then
    in_use=$(losetup -a 2>/dev/null | wc -l)
    [ "${in_use:-0}" -gt 0 ] && \
        echo "NOTE: ${in_use} host loop device(s) in use. This harness never touches them; keep it that way." >&2
fi

qemu_ver=$(qemu-system-x86_64 --version 2>/dev/null | head -1 | awk '{print $4}')
echo "READY: kvm ok (no sudo), qemu ${qemu_ver}, ${avail_gb}G free at $STATE_DIR"

# There is deliberately no host port guard here: this script is executed, never sourced, so a
# helper defined at this point would be unreachable by construction.
#
# The rule it would enforce still holds. The harness must never bind a host service port,
# because a server that fails to bind does not stop -- the next client talks to whatever already
# owns the port, which has meant a test client ingesting into a live user database. Today's arms
# are structurally safe: the guest binds its own 9000 inside the VM and qemu takes an ephemeral
# forward from vm_free_port. Anything that starts binding on the host needs the check in the
# script doing the binding, against `ss -ltn` rather than `ps`, since containers publish on
# 127.0.0.1 and never appear as "our" processes.
