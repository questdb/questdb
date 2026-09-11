#!/usr/bin/env bash
# check-host.sh — verify this host can run the VM crash harness.
#
# Exits 0 if ready; non-zero naming the FIRST missing prerequisite. Every other
# script in this directory calls it before doing anything, so a half-configured
# host fails with a specific message rather than a confusing downstream error.
#
# Nothing here needs root. The harness deliberately requires no host privilege:
# everything that needs it happens inside the guest. See README.md.
set -uo pipefail

fail() { echo "NOT READY: $*" >&2; exit 1; }

# 1. KVM, usable WITHOUT sudo. On this class of host /dev/kvm carries an ACL
#    granting the operator rw directly; kvm group membership works too.
[ -c /dev/kvm ] || fail "/dev/kvm is absent — is this bare metal with virtualisation enabled in firmware?"
{ [ -r /dev/kvm ] && [ -w /dev/kvm ]; } || \
    fail "/dev/kvm is not readable+writable by $(id -un) — need an ACL grant (setfacl -m u:$(id -un):rw /dev/kvm) or kvm group membership"

# 2. Hardware virtualisation. Without this QEMU falls back to emulation, which
#    is slow enough to change the timing of every crash point.
grep -qE '(vmx|svm)' /proc/cpuinfo || fail "CPU exposes neither vmx nor svm — hardware virtualisation unavailable"

# 3. Tooling. genisoimage builds the cloud-init seed, so cloud-image-utils is
#    not required.
for c in qemu-system-x86_64 qemu-img ssh scp ssh-keygen genisoimage curl; do
    command -v "$c" >/dev/null 2>&1 || \
        fail "missing command: $c  (install: sudo apt install qemu-system-x86 qemu-utils genisoimage)"
done

# 4. Run state with real headroom: 40G data + 20G overlay per run, and failed
#    runs KEEP their disks for inspection, so budget for several.
STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
mkdir -p "$STATE_DIR" || fail "cannot create state dir $STATE_DIR"
avail_gb=$(df -BG --output=avail "$STATE_DIR" 2>/dev/null | tail -1 | tr -dc '0-9')
[ "${avail_gb:-0}" -ge 200 ] || fail "only ${avail_gb:-0}G free at $STATE_DIR; need >= 200G"

# 5. Host-safety tripwire. The harness must never touch a host loop device, and
#    on a shared box one may well be a live database's filesystem. We do not
#    need loop devices at all (the guest's data disk is raw /dev/vdb), so this
#    is purely informational — but it is worth printing, because a future edit
#    that reaches for losetup would be destroying real data.
if command -v losetup >/dev/null 2>&1; then
    in_use=$(losetup -a 2>/dev/null | wc -l)
    [ "${in_use:-0}" -gt 0 ] && \
        echo "NOTE: ${in_use} host loop device(s) in use. This harness never touches them; keep it that way." >&2
fi

qemu_ver=$(qemu-system-x86_64 --version 2>/dev/null | head -1 | awk '{print $4}')
echo "READY: kvm ok (no sudo), qemu ${qemu_ver}, ${avail_gb}G free at $STATE_DIR"
