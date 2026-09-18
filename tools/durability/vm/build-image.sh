#!/usr/bin/env bash
# build-image.sh — build the golden guest image. Idempotent: re-run to rebuild.
#
# Produces $STATE_DIR/base/golden.qcow2, a bootable Ubuntu 24.04 guest carrying a JDK, dmsetup,
# e2fsprogs and the dm-log-writes module. A run creates a qcow2 overlay on it, so
# resetting a run is `rm overlay.qcow2` rather than a rebuild.
#
# The guest JDK is a pinned Temurin tarball rather than a distro package or the host's JDK.
# Ubuntu 24.04 ships no JDK 25, so a distro package gives a class-file version mismatch, and
# shipping the host's JDK makes the image depend on the machine that built it. A Nix/flox JDK is
# worse still: its bin/java is a wrapper script with absolute /nix/store paths and a binary
# linked against the Nix loader, none of which exist in the guest, so it produces an image whose
# JVM cannot start. A released Temurin tarball is self-contained by design -- a real ELF bin/java
# with interpreter /lib64/ld-linux-x86-64.so.2, no wrappers and no split /etc symlinks.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/qemu.sh
source "$HERE/lib/qemu.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"
CLOUD_IMG="$BASE/noble-cloudimg.qcow2"
GOLDEN="$BASE/golden.qcow2"
KEY="$BASE/id_ed25519"
URL="https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"

# To bump the guest JDK, change this one line. Keep it at or above the feature version the repo
# builds with, or the guest cannot load our class files. An exact version, never "latest", which
# would change underneath a cached image and make a result depend on when the image was built.
GUEST_JDK_VERSION="${QDB_GUEST_JDK_VERSION:-25.0.4.1+1}"
GUEST_JDK_FEATURE="${GUEST_JDK_VERSION%%.*}"
# The Adoptium versioned endpoint redirects to the release asset. '+' must be percent-encoded.
GUEST_JDK_URL="${QDB_GUEST_JDK_URL:-https://api.adoptium.net/v3/binary/version/jdk-${GUEST_JDK_VERSION/+/%2B}/linux/x64/jdk/hotspot/normal/eclipse}"
JDK_TGZ="$BASE/temurin-$GUEST_JDK_VERSION.tar.gz"

mkdir -p "$BASE"

[ -f "$KEY" ] || ssh-keygen -t ed25519 -N '' -f "$KEY" -C qdb-vmcrash >/dev/null
if [ ! -f "$CLOUD_IMG" ]; then
    echo "downloading Ubuntu 24.04 cloud image (~600MB) ..."
    # --no-progress-meter: the progress bar renders as one enormous line in a captured log,
    # burying the provisioning output that follows it.
    curl -fSL --retry 3 --no-progress-meter -o "$CLOUD_IMG.part" "$URL"
    mv "$CLOUD_IMG.part" "$CLOUD_IMG"
fi

# Cached in $BASE beside the cloud image, so a rebuild does not re-download it.
if [ ! -f "$JDK_TGZ" ]; then
    echo "downloading Temurin $GUEST_JDK_VERSION (~140MB) ..."
    curl -fSL --retry 3 --no-progress-meter -o "$JDK_TGZ.part" "$GUEST_JDK_URL" \
        || { echo "ERROR: could not fetch the guest JDK from $GUEST_JDK_URL" >&2; exit 1; }
    mv "$JDK_TGZ.part" "$JDK_TGZ"
fi

WORK="$(mktemp -d "$BASE/seed.XXXXXX")"
cleanup() { rm -rf "$WORK"; vm_wait_gone "$BASE/provision" 10 2>/dev/null || true; }
trap cleanup EXIT

cat > "$WORK/meta-data" <<EOF
instance-id: qdb-vmcrash
local-hostname: qdb-vmcrash
EOF

cat > "$WORK/user-data" <<EOF
#cloud-config
users:
  - name: ubuntu
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    lock_passwd: true
    ssh_authorized_keys:
      - $(cat "$KEY.pub")
package_update: true
packages:
  - dmsetup
  - e2fsprogs
  - xfsprogs
runcmd:
  - [ modprobe, dm-log-writes ]
  - [ sh, -c, "printf 'dm-log-writes\\n' > /etc/modules-load.d/qdb-vmcrash.conf" ]
  - [ mkdir, -p, /opt/vmcrash ]
  - [ chown, "ubuntu:ubuntu", /opt/vmcrash ]
  - [ touch, /var/lib/cloud/qdb-provisioned ]
EOF

genisoimage -quiet -output "$BASE/seed.iso" -volid cidata -joliet -rock \
    "$WORK/user-data" "$WORK/meta-data"

echo "preparing golden disk ..."
rm -f "$GOLDEN"
qemu-img convert -O qcow2 "$CLOUD_IMG" "$GOLDEN"
qemu-img resize "$GOLDEN" 20G >/dev/null

PORT=$(vm_free_port)
echo "booting for provisioning on port $PORT ..."
vm_boot "$BASE/provision" "$GOLDEN" "" "$PORT" "$BASE/seed.iso"
vm_wait_ssh "$PORT" "$KEY" 300 || { echo "ERROR: provisioning boot never answered SSH" >&2; cat "$BASE/provision/console.log" | tail -40 >&2; exit 1; }

# cloud-init runs asynchronously; wait for its marker before relying on packages.
for _ in $(seq 1 120); do
    vm_ssh "$PORT" "$KEY" "test -f /var/lib/cloud/qdb-provisioned" 2>/dev/null && break
    sleep 2
done
vm_ssh "$PORT" "$KEY" "test -f /var/lib/cloud/qdb-provisioned" \
    || { echo "ERROR: cloud-init never completed" >&2; exit 1; }

echo "shipping Temurin $GUEST_JDK_VERSION ..."
vm_scp "$PORT" "$KEY" "$JDK_TGZ" /tmp/jdk.tgz
vm_ssh "$PORT" "$KEY" "sudo mkdir -p /opt/jdk && sudo tar -C /opt/jdk --strip-components=1 -xzf /tmp/jdk.tgz && rm -f /tmp/jdk.tgz"
vm_ssh "$PORT" "$KEY" "sudo ln -sf /opt/jdk/bin/java /usr/local/bin/java"

# Assert the version rather than printing it: a JVM that cannot start must fail the build here,
# not surface much later as something that looks nothing like its cause.
GUEST_JAVA_VER="$(vm_ssh "$PORT" "$KEY" "java -version" 2>&1 | head -1 || true)"
echo "  guest reports: $GUEST_JAVA_VER"
case "$GUEST_JAVA_VER" in
    *\"$GUEST_JDK_FEATURE.*|*\"$GUEST_JDK_FEATURE\"*) ;;
    *) echo "ERROR: guest java did not report feature version $GUEST_JDK_FEATURE." >&2
       echo "       got: ${GUEST_JAVA_VER:-<no output at all>}" >&2
       exit 1 ;;
esac

# Assert the JDK is complete at build time. `java -version` succeeding is not sufficient
# evidence: it does not take the code path that loads security properties, so a JDK missing
# java.security passes it and dies on the first real workload.
vm_ssh "$PORT" "$KEY" "test -s /opt/jdk/conf/security/java.security" \
    || { echo "ERROR: /opt/jdk/conf/security/java.security missing or empty in the guest" >&2; exit 1; }
vm_ssh "$PORT" "$KEY" "! find /opt/jdk -xtype l | grep -q ." \
    || { echo "ERROR: dangling symlinks under /opt/jdk in the guest:" >&2
         vm_ssh "$PORT" "$KEY" "find /opt/jdk -xtype l | head -20" >&2; exit 1; }
# The guest's bin/java must be a real binary, not a wrapper pointing at paths that exist only on
# the build host. A Nix/flox JDK passes every check above and fails this one.
vm_ssh "$PORT" "$KEY" "head -c2 /opt/jdk/bin/java | grep -q \$'\\x7fE'" \
    || { echo "ERROR: /opt/jdk/bin/java is not an ELF binary -- a wrapper script was shipped" >&2
         vm_ssh "$PORT" "$KEY" "head -3 /opt/jdk/bin/java" >&2; exit 1; }
echo "JDK verified in the guest: real ELF launcher, complete, no dangling symlinks"

# Guest scripts are baked in for a clean default, but each run re-ships them, so editing one does
# not require an image rebuild.
vm_scp_dir "$PORT" "$KEY" "$HERE/guest" /opt/vmcrash/

vm_ssh "$PORT" "$KEY" "sudo cloud-init clean --logs" >/dev/null 2>&1 || true
vm_ssh "$PORT" "$KEY" "sudo poweroff" >/dev/null 2>&1 || true
vm_wait_gone "$BASE/provision" 90

echo "golden image ready: $GOLDEN"
