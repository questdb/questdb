#!/usr/bin/env bash
# build-image.sh — build the golden guest image. Idempotent: re-run to rebuild.
#
# Produces $STATE_DIR/base/golden.qcow2, a bootable Ubuntu 24.04 guest carrying
# a JDK, dmsetup, e2fsprogs and the dm-flakey / dm-log-writes modules. Runs
# create a qcow2 OVERLAY on it, so resetting a run is `rm overlay.qcow2` rather
# than a rebuild.
#
# THE JDK IS A PINNED TEMURIN TARBALL, downloaded — not a distro package, and no
# longer the host's. Ubuntu 24.04 does not ship JDK 25, so a distro package would
# cause a class-file version mismatch; but tarring the HOST's JDK, which is what
# this used to do, has two worse problems:
#
#   * NOT REPRODUCIBLE. It shipped whatever happened to be in JAVA_HOME, so the
#     image depended on which machine built it.
#   * SILENTLY BROKEN on some layouts. A Nix/flox JDK has a bin/java that is a
#     WRAPPER SCRIPT whose shebang and exec target are absolute /nix/store paths,
#     and whose real binary is linked against the Nix loader. Neither exists in
#     the guest. Both assertions below PASS on such a JDK, and the `java -version`
#     step used to print without checking, so the build announced "golden image
#     ready" with a JVM that could not start. Measured, not hypothetical.
#
# A released Temurin tarball is self-contained by design: a real ELF bin/java with
# interpreter /lib64/ld-linux-x86-64.so.2, no split /etc symlinks, no wrappers.
# That removes the tar gymnastics (-h, --exclude src.zip, --ignore-failed-read)
# entirely, and the version is written down here rather than inherited.
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

# TO BUMP THE GUEST JDK: change this one line. Keep it >= the feature version the repo
# builds with, or the guest cannot load our class files. Deliberately an exact version and
# not "latest": "latest" silently changes underneath a cached image, and a durability result
# that depends on when the image happened to be built is not a result.
GUEST_JDK_VERSION="${QDB_GUEST_JDK_VERSION:-25.0.4.1+1}"
GUEST_JDK_FEATURE="${GUEST_JDK_VERSION%%.*}"
# The Adoptium versioned endpoint redirects to the release asset. '+' must be percent-encoded.
GUEST_JDK_URL="${QDB_GUEST_JDK_URL:-https://api.adoptium.net/v3/binary/version/jdk-${GUEST_JDK_VERSION/+/%2B}/linux/x64/jdk/hotspot/normal/eclipse}"
JDK_TGZ="$BASE/temurin-$GUEST_JDK_VERSION.tar.gz"

mkdir -p "$BASE"

[ -f "$KEY" ] || ssh-keygen -t ed25519 -N '' -f "$KEY" -C qdb-vmcrash >/dev/null
if [ ! -f "$CLOUD_IMG" ]; then
    echo "downloading Ubuntu 24.04 cloud image (~600MB) ..."
    # --no-progress-meter: the progress bar renders as one enormous line in a
    # captured log, which buries the provisioning output that follows it.
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
  - [ modprobe, dm-flakey ]
  - [ modprobe, dm-log-writes ]
  - [ sh, -c, "printf 'dm-flakey\\ndm-log-writes\\n' > /etc/modules-load.d/qdb-vmcrash.conf" ]
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

# ASSERT, do not merely print. This step used to be
#     vm_ssh ... "java -version" 2>&1 | head -1
# with no `||` and no grep -- output shown to a human, checked by nobody. A JVM that
# cannot start produced a build that still said "golden image ready", and the failure
# surfaced much later as something that looked nothing like its cause.
GUEST_JAVA_VER="$(vm_ssh "$PORT" "$KEY" "java -version" 2>&1 | head -1 || true)"
echo "  guest reports: $GUEST_JAVA_VER"
case "$GUEST_JAVA_VER" in
    *\"$GUEST_JDK_FEATURE.*|*\"$GUEST_JDK_FEATURE\"*) ;;
    *) echo "ERROR: guest java did not report feature version $GUEST_JDK_FEATURE." >&2
       echo "       got: ${GUEST_JAVA_VER:-<no output at all>}" >&2
       exit 1 ;;
esac

# Assert the JDK is actually complete, at BUILD time. `java -version` succeeding is not
# sufficient evidence on its own: it does NOT take the code path that loads security
# properties, so a JDK missing java.security passes it and dies on the first real workload.
vm_ssh "$PORT" "$KEY" "test -s /opt/jdk/conf/security/java.security" \
    || { echo "ERROR: /opt/jdk/conf/security/java.security missing or empty in the guest" >&2; exit 1; }
vm_ssh "$PORT" "$KEY" "! find /opt/jdk -xtype l | grep -q ." \
    || { echo "ERROR: dangling symlinks under /opt/jdk in the guest:" >&2
         vm_ssh "$PORT" "$KEY" "find /opt/jdk -xtype l | head -20" >&2; exit 1; }
# The guest's bin/java must be a REAL binary, not a wrapper pointing at paths that only
# exist on the build host. This is what a Nix/flox JDK failed, while passing everything above.
vm_ssh "$PORT" "$KEY" "head -c2 /opt/jdk/bin/java | grep -q \$'\\x7fE'" \
    || { echo "ERROR: /opt/jdk/bin/java is not an ELF binary -- a wrapper script was shipped" >&2
         vm_ssh "$PORT" "$KEY" "head -3 /opt/jdk/bin/java" >&2; exit 1; }
echo "JDK verified in the guest: real ELF launcher, complete, no dangling symlinks"

# Guest scripts are baked in for a clean default, but power-cut-vm.sh re-ships
# them on every run so editing one does not require an image rebuild.
vm_scp_dir "$PORT" "$KEY" "$HERE/guest" /opt/vmcrash/

vm_ssh "$PORT" "$KEY" "sudo cloud-init clean --logs" >/dev/null 2>&1 || true
vm_ssh "$PORT" "$KEY" "sudo poweroff" >/dev/null 2>&1 || true
vm_wait_gone "$BASE/provision" 90

echo "golden image ready: $GOLDEN"
