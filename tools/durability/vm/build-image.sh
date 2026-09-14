#!/usr/bin/env bash
# build-image.sh — build the golden guest image. Idempotent: re-run to rebuild.
#
# Produces $STATE_DIR/base/golden.qcow2, a bootable Ubuntu 24.04 guest carrying
# a JDK, dmsetup, e2fsprogs and the dm-flakey / dm-log-writes modules. Runs
# create a qcow2 OVERLAY on it, so resetting a run is `rm overlay.qcow2` rather
# than a rebuild.
#
# THE JDK IS THE HOST'S, tarred and copied in — not a distro package. The repo
# builds under JDK 25, which Ubuntu 24.04 does not ship, and a class-file
# version mismatch would surface as a confusing guest-side failure. Shipping the
# host's JVM removes the skew entirely.
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
JDK_HOME="${JAVA_HOME:-/usr/lib/jvm/java-25-openjdk-amd64}"
URL="https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"

mkdir -p "$BASE"
[ -d "$JDK_HOME" ] || { echo "ERROR: no JDK at $JDK_HOME; set JAVA_HOME" >&2; exit 1; }

[ -f "$KEY" ] || ssh-keygen -t ed25519 -N '' -f "$KEY" -C qdb-vmcrash >/dev/null
if [ ! -f "$CLOUD_IMG" ]; then
    echo "downloading Ubuntu 24.04 cloud image (~600MB) ..."
    # --no-progress-meter: the progress bar renders as one enormous line in a
    # captured log, which buries the provisioning output that follows it.
    curl -fSL --retry 3 --no-progress-meter -o "$CLOUD_IMG.part" "$URL"
    mv "$CLOUD_IMG.part" "$CLOUD_IMG"
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

echo "shipping the host JDK ($JDK_HOME) ..."
# -h DEREFERENCES SYMLINKS, and that is load-bearing on Debian/Ubuntu. Their
# packaging uses a split layout where the files under $JDK_HOME/conf are
# symlinks into /etc/java-<v>-openjdk/. Archived as symlinks, they arrive in the
# guest dangling, and the JVM then dies with
#   NoSuchFileException: /opt/jdk/conf/security/java.security
# on the first code path that loads security properties. `java -version` does
# NOT take that path, so the image looks fine right up until a real workload
# runs. Dereferencing materialises the actual files.
#
# --exclude src.zip: on this layout lib/src.zip is itself a DANGLING symlink on
# the host (-> ../../openjdk-25/src.zip, provided by a separate sources package
# that need not be installed). With -h, tar tries to follow it, fails, and exits
# non-zero -- which under `set -e` aborts the build midway and leaves a golden
# image with no JDK at all. It is JDK source code; nothing at runtime reads it.
#
# --ignore-failed-read: any OTHER unreadable entry should warn rather than kill
# the build. The post-install assertions below are what decide whether the
# result is actually usable -- a missing file that matters will fail there, with
# a message naming it, instead of here with a bare tar warning.
tar -h --exclude='*/lib/src.zip' --ignore-failed-read \
    -C "$(dirname "$JDK_HOME")" -czf "$WORK/jdk.tgz" "$(basename "$JDK_HOME")"
vm_scp "$PORT" "$KEY" "$WORK/jdk.tgz" /tmp/jdk.tgz
vm_ssh "$PORT" "$KEY" "sudo mkdir -p /opt/jdk && sudo tar -C /opt/jdk --strip-components=1 -xzf /tmp/jdk.tgz && rm -f /tmp/jdk.tgz"
vm_ssh "$PORT" "$KEY" "sudo ln -sf /opt/jdk/bin/java /usr/local/bin/java"
vm_ssh "$PORT" "$KEY" "java -version" 2>&1 | head -1

# Assert the JDK is actually complete, at BUILD time. `java -version` succeeding
# is not sufficient evidence -- it was passing while java.security was dangling.
vm_ssh "$PORT" "$KEY" "test -s /opt/jdk/conf/security/java.security" \
    || { echo "ERROR: /opt/jdk/conf/security/java.security missing or empty in the guest" >&2; exit 1; }
vm_ssh "$PORT" "$KEY" "! find /opt/jdk -xtype l | grep -q ." \
    || { echo "ERROR: dangling symlinks under /opt/jdk in the guest:" >&2
         vm_ssh "$PORT" "$KEY" "find /opt/jdk -xtype l | head -20" >&2; exit 1; }
echo "JDK verified complete in the guest (no dangling symlinks)"

# Guest scripts are baked in for a clean default, but power-cut-vm.sh re-ships
# them on every run so editing one does not require an image rebuild.
vm_scp_dir "$PORT" "$KEY" "$HERE/guest" /opt/vmcrash/

vm_ssh "$PORT" "$KEY" "sudo cloud-init clean --logs" >/dev/null 2>&1 || true
vm_ssh "$PORT" "$KEY" "sudo poweroff" >/dev/null 2>&1 || true
vm_wait_gone "$BASE/provision" 90

echo "golden image ready: $GOLDEN"
