#!/usr/bin/env bash
# guest/build-dm-log-writes.sh PATCH — run as root in the guest by build-image.sh.
#
# Replaces Ubuntu's dm-log-writes module with the same source plus PATCH (the flush-order fix in
# kernel/dm-log-writes-flush-order.patch), built for the running kernel. The stock module stays on
# disk, unused, for t14 to load. Writes the facts t01 and t14 check to $FACTS.
#
# Every step must succeed or the image build fails: an image that quietly kept the stock module
# would record overlapping flushes out of order and nothing downstream would say so.
set -euo pipefail

PATCH="${1:?usage: build-dm-log-writes.sh PATCH}"
FACTS=/etc/qdb-vmcrash/dm-log-writes.env
KVER="$(uname -r)"
BASEVER="${KVER%%-*}"
fail() { echo "ERROR build-dm-log-writes: $*" >&2; exit 1; }

# The source must be the source of the module that runs. Ubuntu publishes linux-source-<base>
# for every kernel version it ships, so take the one matching the running kernel's package.
PKGVER="$(dpkg-query -W -f='${Version}' "linux-image-$KVER")" || fail "no package for kernel $KVER"
STOCK_KO="$(modinfo -n dm-log-writes)" || fail "no stock dm-log-writes module for $KVER"
STOCK_SRCVERSION="$(modinfo -F srcversion "$STOCK_KO")"
[ -n "$STOCK_SRCVERSION" ] || fail "stock module $STOCK_KO has no srcversion"

export DEBIAN_FRONTEND=noninteractive NEEDRESTART_SUSPEND=1
apt-get install -y -q "linux-headers-$KVER" build-essential >/dev/null \
    || fail "could not install linux-headers-$KVER and build-essential"

W="$(mktemp -d)"
trap 'rm -rf "$W"' EXIT
cd "$W"
# As root into a private directory: the _apt sandbox user could not write to it.
apt-get download -q -o APT::Sandbox::User=root "linux-source-$BASEVER=$PKGVER" >/dev/null \
    || fail "could not download linux-source-$BASEVER=$PKGVER"
dpkg-deb --fsys-tarfile linux-source-"$BASEVER"_*.deb \
    | tar -xO "./usr/src/linux-source-$BASEVER/linux-source-$BASEVER.tar.bz2" \
    | tar -xjO "linux-source-$BASEVER/drivers/md/dm-log-writes.c" > dm-log-writes.c
rm -f linux-source-"$BASEVER"_*.deb
[ -s dm-log-writes.c ] || fail "drivers/md/dm-log-writes.c not found in linux-source-$BASEVER=$PKGVER"

build() {  # DIR: builds DIR/dm-log-writes.c, prints the module's srcversion
    printf 'obj-m := dm-log-writes.o\n' > "$1/Makefile"
    make -s -C "/lib/modules/$KVER/build" M="$W/$1" modules >"$W/$1.log" 2>&1 \
        || { cat "$W/$1.log" >&2; fail "build of the $1 module failed"; }
    modinfo -F srcversion "$W/$1/dm-log-writes.ko"
}

# Built unchanged first: an equal srcversion proves this file is the source of the shipped module,
# so the fix lands on exactly the code that runs and on nothing else.
mkdir stock fixed
cp dm-log-writes.c stock/
[ "$(build stock)" = "$STOCK_SRCVERSION" ] \
    || fail "linux-source-$BASEVER=$PKGVER does not rebuild the shipped module (srcversion $STOCK_SRCVERSION)"

mkdir -p fixed/drivers/md
cp dm-log-writes.c fixed/drivers/md/
(cd fixed && patch -p1 --quiet < "$PATCH") || fail "$PATCH does not apply to linux-source-$BASEVER=$PKGVER"
mv fixed/drivers/md/dm-log-writes.c fixed/
FIXED_SRCVERSION="$(build fixed)"
[ "$FIXED_SRCVERSION" != "$STOCK_SRCVERSION" ] || fail "the patched module has the stock srcversion"

# updates/ comes before the kernel's own tree in Ubuntu's depmod search order.
FIXED_KO="/lib/modules/$KVER/updates/dm-log-writes.ko"
install -D -m 0644 fixed/dm-log-writes.ko "$FIXED_KO"
depmod -a "$KVER"
[ "$(modinfo -n dm-log-writes)" = "$FIXED_KO" ] || fail "modprobe would still pick $(modinfo -n dm-log-writes)"
if lsinitramfs "/boot/initrd.img-$KVER" 2>/dev/null | grep -q 'dm-log-writes'; then
    update-initramfs -u -k "$KVER" >/dev/null
fi

if lsmod | grep -q '^dm_log_writes '; then
    rmmod dm_log_writes
fi
modprobe dm-log-writes
[ "$(cat /sys/module/dm_log_writes/srcversion)" = "$FIXED_SRCVERSION" ] \
    || fail "the loaded module is not the patched one"

mkdir -p "$(dirname "$FACTS")"
cat > "$FACTS" <<EOF
KVER=$KVER
SOURCE=linux-source-$BASEVER=$PKGVER
STOCK_KO=$STOCK_KO
STOCK_SRCVERSION=$STOCK_SRCVERSION
FIXED_KO=$FIXED_KO
FIXED_SRCVERSION=$FIXED_SRCVERSION
EOF
apt-get clean
echo "dm-log-writes: patched module installed for $KVER (stock $STOCK_SRCVERSION, patched $FIXED_SRCVERSION)"
