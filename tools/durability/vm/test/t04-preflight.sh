#!/usr/bin/env bash
# t04 — the preflight gate, in BOTH directions.
#
# Direction 1: with a real cut (dm-flakey drop_writes armed), the preflight PASSES.
# Direction 2: with the cut DEFANGED (armed without drop_writes), it must FAIL.
#
# Direction 2 is the point of this test. A guard that cannot fail is decorative,
# and every durability verdict downstream of a decorative guard is vacuous.
#
# WHY THE MUTATION IS drop_writes AND NOT THE QEMU CACHE MODE.
# An earlier version of this test mutated the cache mode (none vs writeback) and
# reported a false alarm, because that variable is not observable here:
#
#   * A post-arm write is discarded by dm-flakey INSIDE THE GUEST. It never
#     reaches virtio, so it never reaches QEMU's cache layer — the cache mode is
#     masked entirely for the probe that matters.
#   * An un-fsync'd write the guest kernel already wrote back survives a VMM kill
#     under EVERY cache mode, because killing the VMM does not take the host's
#     power with it. cache=none lands it on host storage; cache=writeback lands
#     it in the host page cache; the host is alive in both cases, so both persist.
#
# cache=none is still required (see lib/qemu.sh) — it keeps flush semantics
# honest and matters under real host power loss — but it is NOT what delivers
# un-flushed-write loss in this harness. drop_writes is. So drop_writes is what
# the negative control must remove.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=../lib/verdict.sh
source "$HERE/lib/verdict.sh"
# shellcheck source=../lib/preflight.sh
source "$HERE/lib/preflight.sh"

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"
KEY="$BASE/id_ed25519"

good=$(run_preflight_cycle real "$HERE")
[ "$good" = "PREFLIGHT_OK" ] || { echo "FAIL t04: real cut gave $good, expected PREFLIGHT_OK"; exit 1; }

bad=$(run_preflight_cycle defanged "$HERE")
[ "$bad" = "PREFLIGHT_FAILED" ] || {
    echo "FAIL t04: a cut armed WITHOUT drop_writes gave $bad, expected PREFLIGHT_FAILED."
    echo "  The preflight cannot fire, so it proves nothing and neither does any"
    echo "  durability verdict that depends on it."
    exit 1
}

echo "PASS t04 (guard fires: real=$good defanged=$bad)"
