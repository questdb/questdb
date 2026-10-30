# dm-flakey Power-Cut Durability — Manual Step-by-Step

This is a manual SYNC/NOSYNC, non-WAL example of the `dm-flakey` mechanism. For adaptive WAL,
the group window and its frontier oracle, use the current `power-cut-dmflakey.sh` or the
[VM replay harness](vm/README.md); do not change just MODE in this example.

These commands format a disposable device and kill a test JVM. Run them only on a dedicated
Linux test machine, as root. Set WT to the checkout and use its supported JDK and built native
libraries. No harness execution is implied by this documentation; record revisions and results
for each actual run.

---

## Prerequisites

```bash
# dm-flakey kernel module (standard since ~4.0)
modprobe dm-flakey
lsmod | grep dm_flakey      # should show dm_flakey

# Tools: dmsetup, losetup, mkfs.ext4, blockdev
which dmsetup losetup mkfs.ext4 blockdev

# Use the JDK required by this checkout (Java 25+ for this revision).
java -version
# Set WT and check the built benchmarks jar in the next section.
```

---

## Variables (set these once in your shell)

```bash
WT="/absolute/path/to/questdb"
JAR="$WT/benchmarks/target/benchmarks.jar"
RUN_ID="manual-$$"
IMG="/data/qdb-pcut-$RUN_ID.img"     # disposable; on disk, not tmpfs
MNT="/mnt/qdbpcut-$RUN_ID"
DM_NAME="qdbflakey-$RUN_ID"
DM_DEV="/dev/mapper/$DM_NAME"
DBDIR="$MNT/db"
MODE="SYNC"                  # change to NOSYNC for the contrast run
MIN_COMMITTED=50000
EVIDENCE=$(mktemp -d /var/tmp/qdb-pcut-evidence.XXXXXX)  # outside the tested filesystem
QDB_JVM=(
  --enable-native-access=ALL-UNNAMED
  --sun-misc-unsafe-memory-access=allow
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.time.zone=ALL-UNNAMED
  --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
)
test -f "$JAR" || exit 1
test ! -e "$IMG" && test ! -e "$MNT" && test ! -e "$DM_DEV" || exit 1
```

Do not reuse an existing image, mapper name or mount. Keep the evidence directory after cleanup.
The JVM flags must match the checkout's harness; without the continuation export, workers can die
and invalidate the experiment.

---

## dm-flakey table string syntax

```
"<start_sector> <num_sectors> flakey <real_dev> <offset> <up_interval> <down_interval> [<num_features> <feature>...]"
```

| Field | Meaning |
|-------|---------|
| `start_sector` | Always 0 for a full-device mapping |
| `num_sectors` | From `blockdev --getsz` on the loop device |
| `real_dev` | The underlying device (loop device here) |
| `offset` | Sector offset into `real_dev`; 0 = start |
| `up_interval` | Seconds the device passes writes through |
| `down_interval` | Seconds the device is "failed" |
| `drop_writes` | Feature: silently discard writes (not fail them) during down phase |

**Always UP (pass-through):**
```
"0 $SECTORS flakey $LOOP 0 180 0"
```
up=180s, down=0s, no features → never enters down phase.

**Always DOWN + drop_writes:**
```
"0 $SECTORS flakey $LOOP 0 0 180 1 drop_writes"
```
up=0s, down=180s, 1 feature: `drop_writes` → always in down phase, all writes silently discarded.

> **Why `drop_writes` and not just down?**
> Plain "down" mode returns I/O errors to the kernel, which makes ext4 remount read-only.
> `drop_writes` silently drops writes (returns success) so the filesystem keeps writing —
> just none of it reaches the image. This models losing writes still above the mapper at the cut,
> not every physical power-loss state. Unflushed writes that reached the image earlier survive too.

---

## Step 1 — Create the disk image and loop device

```bash
truncate -s 4G "$IMG"
LOOP=$(losetup -f --show "$IMG")
echo "LOOP=$LOOP"
SECTORS=$(blockdev --getsz "$LOOP")
echo "SECTORS=$SECTORS"
```

---

## Step 2 — Create dm-flakey in always-UP mode

```bash
dmsetup create "$DM_NAME" --table "0 $SECTORS flakey $LOOP 0 180 0"
dmsetup table "$DM_NAME"         # verify: should show the UP table
ls -la "$DM_DEV"                 # /dev/mapper/qdbflakey should exist
```

---

## Step 3 — Format ext4 and mount

```bash
mkfs.ext4 -F -q "$DM_DEV"
mkdir -p "$MNT"
mount "$DM_DEV" "$MNT"
mountpoint "$MNT"                # confirms mount
mkdir -p "$DBDIR"
```

---

## Step 4 — Start CrashIngestWriter

```bash
java "${QDB_JVM[@]}" -cp "$JAR" \
    -DcommitMode="$MODE" -Dwal.table=false \
    org.questdb.CrashIngestWriter "$DBDIR" \
    > "$EVIDENCE/writer.log" 2>&1 &
WRITER_PID=$!
echo "WRITER_PID=$WRITER_PID"
```

Watch progress:

```bash
# In another terminal (or poll in this one):
tail -f "$EVIDENCE/writer.log"
# or:
watch -n1 "cat $DBDIR/_progress 2>/dev/null || echo waiting"
```

---

## Step 5 — Wait until MIN_COMMITTED rows committed, then capture COMMITTED

Wait until `_progress` shows >= 50000 rows:

```bash
while true; do
    val=$(cat "$DBDIR/_progress" 2>/dev/null || echo 0)
    echo "_progress=$val"
    [ "$val" -ge "$MIN_COMMITTED" ] && break
    sleep 1
done
sleep 2    # let a couple more commits land
```

**Capture COMMITTED into a shell variable** — this must happen BEFORE the kill and cut,
because the `_progress` file itself may not survive the power cut (NOSYNC) and we need
this number to interpret the verifier result:

```bash
cp "$DBDIR/_progress" "$EVIDENCE/progress-before-cut"
COMMITTED=$(head -1 "$EVIDENCE/progress-before-cut")
echo "COMMITTED=$COMMITTED  (keep the evidence file)"
```

---

## Step 6 — Kill the writer (page cache still intact)

```bash
kill -9 "$WRITER_PID"
wait "$WRITER_PID" 2>/dev/null
echo "writer gone"
```

At this point the page cache still holds all dirty pages.  No data has been cut yet.

---

## Step 7 — Drop later writes

This is the critical sequence. Execute them in order without pausing.

```bash
# 7a. Suspend WITHOUT freezing/flushing the filesystem first.
# Plain suspend can flush the very dirty state the experiment needs to lose.
dmsetup suspend --nolockfs "$DM_NAME"

# 7b. Load the "always DOWN + drop_writes" table
dmsetup load "$DM_NAME" --table "0 $SECTORS flakey $LOOP 0 0 180 1 drop_writes"

# 7c. Resume — from now on ALL writes are silently discarded
dmsetup resume "$DM_NAME"

# 7d. Unmount — the kernel writes back dirty pages, dm-flakey DROPS them
#     Writes still dirty at the cut are discarded; earlier background writes remain.
umount "$MNT"

# 7e. Suspend again to switch back to UP mode
dmsetup suspend "$DM_NAME"

# 7f. Reload the always-UP (pass-through) table
dmsetup load "$DM_NAME" --table "0 $SECTORS flakey $LOOP 0 180 0"

# 7g. Resume pass-through
dmsetup resume "$DM_NAME"
```

The write-drop phase is complete. The image contains writes that reached it before the drop
phase, whether from explicit flushes or background writeback. A passing sample does not prove
that every required barrier exists; run negative controls and retain the storage configuration.

---

## Step 8 — Remount and verify

```bash
mount "$DM_DEV" "$MNT"
echo "=== MODE=$MODE committed_before_cut=$COMMITTED ==="
java "${QDB_JVM[@]}" -cp "$JAR" -DcommitMode="$MODE" -Dwal.table=false \
    org.questdb.CrashVerifier "$DBDIR" > "$EVIDENCE/verifier.log" 2>&1
cat "$EVIDENCE/verifier.log"
# Compare count to the external COMMITTED value, not just the on-disk watermark.
```

---

## Step 9 — Interpret the result

| Scenario | Expected | Meaning |
|----------|----------|---------|
| SYNC + `CONSISTENT` + count >= COMMITTED | **DURABLE** | fsync'd data survived power cut |
| SYNC + `CONSISTENT` + count < COMMITTED | **DURABILITY_FAILURE** | SYNC lost data — serious |
| SYNC + `SILENT_CORRUPTION` | **DURABILITY_FAILURE** | corrupted data — serious |
| SYNC + `LOUD_FAILURE` | Investigate | torn metadata; engine detected it |
| NOSYNC + any | Expected loss | page cache discarded; NOSYNC makes no durability promise |

---

## Step 10 — Cleanup

```bash
umount "$MNT" 2>/dev/null || true
dmsetup remove "$DM_NAME"
losetup -d "$LOOP"
rm -f "$IMG"
```

---

## Troubleshooting

**`dmsetup load` fails with "Invalid argument"**

Check your kernel's dm-flakey supports `drop_writes`:

```bash
dmsetup targets | grep flakey
# Look for the version; drop_writes was added in kernel 3.9 / dm-flakey v1.1
```

If `drop_writes` is not recognised, stop and inspect the kernel's target documentation and
`dmesg`. Do not substitute `error_writes`: reporting I/O errors is a different failure model
and can remount the filesystem read-only.

**`umount: target is busy`**

The writer wasn't killed cleanly or a shell still has a CWD inside `$MNT`:

```bash
fuser -mv "$MNT"      # list what has the mount open
# Stop only this experiment's remaining holders, then retry a normal unmount.
umount "$MNT"
```

Do not proceed to remount after a lazy unmount: return from `umount -l` does not establish
that the old mount and its pending writeback are gone. Record this as an incomplete run.

**`mount` after step 7g fails with "wrong fs type" or journal errors**

The journal may be partially on disk (metadata goes via `jbd2` which has its own
fsync path). Try:

```bash
e2fsck -fn "$DM_DEV"   # check without fixing; see what's damaged
# or mount with journal recovery disabled:
mount -o ro,noload "$DM_DEV" "$MNT"
```

Preserve the failed image before any repair. Changing journal or mount settings creates a
new experiment; it must not turn the original failure into a reported pass. Diagnose whether
the failure came from setup, the failure model, storage or the database.

**SYNC shows DURABILITY_FAILURE (count < COMMITTED)**

Possible causes:

1. Filesystem mounted with `barrier=0` or `nobarrier` — write barriers are disabled,
   which means the disk may reorder writes past the fsync.  Check:
   ```bash
   cat /proc/mounts | grep "$MNT"
   # look for barrier= option
   # ext4 has barriers ON by default; check /sys/block/... queue/write_cache
   ```
2. The loop device itself uses a writeback cache — loop devices inherit the underlying
   file's block I/O but barriers may not propagate correctly on older kernels.  Prefer
   a real block device (e.g. `/dev/sdb`) over a loop device for production validation.
3. QuestDB's SYNC path is calling `msync(MS_SYNC)` but a kernel version change altered
   when mmap-backed pages reach the block layer. Check `dmesg` and the actual mount/device
   settings. This feature adds no startup warning that certifies barriers are enabled.

**The `_progress` file disappears after the cut (NOSYNC run)**

That is expected and correct: `_progress` is written with a `rename(2)` which is
metadata — even with ext4 `data=ordered` mode, the data blocks may not be flushed
before the power cut.  This is why the script captures `COMMITTED` into a shell
variable BEFORE the cut, not by re-reading the file after remount.
