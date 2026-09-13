# dm-flakey Power-Cut Durability — Manual Step-by-Step

This document mirrors `power-cut-dmflakey.sh` but gives each command individually so you can
run them interactively, inspect state between steps, and debug if the all-in-one script hits
a timing or kernel issue (the script is UNTESTED — no root access in the dev environment).

Run every command below as **root** (`sudo -i` or `sudo bash` for each line).

---

## Prerequisites

```bash
# dm-flakey kernel module (standard since ~4.0)
modprobe dm-flakey
lsmod | grep dm_flakey      # should show dm_flakey

# Tools: dmsetup, losetup, mkfs.ext4, blockdev
which dmsetup losetup mkfs.ext4 blockdev

# Java 21+ and the benchmarks jar
java -version
ls -lh ~/claude/wt/oss/sync-batch/benchmarks/target/benchmarks.jar
```

---

## Variables (set these once in your shell)

```bash
WT="$HOME/claude/wt/oss/sync-batch"
JAR="$WT/benchmarks/target/benchmarks.jar"
IMG="/data/qdb-pcut.img"     # on a REAL disk; NOT tmpfs
MNT="/mnt/qdbpcut"
DM_NAME="qdbflakey"
DM_DEV="/dev/mapper/$DM_NAME"
DBDIR="$MNT/db"
MODE="SYNC"                  # change to NOSYNC for the contrast run
MIN_COMMITTED=50000
```

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
> just none of it reaches the image. That is the correct model for a power cut.

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
java -cp "$JAR" \
    -DcommitMode="$MODE" \
    org.questdb.CrashIngestWriter "$DBDIR" \
    > /tmp/writer.log 2>&1 &
WRITER_PID=$!
echo "WRITER_PID=$WRITER_PID"
```

Watch progress:

```bash
# In another terminal (or poll in this one):
tail -f /tmp/writer.log
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
COMMITTED=$(cat "$DBDIR/_progress")
echo "COMMITTED=$COMMITTED  (write this down!)"
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

## Step 7 — THE POWER CUT (6 commands)

This is the critical sequence. Execute them in order without pausing.

```bash
# 7a. Suspend the dm device (queues any in-flight I/O)
dmsetup suspend "$DM_NAME"

# 7b. Load the "always DOWN + drop_writes" table
dmsetup load "$DM_NAME" --table "0 $SECTORS flakey $LOOP 0 0 180 1 drop_writes"

# 7c. Resume — from now on ALL writes are silently discarded
dmsetup resume "$DM_NAME"

# 7d. Unmount — the kernel writes back dirty pages, dm-flakey DROPS them
#     Un-fsync'd data never reaches the image.  This is the power cut.
umount "$MNT"

# 7e. Suspend again to switch back to UP mode
dmsetup suspend "$DM_NAME"

# 7f. Reload the always-UP (pass-through) table
dmsetup load "$DM_NAME" --table "0 $SECTORS flakey $LOOP 0 180 0"

# 7g. Resume pass-through
dmsetup resume "$DM_NAME"
```

The "power cut" is complete.  The image now contains exactly what was fsync'd before step 7d.

---

## Step 8 — Remount and verify

```bash
mount "$DM_DEV" "$MNT"
echo "=== MODE=$MODE committed_before_cut=$COMMITTED ==="
java -cp "$JAR" org.questdb.CrashVerifier "$DBDIR"
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

If `drop_writes` is not recognised, try the feature name `error_writes` (some older kernels
used this name) or check `/proc/device-mapper` / `dmesg` for the rejection reason.

**`umount: target is busy`**

The writer wasn't killed cleanly or a shell still has a CWD inside `$MNT`:

```bash
fuser -mv "$MNT"      # list what has the mount open
fuser -mk "$MNT"      # force-kill those processes
umount "$MNT"
```

Or use `umount -l "$MNT"` (lazy unmount) — the writeback still happens, but the
unmount returns immediately.  This is fine for the power-cut simulation.

**`mount` after step 7g fails with "wrong fs type" or journal errors**

The journal may be partially on disk (metadata goes via `jbd2` which has its own
fsync path). Try:

```bash
e2fsck -fn "$DM_DEV"   # check without fixing; see what's damaged
# or mount with journal recovery disabled:
mount -o ro,noload "$DM_DEV" "$MNT"
```

If ext4 journal itself is torn you may need to add `-o journal_data` to mkfs.ext4
so the journal uses data mode (slower, but journals both metadata AND data), which
makes the power-cut model cleaner.  For the SYNC test what matters is that QuestDB's
own fsync calls hit the device before the cut.

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
   when mmap-backed pages are actually written to the block layer.  Check `dmesg` and
   QuestDB startup logs for the `nobarrier` warning.

**The `_progress` file disappears after the cut (NOSYNC run)**

That is expected and correct: `_progress` is written with a `rename(2)` which is
metadata — even with ext4 `data=ordered` mode, the data blocks may not be flushed
before the power cut.  This is why the script captures `COMMITTED` into a shell
variable BEFORE the cut, not by re-reading the file after remount.
