# Durability & power-loss test harnesses

Manual, **root-required** harnesses that validate QuestDB's crash and power-loss behaviour.
They are deliberately **not** part of the JUnit suite — they need `dm-flakey`, block-layer
control, and abrupt process kills, so run them by hand on a real Linux box (the JVM-level
crash-consistency tests live in `core/src/test/java/io/questdb/test/cairo/crash/`).

| Tool | What it tests | Method |
|---|---|---|
| `crash-consistency-pkill.sh` | **process-crash consistency** — the page cache survives, so even NOSYNC-committed data is intact | `kill -9` mid-write, reopen, assert committed rows intact + any in-flight batch cleanly rolled back |
| `power-cut-dmflakey.sh` | **power-loss durability** — un-fsync'd writes in the page cache are lost | `dm-flakey drop_writes` over a loop device, `umount` (writeback dropped), remount, assert survival per commit mode |
| `power-cut-manual.md` | — | step-by-step runbook for the dm-flakey harness, for running interactively / debugging a timing or kernel issue |
| `syncfs-microtest.sh` | does `syncfs(2)` **durably journal** ext4's unwritten→written extent conversion across a power cut? (what the batched-flush optimisation relies on) | QuestDB-independent `xfs_io` + the same dm-flakey power cut |

**The core distinction:** `crash-consistency-pkill.sh` leaves the page cache intact (a
process crash), so it proves *consistency* but not *durability*; `power-cut-dmflakey.sh`
discards un-fsync'd writes at the block layer, so it proves *durability* (and distinguishes
the commit modes — a NOSYNC table loses its tail, a durable one does not).

Each script's header documents its exact usage and pass/fail verdicts. The dm-flakey harness
has been run on real hardware against adaptive commit — `W=0` DURABLE (zero loss), `W=50ms`
RPO_OK (loss ≤ W), `SYNC` DURABLE, `NOSYNC` total loss (the expected control).
