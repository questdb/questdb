# Durability & power-loss test harnesses

Harnesses for process-crash and power-loss models. The host `dm-flakey` tools require root and
must use disposable test devices. The QEMU harness needs no host root; privileged operations
stay in its guest, while images and logs live in its configured host state directory.
These are separate from the JVM crash tests in `core/src/test/java/io/questdb/test/cairo/crash/`.
For the product contract, see [adaptive commit mode](../../docs/adaptive-commit-mode.md).

| Tool | What it tests | Method |
|---|---|---|
| `crash-consistency-pkill.sh` | **process-crash consistency** — the page cache survives, so even NOSYNC-committed data is intact | `kill -9` mid-write, reopen, assert committed rows intact + any in-flight batch cleanly rolled back |
| `power-cut-dmflakey.sh` | **power-loss durability** — un-fsync'd writes in the page cache are lost | `dm-flakey drop_writes` over a loop device, `umount` (writeback dropped), remount, assert survival per commit mode |
| `power-cut-manual.md` | — | step-by-step runbook for the dm-flakey harness, for running interactively / debugging a timing or kernel issue |
| `syncfs-microtest.sh` | does `syncfs(2)` **durably journal** ext4's unwritten→written extent conversion across a power cut? (what the batched-flush optimisation relies on) | QuestDB-independent `xfs_io` + the same dm-flakey power cut |
| [`vm/`](vm/README.md) | repeatable crash states on a real guest kernel and block layer | record with `dm-log-writes`, reset the disposable device and replay selected flush boundaries; killing QEMU only ends recording |

A process kill leaves the kernel page cache alive and does not test loss of volatile writes.
`dm-flakey` drops writes submitted after its cut, including unmount writeback. Data already
written before the cut can survive even without an explicit flush. VM replay reconstructs
selected block-log boundaries; it does not enumerate every possible device tear or reordering.
Neither mechanism is an actual hardware power cut.

Use the scripts' current arguments and JVM flags. For every result retain the exact server and
client revisions, native artifacts, mode, workload, selected boundaries and negative controls.
A historical green run is not validation of a newer revision. `RPO_OK` establishes the tested
frontier inequality, not a hard elapsed-time loss bound under worker or storage stalls. These
harnesses were not rerun as part of the documentation update.
