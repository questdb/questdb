# cairo (storage engine)

## Partition splits (partition top)

An O3 commit can split a partition zero-copy: the suffix becomes a child partition that
hardlinks the donor's column files and reads them at `file_row = logical + partitionTop - columnTop`.
Design doc: [ZERO_COPY_PARTITION_SPLIT.md](ZERO_COPY_PARTITION_SPLIT.md).
