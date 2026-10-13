/******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 ******************************************************************************/
package io.questdb.cairo;

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

/**
 * One save point generation of one table, in one file: a {@link #FILE_SIZE}-byte header, then byte copies
 * (sections) of the live {@code _txn}, {@code _cv} and {@code _meta}, in that order. The header binds the
 * sections to one table and cut and carries the size and checksum of each, so every reader rejects a file whose
 * sections did not all land. A table has two generations, {@code 0} and {@code 1}. The table stands on one of
 * them, the current save point: the generation recovery selected, or the one its writer last published, which the
 * table's {@code SeqTxnTracker} records with the save point's pin; without a pin it is the valid generation with the
 * higher (seqTxn, txn). A new save point is always written into the other generation. The header is written last,
 * once the sections and the file's name are durable, so its fsync publishes the generation.
 */
public final class DurableEpochManifest {
    public static final String FILE_NAME = "_epoch.manifest";
    // The header; the sections follow it.
    public static final int FILE_SIZE = 120;
    private static final int BODY_SIZE = 104;
    private static final int FORMAT_VERSION = 3;
    private static final int HEADER_ABSENT = 0;
    private static final int HEADER_DAMAGED = 2;
    private static final int HEADER_UNPUBLISHED = 1;
    private static final int HEADER_VALID = 3;
    private static final Log LOG = LogFactory.getLog(DurableEpochManifest.class);
    private static final int OFFSET_CV_SIZE = 64;
    private static final int OFFSET_META_SIZE = 80;
    private static final int OFFSET_SEQ_TXN = 24;
    private static final int OFFSET_TXN = 32;
    private static final int OFFSET_TXN_SIZE = 48;

    private DurableEpochManifest() {
    }

    // Whether a save point file of either generation exists, published or not. Leaves the path at the table root.
    public static boolean exists(FilesFacade ff, Path tablePath, int rootLen) {
        final boolean exists = ff.exists(tablePath.trimTo(rootLen).concat(FILE_NAME).put(".0").$())
                || ff.exists(tablePath.trimTo(rootLen).concat(FILE_NAME).put(".1").$());
        tablePath.trimTo(rootLen);
        return exists;
    }

    /**
     * The generations whose header is valid, the current one (the higher seqTxn, then txn) first. Reads only the
     * headers; {@link #validateSections} checks the rest. A {@code tableId} of {@code -1} accepts any table.
     */
    public static Candidate[] loadCandidates(FilesFacade ff, Path tablePath, int rootLen, int tableId) {
        final long scratch = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Candidate current = null;
            Candidate other = null;
            for (int generation = 0; generation < 2; generation++) {
                tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
                if (readHeader(ff, tablePath, tableId, generation, scratch) == HEADER_VALID) {
                    final Candidate candidate = new Candidate(
                            Unsafe.getUnsafe().getLong(scratch + OFFSET_SEQ_TXN),
                            Unsafe.getUnsafe().getLong(scratch + OFFSET_TXN),
                            generation
                    );
                    if (current == null) {
                        current = candidate;
                    } else if (candidate.epochSeqTxn > current.epochSeqTxn
                            || (candidate.epochSeqTxn == current.epochSeqTxn && candidate.epochTxn > current.epochTxn)) {
                        other = current;
                        current = candidate;
                    } else {
                        other = candidate;
                    }
                }
            }
            if (current == null) {
                return new Candidate[0];
            }
            return other == null ? new Candidate[]{current} : new Candidate[]{current, other};
        } finally {
            Unsafe.free(scratch, FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            tablePath.trimTo(rootLen);
        }
    }

    /**
     * Whether a save point of either generation lists the partition version, so that recovery can restore it. A
     * generation whose {@code _txn} copy cannot be read or fails its checksum counts as listing it.
     */
    public static boolean namesPartition(FilesFacade ff, Path tablePath, int rootLen, int tableId, long partitionTimestamp, long partitionNameTxn) {
        final long header = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int generation = 0; generation < 2; generation++) {
                tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
                if (readHeader(ff, tablePath, tableId, generation, header) == HEADER_VALID
                        && txnSectionListsPartition(ff, tablePath, header, partitionTimestamp, partitionNameTxn)) {
                    return true;
                }
            }
            return false;
        } catch (CairoException e) {
            return true;
        } finally {
            Unsafe.free(header, FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            tablePath.trimTo(rootLen);
        }
    }

    /**
     * The lowest seqTxn a valid generation of the table names, {@link Long#MAX_VALUE} when there is none, or
     * {@code 0} when a header cannot be read. For threads other than the table's writer.
     *
     * @param scratch native memory of at least {@link #FILE_SIZE} bytes, owned by the caller
     */
    public static long readLowestSeqTxn(FilesFacade ff, Path tablePath, int rootLen, int tableId, long scratch) {
        long lowest = Long.MAX_VALUE;
        try {
            for (int generation = 0; generation < 2; generation++) {
                tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
                if (readHeader(ff, tablePath, tableId, generation, scratch) == HEADER_VALID) {
                    lowest = Math.min(lowest, Unsafe.getUnsafe().getLong(scratch + OFFSET_SEQ_TXN));
                }
            }
        } catch (CairoException e) {
            return 0;
        } finally {
            tablePath.trimTo(rootLen);
        }
        return lowest;
    }

    /**
     * Removes each generation file whose header was never written (no magic, an older format, shorter than a
     * header): a write a crash interrupted, never published. Syncs the directory once after a removal. A
     * generation with magic but an invalid header is damaged and stays.
     *
     * @return whether a file was removed
     */
    public static boolean removeUnpublished(CairoConfiguration configuration, Path tablePath, int rootLen, int tableId) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long scratch = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        boolean removed = false;
        try {
            for (int generation = 0; generation < 2; generation++) {
                tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
                if (readHeader(ff, tablePath, tableId, generation, scratch) == HEADER_UNPUBLISHED) {
                    if (!ff.removeQuiet(tablePath.$())) {
                        throw CairoException.critical(ff.errno()).put("could not remove unpublished adaptive save point [path=").put(tablePath).put(']');
                    }
                    LOG.info().$("removed unpublished adaptive save point [path=").$(tablePath).I$();
                    removed = true;
                }
            }
        } finally {
            Unsafe.free(scratch, FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            tablePath.trimTo(rootLen);
        }
        if (removed) {
            fsyncDirectory(configuration, tablePath, rootLen);
        }
        return removed;
    }

    public static void publishInitial(
            CairoConfiguration configuration,
            TableToken tableToken,
            int timestampType,
            int partitionBy
    ) {
        publishBaseline(configuration, tableToken, null, timestampType, partitionBy);
    }

    /**
     * {@link #publishInitial} for a table that is not at its final path yet: publishes the generation-zero
     * baseline into {@code tableDirPath}, the staging clone of an {@code ALTER TABLE ... REBASE WAL}. The clone is
     * a new table (its {@code _txn} and {@code _meta} are reset), so it needs its own save point. Publishing before
     * the rename makes the table carry a valid save point from the moment it is visible: a start that finds an
     * adaptive table with no valid generation fails rather than trust its live files.
     *
     * @return the txn the baseline names, for the caller to pin
     */
    public static long publishInitialAt(
            CairoConfiguration configuration,
            TableToken tableToken,
            Path tableDirPath,
            int timestampType,
            int partitionBy
    ) {
        return publishBaseline(configuration, tableToken, tableDirPath, timestampType, partitionBy);
    }

    private static long publishBaseline(
            CairoConfiguration configuration,
            TableToken tableToken,
            @Nullable Path tableDirPath,
            int timestampType,
            int partitionBy
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path tablePath = new Path()) {
            // A staging clone publishes into its own (not yet renamed) dir; everything else into the live one.
            if (tableDirPath != null) {
                tablePath.of(tableDirPath);
            } else {
                tablePath.of(configuration.getDbRoot()).concat(tableToken);
            }
            final int rootLen = tablePath.size();
            fsyncFile(configuration, tablePath, rootLen, TableUtils.META_FILE_NAME);
            fsyncFile(configuration, tablePath, rootLen, TableUtils.COLUMN_VERSION_FILE_NAME);
            fsyncFile(configuration, tablePath, rootLen, TableUtils.TXN_FILE_NAME);

            long seqTxn;
            long txn;
            long columnVersion;
            long metadataVersion;
            try (TxReader txReader = new TxReader(ff); ColumnVersionReader cvReader = new ColumnVersionReader()) {
                tablePath.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME);
                txReader.ofRO(tablePath.$(), timestampType, partitionBy);
                // A torn live area loads its predecessor silently; saving the file would make the tear the
                // durable cut, and the writer refuses to roll back an anchored table.
                if (!txReader.unsafeLoadAll() || txReader.unsafeIsLiveAreaTorn()) {
                    throw CairoException.critical(0).put("could not validate initial adaptive _txn baseline [table=").put(tableToken.getTableName()).put(']');
                }
                tablePath.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
                cvReader.ofRO(ff, tablePath.$());
                if (!cvReader.readSafe() || cvReader.hasTornLiveArea() || txReader.getColumnVersion() != cvReader.getVersion()) {
                    throw CairoException.critical(0).put("could not validate initial adaptive _cv baseline [table=").put(tableToken.getTableName()).put(']');
                }
                seqTxn = txReader.getSeqTxn();
                txn = txReader.getTxn();
                columnVersion = txReader.getColumnVersion();
                metadataVersion = txReader.getMetadataVersion();
            } finally {
                tablePath.trimTo(rootLen);
            }
            if (seqTxn != 0) {
                throw CairoException.critical(0).put("initial adaptive baseline is not at sequencer transaction zero [table=")
                        .put(tableToken.getTableName()).put(", seqTxn=").put(seqTxn).put(']');
            }
            // Durable, name included, before table-name/sequencer registration.
            write(configuration, tableToken, tablePath, rootLen, 0, seqTxn, txn, columnVersion, metadataVersion);
            return txn;
        }
    }

    /**
     * Whether the live {@code _txn} is byte-equal to the {@code _txn} section of a generation that
     * {@link #validateSections} accepted, that is, whether the live cut is the one that generation saved. A read
     * failure of either file throws, so {@code false} is a definite difference: the caller takes it as a verdict on
     * the table's lineage.
     */
    public static boolean liveTxnEqualsSection(CairoConfiguration configuration, Path tablePath, int rootLen, int generation) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path manifest = new Path()) {
            manifest.of(tablePath).trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
            final long manifestSize = ff.length(manifest.$());
            if (manifestSize < 0) {
                throw CairoException.critical(ff.errno()).put("could not stat adaptive save point [path=").put(manifest).put(']');
            }
            try (MemoryCMR mem = Vm.getCMRInstance(ff, manifest.$(), manifestSize, MemoryTag.MMAP_TABLE_READER)) {
                return liveFileEqualsOrFail(ff, tablePath, rootLen, TableUtils.TXN_FILE_NAME, mem.addressOf(FILE_SIZE), mem.getLong(OFFSET_TXN_SIZE));
            }
        }
    }

    /**
     * Whether the header of generation {@code generation} records these column and metadata versions. Like
     * {@link #validate}, a read failure throws, so {@code false} is a definite mismatch.
     */
    public static boolean recordsVersions(CairoConfiguration configuration, Path tablePath, int rootLen, int generation, long columnVersion, long metadataVersion) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path manifest = new Path()) {
            manifest.of(tablePath).trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
            try (MemoryCMR mem = Vm.getCMRInstance(ff, manifest.$(), FILE_SIZE, MemoryTag.MMAP_TABLE_READER)) {
                return mem.getLong(40) == columnVersion && mem.getLong(96) == metadataVersion;
            }
        }
    }

    /**
     * Writes each section of a generation that {@link #validateSections} accepted over its live file where the
     * two differ: truncate, copy, fsync. {@code _meta} goes first and its directory entry is synced before the
     * pointers, so they are always interpreted with the metadata of the same cut. {@code _txn} goes before
     * {@code _cv}: a crash between the two leaves {@code _txn} behind {@code _cv}, which never references a
     * column version it does not have. The directory is synced again after them. Truncating first leaves each
     * live file exactly as long as its section, so the next start compares equal and restores nothing.
     *
     * @return whether any live file was written
     */
    public static boolean restoreLiveFiles(CairoConfiguration configuration, TableToken tableToken, Path tablePath, int rootLen, int generation) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path manifest = new Path()) {
            manifest.of(tablePath).trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
            final long txnSize;
            final long cvSize;
            final long metaSize;
            final boolean txnDiffers;
            final boolean cvDiffers;
            final boolean metaDiffers;
            try (MemoryCMR mem = Vm.getCMRInstance(ff, manifest.$(), ff.length(manifest.$()), MemoryTag.MMAP_TABLE_READER)) {
                txnSize = mem.getLong(OFFSET_TXN_SIZE);
                cvSize = mem.getLong(OFFSET_CV_SIZE);
                metaSize = mem.getLong(OFFSET_META_SIZE);
                txnDiffers = !liveFileEquals(ff, tablePath, rootLen, TableUtils.TXN_FILE_NAME, mem.addressOf(FILE_SIZE), txnSize);
                cvDiffers = !liveFileEquals(ff, tablePath, rootLen, TableUtils.COLUMN_VERSION_FILE_NAME, mem.addressOf(FILE_SIZE + txnSize), cvSize);
                metaDiffers = !liveFileEquals(ff, tablePath, rootLen, TableUtils.META_FILE_NAME, mem.addressOf(FILE_SIZE + txnSize + cvSize), metaSize);
            }
            if (!txnDiffers && !cvDiffers && !metaDiffers) {
                return false;
            }
            final long manifestFd = TableUtils.openRO(ff, manifest.$(), LOG);
            try {
                if (metaDiffers) {
                    restoreSection(configuration, tableToken, tablePath, rootLen, TableUtils.META_FILE_NAME, manifestFd, FILE_SIZE + txnSize + cvSize, metaSize);
                    fsyncDirectory(configuration, tablePath, rootLen);
                }
                if (txnDiffers) {
                    restoreSection(configuration, tableToken, tablePath, rootLen, TableUtils.TXN_FILE_NAME, manifestFd, FILE_SIZE, txnSize);
                }
                if (cvDiffers) {
                    restoreSection(configuration, tableToken, tablePath, rootLen, TableUtils.COLUMN_VERSION_FILE_NAME, manifestFd, FILE_SIZE + txnSize, cvSize);
                }
            } finally {
                ff.close(manifestFd);
            }
            fsyncDirectory(configuration, tablePath, rootLen);
            return true;
        } finally {
            tablePath.trimTo(rootLen);
        }
    }

    /**
     * Writes the header of generation {@code generation} over itself and, with {@code sync}, syncs it to the device;
     * without it, the caller flushes the filesystem before anything trusts the header. A failed header sync stops
     * the engine and leaves the header only in the page cache; a restart without a power loss then selects that save
     * point, while the device still holds the older one. The write matters: a failed writeback leaves the page clean,
     * and a bare sync would write nothing.
     */
    public static void rewriteHeader(FilesFacade ff, Path tablePath, int rootLen, int generation, boolean sync) {
        tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
        final long fd = TableUtils.openRW(ff, tablePath.$(), LOG, CairoConfiguration.O_NONE);
        try {
            final long header = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_TABLE_WRITER);
            try {
                if (ff.read(fd, header, FILE_SIZE, 0) != FILE_SIZE || ff.write(fd, header, FILE_SIZE, 0) != FILE_SIZE) {
                    throw CairoException.critical(ff.errno()).put("could not rewrite adaptive save point header [path=").put(tablePath).put(']');
                }
                if (sync) {
                    ff.fsyncDurable(fd);
                }
            } finally {
                Unsafe.free(header, FILE_SIZE, MemoryTag.NATIVE_TABLE_WRITER);
            }
        } finally {
            ff.close(fd);
            tablePath.trimTo(rootLen);
        }
    }

    /**
     * Whether generation {@code generation} is a complete save point of this table at the given cut, and its
     * {@code _txn} section records the given column and metadata versions.
     */
    public static boolean validate(
            CairoConfiguration configuration,
            TableToken tableToken,
            Path tablePath,
            int rootLen,
            int generation,
            long seqTxn,
            long txn,
            long columnVersion,
            long metadataVersion
    ) {
        return validate0(configuration, tableToken, tablePath, rootLen, generation, seqTxn, txn, true, columnVersion, metadataVersion);
    }

    /**
     * Whether generation {@code generation} is a complete save point of this table at the given cut: its header
     * is whole and names this table, generation and cut, and every section has the size and checksum the
     * header records. Reads only the save point file.
     */
    public static boolean validateSections(
            CairoConfiguration configuration,
            TableToken tableToken,
            Path tablePath,
            int rootLen,
            int generation,
            long seqTxn,
            long txn
    ) {
        return validate0(configuration, tableToken, tablePath, rootLen, generation, seqTxn, txn, false, 0, 0);
    }

    /**
     * Saves the live {@code _txn}, {@code _cv} and {@code _meta} as generation {@code generation} and publishes it.
     * Truncates the file, writes the three sections after the header and fsyncs them, syncs the directory unless
     * the file already held a valid header (whose name a sync before that header made durable), then writes the
     * header in one call and fsyncs it to the device. That fsync is the publish point: a crash before the header
     * lands leaves a file with no header, which is never selected, and nothing that can fail without poisoning the
     * engine follows it. The caller holds the table, so the live files do not change meanwhile.
     */
    public static void write(
            CairoConfiguration configuration,
            TableToken tableToken,
            Path tablePath,
            int rootLen,
            int generation,
            long seqTxn,
            long txn,
            long columnVersion,
            long metadataVersion
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        long fd = -1;
        long header = 0;
        // The finally trims the path back to rootLen: it belongs to the caller (TableWriter passes its own),
        // which concatenates onto it next.
        try {
            header = Unsafe.calloc(FILE_SIZE, MemoryTag.NATIVE_TABLE_WRITER);
            tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
            final boolean nameDurable = readHeader(ff, tablePath, tableToken.getTableId(), generation, header) == HEADER_VALID;
            Vect.memset(header, FILE_SIZE, 0);
            Unsafe.getUnsafe().putLong(header + OFFSET_SEQ_TXN, seqTxn);
            Unsafe.getUnsafe().putLong(header + OFFSET_TXN, txn);
            Unsafe.getUnsafe().putLong(header + 40, columnVersion);
            Unsafe.getUnsafe().putLong(header + 96, metadataVersion);
            fd = TableUtils.openRW(ff, tablePath.$(), LOG, CairoConfiguration.O_NONE);
            if (fd == -1) {
                throw CairoException.critical(ff.errno()).put("could not open adaptive epoch manifest [path=").put(tablePath).put(']');
            }
            if (!ff.truncate(fd, 0)) {
                throw CairoException.critical(ff.errno()).put("could not truncate adaptive epoch manifest [path=").put(tablePath).put(']');
            }
            long offset = FILE_SIZE;
            offset = writeSection(ff, tablePath, rootLen, TableUtils.TXN_FILE_NAME, fd, offset, header, OFFSET_TXN_SIZE);
            offset = writeSection(ff, tablePath, rootLen, TableUtils.COLUMN_VERSION_FILE_NAME, fd, offset, header, OFFSET_CV_SIZE);
            writeSection(ff, tablePath, rootLen, TableUtils.META_FILE_NAME, fd, offset, header, OFFSET_META_SIZE);
            ff.fsync(fd);
            if (!nameDurable) {
                fsyncDirectory(configuration, tablePath, rootLen);
            }

            Unsafe.getUnsafe().putLong(header, TableUtils.SNAPSHOT_CHECKSUM_MAGIC);
            Unsafe.getUnsafe().putInt(header + 8, FORMAT_VERSION);
            Unsafe.getUnsafe().putInt(header + 12, generation);
            Unsafe.getUnsafe().putInt(header + 16, tableToken.getTableId());
            Unsafe.getUnsafe().putLong(header + BODY_SIZE, TableUtils.SNAPSHOT_CHECKSUM_MAGIC);
            Unsafe.getUnsafe().putLong(header + BODY_SIZE + 8, TableUtils.calculateCvAreaChecksum(header, BODY_SIZE));
            tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
            if (ff.write(fd, header, FILE_SIZE, 0) != FILE_SIZE) {
                throw CairoException.critical(ff.errno()).put("could not write adaptive epoch manifest header [path=").put(tablePath).put(']');
            }
            ff.fsyncDurable(fd);
        } finally {
            if (header != 0) {
                Unsafe.free(header, FILE_SIZE, MemoryTag.NATIVE_TABLE_WRITER);
            }
            if (fd != -1) {
                ff.close(fd);
            }
            tablePath.trimTo(rootLen);
        }
    }

    /**
     * Makes the table directory's entries durable, and hands the caller's path back at {@code rootLen}.
     * The trim sits in a {@code finally} outside the platform guard: Windows has no directory fsync, but its
     * caller needs the path back as much as any other.
     */
    public static void fsyncDirectory(CairoConfiguration configuration, Path tablePath, int rootLen) {
        fsyncDirectory(configuration.getFilesFacade(), tablePath, rootLen);
    }

    public static void fsyncDirectory(FilesFacade ff, Path tablePath, int rootLen) {
        try {
            if (Os.isWindows()) {
                return; // no directory fsync on Windows -- but the caller still gets its path back
            }
            tablePath.trimTo(rootLen).slash$();
            final long fd = TableUtils.openRONoCache(ff, tablePath.$(), LOG);
            if (fd == -1) {
                throw CairoException.critical(ff.errno()).put("could not open adaptive epoch directory for fsync [path=").put(tablePath).put(']');
            }
            ff.fsyncAndClose(fd);
        } finally {
            tablePath.trimTo(rootLen);
        }
    }

    public static void fsyncDirectoryFiles(CairoConfiguration configuration, Path dirPath, int dirLen, boolean readOnly) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long findPtr = ff.findFirst(dirPath.trimTo(dirLen).$());
        if (findPtr <= 0) {
            throw CairoException.critical(ff.errno()).put("could not enumerate adaptive directory for fsync [path=").put(dirPath).put(']');
        }
        try {
            int findResult;
            do {
                final long namePtr = ff.findName(findPtr);
                final int type = ff.findType(findPtr);
                if (Files.notDots(namePtr) && (type == Files.DT_FILE || type == Files.DT_UNKNOWN)) {
                    dirPath.trimTo(dirLen).concat(namePtr);
                    final long fd = openForFsync(configuration, dirPath, readOnly);
                    if (fd == -1) {
                        throw CairoException.critical(ff.errno()).put("could not open adaptive file for fsync [path=").put(dirPath).put(']');
                    }
                    try {
                        ff.fsync(fd);
                    } finally {
                        ff.close(fd);
                    }
                }
                findResult = ff.findNext(findPtr);
            } while (findResult > 0);
            if (findResult < 0) {
                throw CairoException.critical(ff.errno()).put("could not finish enumerating adaptive directory for fsync [path=")
                        .put(dirPath.trimTo(dirLen)).put(']');
            }
        } finally {
            ff.findClose(findPtr);
            dirPath.trimTo(dirLen);
        }
        // Per-file fsync does not persist a newly created file's directory entry.
        fsyncDirectory(configuration, dirPath, dirLen);
    }

    /**
     * Flushes a table's materialized state file by file: every file of every attached partition and the partition
     * directory, then every file in the table root and the root itself, then one device barrier. This is the
     * fallback for platforms where {@link FilesFacade#syncfs(long)} flushes a single file, and it covers the same
     * write set as the writer's first epoch there ({@code TableWriter.fsyncAttachedPartitionFiles}). Sealing
     * a partition does not make its last writes durable. The table's entry in the database root closes the flush:
     * nothing else makes a table created since the last filesystem-wide sync durable.
     *
     * <p>The per-file fsync moves each file's bytes to the drive, and the closing {@code syncfs} flushes the drive
     * cache once for all of them: F_FULLFSYNC on macOS acts as a barrier for data fsync'd on the same device before
     * it, and on Windows each file's flush is already durable.
     */
    public static void fsyncTableFiles(CairoConfiguration configuration, TableToken tableToken, int timestampType, int partitionBy) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
            final int dbRootLen = path.of(configuration.getDbRoot()).size();
            final int rootLen = path.concat(tableToken).size();
            txReader.ofRO(path.concat(TableUtils.TXN_FILE_NAME).$(), timestampType, partitionBy);
            if (!txReader.unsafeLoadAll()) {
                throw CairoException.critical(0).put("could not read adaptive _txn to flush the table [table=")
                        .put(tableToken.getTableName()).put(']');
            }
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                TableUtils.setPathForNativePartition(
                        path.trimTo(rootLen),
                        timestampType,
                        partitionBy,
                        txReader.getPartitionTimestampByIndex(i),
                        txReader.getPartitionNameTxn(i)
                );
                fsyncDirectoryFiles(configuration, path, path.size(), txReader.isPartitionReadOnly(i));
            }
            fsyncTableRoot(configuration, path, rootLen);
            fsyncDirectory(configuration, path, dbRootLen);
        }
    }

    /**
     * Flushes every file in the table root and the root itself, then issues one device barrier through {@code _txn}:
     * the closing step of a file-by-file table flush.
     */
    public static void fsyncTableRoot(CairoConfiguration configuration, Path tablePath, int rootLen) {
        final FilesFacade ff = configuration.getFilesFacade();
        fsyncDirectoryFiles(configuration, tablePath, rootLen, false);
        tablePath.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME);
        final long fd = TableUtils.openRW(ff, tablePath.$(), LOG, configuration.getWriterFileOpenOpts());
        if (fd == -1) {
            throw CairoException.critical(ff.errno()).put("could not open adaptive _txn for the device flush [path=").put(tablePath).put(']');
        }
        try {
            ff.syncfs(fd);
        } finally {
            ff.close(fd);
            tablePath.trimTo(rootLen);
        }
    }

    /**
     * Makes the live files of the given tables durable, for a caller that keeps them while no save point protects
     * them: a start that leaves adaptive commit mode, and an Enterprise promote, which inherits what the replica applied
     * lazily. Where {@link FilesFacade#syncfs(long)} is filesystem-wide, one sync of the database root and of every
     * volume a table links to covers every table; elsewhere each table is flushed file by file.
     */
    public static void fsyncTables(CairoEngine engine, ObjList<TableToken> tableTokens) {
        if (tableTokens.size() == 0) {
            return;
        }
        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        if (ff.isSyncfsFileSystemWide()) {
            try (Path path = new Path()) {
                DatabaseCheckpointAgent.syncRestoredState(ff, path.of(configuration.getDbRoot()), true);
            }
            return;
        }
        for (int i = 0, n = tableTokens.size(); i < n; i++) {
            final TableToken tableToken = tableTokens.getQuick(i);
            try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
                fsyncTableFiles(configuration, tableToken, metadata.getTimestampType(), metadata.getPartitionBy());
            }
        }
    }

    /**
     * Opens a file for a per-file flush. FlushFileBuffers needs a writable handle, so this is a write open, except
     * for a read-only partition on POSIX: it may be a link to storage that refuses write opens, and POSIX fsyncs a
     * readable handle.
     */
    public static long openForFsync(CairoConfiguration configuration, Path path, boolean readOnly) {
        final FilesFacade ff = configuration.getFilesFacade();
        return readOnly && !Os.isWindows()
                ? TableUtils.openRO(ff, path.$(), LOG)
                : TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
    }

    private static void fsyncFile(CairoConfiguration configuration, Path tablePath, int rootLen, String name) {
        final FilesFacade ff = configuration.getFilesFacade();
        tablePath.trimTo(rootLen).concat(name);
        final long fd = TableUtils.openRW(ff, tablePath.$(), LOG, configuration.getWriterFileOpenOpts());
        if (fd == -1) {
            throw CairoException.critical(ff.errno()).put("could not open initial adaptive file for fsync [path=").put(tablePath).put(']');
        }
        try {
            ff.fsync(fd);
        } finally {
            ff.close(fd);
        }
    }

    // Whether the header at `addr` is whole and names this generation and table (any table for a tableId of -1).
    private static boolean isHeaderValid(long addr, int generation, int tableId) {
        return Unsafe.getUnsafe().getLong(addr) == TableUtils.SNAPSHOT_CHECKSUM_MAGIC
                && Unsafe.getUnsafe().getInt(addr + 8) == FORMAT_VERSION
                && Unsafe.getUnsafe().getInt(addr + 12) == generation
                && (tableId == -1 || Unsafe.getUnsafe().getInt(addr + 16) == tableId)
                && Unsafe.getUnsafe().getLong(addr + BODY_SIZE) == TableUtils.SNAPSHOT_CHECKSUM_MAGIC
                && Unsafe.getUnsafe().getLong(addr + BODY_SIZE + 8) == TableUtils.calculateCvAreaChecksum(addr, BODY_SIZE);
    }

    // The restore's variant: any failure to compare sends the file down the restore.
    private static boolean liveFileEquals(FilesFacade ff, Path tablePath, int rootLen, String name, long sectionAddr, long sectionSize) {
        try {
            return liveFileEqualsOrFail(ff, tablePath, rootLen, name, sectionAddr, sectionSize);
        } catch (CairoException | CairoError e) {
            return false;
        }
    }

    // A read failure of the live file throws, so false is a definite difference; a missing live file differs.
    private static boolean liveFileEqualsOrFail(FilesFacade ff, Path tablePath, int rootLen, String name, long sectionAddr, long sectionSize) {
        tablePath.trimTo(rootLen).concat(name);
        try {
            if (!ff.exists(tablePath.$())) {
                return false;
            }
            final long length = ff.length(tablePath.$());
            if (length < 0) {
                throw CairoException.critical(ff.errno()).put("could not stat live table file [path=").put(tablePath).put(']');
            }
            if (length != sectionSize) {
                return false;
            }
            try (MemoryCMR live = Vm.getCMRInstance(ff, tablePath.$(), sectionSize, MemoryTag.MMAP_TABLE_READER)) {
                return Vect.memeq(live.addressOf(0), sectionAddr, sectionSize);
            }
        } finally {
            tablePath.trimTo(rootLen);
        }
    }

    // Reads the header of generation `generation` (at `path`) into `scratch` and classifies it. A header is written in
    // one call after its sections are durable, so a file without magic, of an older format or shorter than a header
    // was never published, and one with magic that fails a check is damaged.
    private static int readHeader(FilesFacade ff, Path path, int tableId, int generation, long scratch) {
        // Check existence first: whether a failed open means "absent" must not depend on errno, which some
        // facades report for every failure. A file that exists but cannot be opened still fails.
        if (!ff.exists(path.$())) {
            return HEADER_ABSENT;
        }
        final long fd = ff.openRO(path.$());
        if (fd < 0) {
            // Removed after the existence check.
            if (Files.isErrnoFileDoesNotExist(ff.errno())) {
                return HEADER_ABSENT;
            }
            throw CairoException.critical(ff.errno()).put("could not open adaptive save point [path=").put(path).put(']');
        }
        final long bytesRead;
        try {
            bytesRead = ff.read(fd, scratch, FILE_SIZE, 0);
        } finally {
            ff.close(fd);
        }
        if (bytesRead < 0) {
            throw CairoException.critical(ff.errno()).put("could not read adaptive save point [path=").put(path).put(']');
        }
        if (bytesRead < FILE_SIZE
                || Unsafe.getUnsafe().getLong(scratch) != TableUtils.SNAPSHOT_CHECKSUM_MAGIC
                || Unsafe.getUnsafe().getInt(scratch + 8) < FORMAT_VERSION) {
            return HEADER_UNPUBLISHED;
        }
        return isHeaderValid(scratch, generation, tableId) ? HEADER_VALID : HEADER_DAMAGED;
    }

    private static void restoreSection(
            CairoConfiguration configuration,
            TableToken tableToken,
            Path tablePath,
            int rootLen,
            String name,
            long manifestFd,
            long offset,
            long size
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        tablePath.trimTo(rootLen).concat(name);
        // Windows cannot shrink a file while a mapping of it is live; replace the file there, as
        // TableUtils.replaceFileContent does.
        if (Os.isWindows() && !ff.removeQuiet(tablePath.$()) && ff.exists(tablePath.$())) {
            throw CairoException.critical(ff.errno()).put("adaptive epoch roll-forward could not replace [table=")
                    .put(tableToken.getTableName()).put(", path=").put(tablePath).put(']');
        }
        final long fd = TableUtils.openRW(ff, tablePath.$(), LOG, configuration.getWriterFileOpenOpts());
        if (fd == -1) {
            throw CairoException.critical(ff.errno()).put("adaptive epoch roll-forward could not open [table=")
                    .put(tableToken.getTableName()).put(", path=").put(tablePath).put(']');
        }
        try {
            if (!ff.truncate(fd, 0) || ff.copyData(manifestFd, fd, offset, size) != size) {
                throw CairoException.critical(ff.errno()).put("adaptive epoch roll-forward failed to restore [table=")
                        .put(tableToken.getTableName()).put(", path=").put(tablePath).put(", size=").put(size).put(']');
            }
            ff.fsync(fd);
        } finally {
            ff.close(fd);
            tablePath.trimTo(rootLen);
        }
    }

    // Reads the _txn copy of the generation at `path`, whose valid header is at `header`.
    private static boolean txnSectionListsPartition(FilesFacade ff, Path path, long header, long partitionTimestamp, long partitionNameTxn) {
        final long size = Unsafe.getUnsafe().getLong(header + OFFSET_TXN_SIZE);
        if (size <= 0) {
            return true;
        }
        final long fd = ff.openRO(path.$());
        if (fd < 0) {
            return true;
        }
        try {
            final long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                return ff.read(fd, buf, size, FILE_SIZE) != size
                        || Unsafe.getUnsafe().getLong(header + OFFSET_TXN_SIZE + Long.BYTES) != TableUtils.calculateCvAreaChecksum(buf, size)
                        || TxReader.imageListsPartition(buf, size, partitionTimestamp, partitionNameTxn);
            } finally {
                Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }
    }

    private static boolean validate0(
            CairoConfiguration configuration,
            TableToken tableToken,
            Path tablePath,
            int rootLen,
            int generation,
            long seqTxn,
            long txn,
            boolean checkVersions,
            long columnVersion,
            long metadataVersion
    ) {
        if (generation < 0) {
            return false;
        }
        final FilesFacade ff = configuration.getFilesFacade();
        tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
        try {
            if (!ff.exists(tablePath.$())) {
                return false;
            }
            // A read error is never a verdict: a generation this cannot stat, open or map has not failed its
            // checksums, and a caller that fell back on false would rewind the table over a transient fault. The
            // stat throws here; the open and the map throw in getCMRInstance. Every false below is a definite
            // comparison, since the sizes are checked before any section is read.
            final long fileSize = ff.length(tablePath.$());
            if (fileSize < 0) {
                throw CairoException.critical(ff.errno()).put("could not stat adaptive save point [path=").put(tablePath).put(']');
            }
            if (fileSize <= FILE_SIZE) {
                return false;
            }
            try (MemoryCMR mem = Vm.getCMRInstance(ff, tablePath.$(), fileSize, MemoryTag.MMAP_TABLE_READER)) {
                if (!isHeaderValid(mem.addressOf(0), generation, tableToken.getTableId())
                        || mem.getLong(OFFSET_SEQ_TXN) != seqTxn
                        || mem.getLong(OFFSET_TXN) != txn
                        || (checkVersions && (mem.getLong(40) != columnVersion || mem.getLong(96) != metadataVersion))) {
                    return false;
                }
                final long txnSize = mem.getLong(OFFSET_TXN_SIZE);
                final long cvSize = mem.getLong(OFFSET_CV_SIZE);
                final long metaSize = mem.getLong(OFFSET_META_SIZE);
                if (txnSize <= 0 || cvSize <= 0 || metaSize <= 0
                        || txnSize > fileSize || cvSize > fileSize || metaSize > fileSize
                        || FILE_SIZE + txnSize + cvSize + metaSize != fileSize) {
                    return false;
                }
                return TableUtils.calculateCvAreaChecksum(mem.addressOf(FILE_SIZE), txnSize) == mem.getLong(OFFSET_TXN_SIZE + 8)
                        && TableUtils.calculateCvAreaChecksum(mem.addressOf(FILE_SIZE + txnSize), cvSize) == mem.getLong(OFFSET_CV_SIZE + 8)
                        && TableUtils.calculateCvAreaChecksum(mem.addressOf(FILE_SIZE + txnSize + cvSize), metaSize) == mem.getLong(OFFSET_META_SIZE + 8);
            }
        } finally {
            tablePath.trimTo(rootLen);
        }
    }

    /**
     * Appends the live file {@code name} to the save point file at {@code offset}, records its size and
     * checksum in the header at {@code sizeOffset}, and returns the offset after it.
     */
    private static long writeSection(FilesFacade ff, Path tablePath, int rootLen, String name, long fd, long offset, long header, int sizeOffset) {
        tablePath.trimTo(rootLen).concat(name);
        final long size = ff.length(tablePath.$());
        if (size <= 0) {
            throw CairoException.critical(ff.errno()).put("invalid adaptive epoch section size [path=").put(tablePath).put(", size=").put(size).put(']');
        }
        try (MemoryCMR mem = Vm.getCMRInstance(ff, tablePath.$(), size, MemoryTag.MMAP_TABLE_READER)) {
            if (sizeOffset == OFFSET_TXN_SIZE) {
                if (size < TableUtils.TX_BASE_HEADER_SIZE) {
                    throw CairoException.critical(0).put("invalid adaptive epoch transaction header [path=").put(tablePath).put(']');
                }
                final long version = mem.getLong(TableUtils.TX_BASE_OFFSET_VERSION_64);
                final int base = mem.getInt((version & 1) == 0 ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32);
                if (base < TableUtils.TX_BASE_HEADER_SIZE || base > size - TableUtils.TX_OFFSET_SEQ_TXN_64 - Long.BYTES) {
                    throw CairoException.critical(0).put("invalid adaptive epoch transaction record [path=").put(tablePath).put(']');
                }
                final long expectedSeqTxn = Unsafe.getUnsafe().getLong(header + OFFSET_SEQ_TXN);
                final long expectedTxn = Unsafe.getUnsafe().getLong(header + OFFSET_TXN);
                final long expectedColumnVersion = Unsafe.getUnsafe().getLong(header + 40);
                final long expectedMetadataVersion = Unsafe.getUnsafe().getLong(header + 96);
                final long seqTxn = mem.getLong(base + TableUtils.TX_OFFSET_SEQ_TXN_64);
                final long txn = mem.getLong(base + TableUtils.TX_OFFSET_TXN_64);
                final long columnVersion = mem.getLong(base + TableUtils.TX_OFFSET_COLUMN_VERSION_64);
                final int metadataVersion = Numbers.decodeLowInt(mem.getLong(base + TableUtils.TX_OFFSET_STRUCT_VERSION_64));
                if (expectedSeqTxn != seqTxn || expectedTxn != txn || expectedColumnVersion != columnVersion || expectedMetadataVersion != metadataVersion) {
                    LOG.critical().$("adaptive epoch identity mismatch [path=").$(tablePath)
                            .$(", expectedSeqTxn=").$(expectedSeqTxn).$(", foundSeqTxn=").$(seqTxn)
                            .$(", expectedTxn=").$(expectedTxn).$(", foundTxn=").$(txn)
                            .$(", expectedColumnVersion=").$(expectedColumnVersion).$(", foundColumnVersion=").$(columnVersion)
                            .$(", expectedMetadataVersion=").$(expectedMetadataVersion).$(", foundMetadataVersion=").$(metadataVersion).$(']').$();
                    throw CairoException.critical(0).put("adaptive epoch identity mismatch [path=").put(tablePath).put(']');
                }
            }
            if (ff.write(fd, mem.addressOf(0), size, offset) != size) {
                throw CairoException.critical(ff.errno()).put("could not write adaptive epoch section [path=").put(tablePath).put(", size=").put(size).put(']');
            }
            Unsafe.getUnsafe().putLong(header + sizeOffset, size);
            Unsafe.getUnsafe().putLong(header + sizeOffset + 8, TableUtils.calculateCvAreaChecksum(mem.addressOf(0), size));
        } finally {
            tablePath.trimTo(rootLen);
        }
        return offset + size;
    }

    public static final class Candidate {
        public final long epochSeqTxn;
        public final long epochTxn;
        public final int generation;

        private Candidate(long epochSeqTxn, long epochTxn, int generation) {
            this.epochSeqTxn = epochSeqTxn;
            this.epochTxn = epochTxn;
            this.generation = generation;
        }
    }
}
