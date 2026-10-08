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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

/**
 * One save point generation of one table, in one file: a {@link #FILE_SIZE}-byte header, then byte copies
 * (sections) of the live {@code _txn}, {@code _cv} and {@code _meta}, in that order. The header binds the
 * sections to one table and cut and carries the size and checksum of each. It is written last, after an fsync
 * of the sections, so a header that validates always has durable sections behind it.
 */
public final class DurableEpochManifest {
    public static final String FILE_NAME = "_epoch.manifest";
    // The header; the sections follow it.
    public static final int FILE_SIZE = 120;
    private static final int BODY_SIZE = 104;
    private static final int FORMAT_VERSION = 3;
    private static final Log LOG = LogFactory.getLog(DurableEpochManifest.class);
    private static final int OFFSET_CV_SIZE = 64;
    private static final int OFFSET_META_SIZE = 80;
    private static final int OFFSET_TXN_SIZE = 48;

    private DurableEpochManifest() {
    }

    public static void publishInitial(
            CairoConfiguration configuration,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long nowMs
    ) {
        publishBaseline(configuration, tableToken, null, timestampType, partitionBy, nowMs, true);
    }

    /**
     * {@link #publishInitial} for a table that is not at its final path yet: publishes the generation-zero
     * baseline into {@code tableDirPath} — an {@code ALTER TABLE ... REBASE WAL} staging clone. The clone is a
     * NEW table (its {@code _txn}/{@code _meta} are reset), so it needs its OWN anchor; publishing it into the
     * staging dir means the atomic rename carries a table whose durable epoch is self-consistent from the
     * first instant it is visible. Publishing after the rename instead would leave a window in which a boot
     * can find, and adopt, a dir whose anchors it cannot validate — and an adaptive table with no trustworthy
     * generation aborts startup rather than falling back to unverified live state.
     */
    public static void publishInitialAt(
            CairoConfiguration configuration,
            TableToken tableToken,
            Path tableDirPath,
            int timestampType,
            int partitionBy,
            long nowMs
    ) {
        publishBaseline(configuration, tableToken, tableDirPath, timestampType, partitionBy, nowMs, true);
    }

    /**
     * The replica counterpart of {@link #publishInitial} and {@link #publishInitialAt}, for a caller running
     * under {@link io.questdb.cairo.wal.LocalDurabilityPolicy#REPLICA_SKIP}: enrols the new table under
     * ADAPTIVE WITHOUT an anchor. It marks the table for enrolment, clears any anchor the directory already
     * holds, then records the enrolment. See {@link RecoveryCoordinator#replaceAnchorWithEnrolmentMarker} for
     * why a replica must not keep an anchor.
     *
     * <p>Marker first, record last, for the same reason {@link #recordEnrollment} runs after the anchor: an
     * enrolled table must never be found with neither. A crash between the two leaves an unenrolled, marked
     * table; startup publishes its baseline at the live cut, and the first writer records the enrolment
     * against it.
     *
     * @param tableDirPath the table's directory when it is not at its final path yet (the REBASE WAL staging
     *                     clone), or {@code null} for the live directory under the db root
     */
    public static void enrolWithoutAnchor(
            CairoConfiguration configuration,
            TableToken tableToken,
            @Nullable Path tableDirPath
    ) {
        try (Path tablePath = new Path()) {
            if (tableDirPath != null) {
                tablePath.of(tableDirPath);
            } else {
                tablePath.of(configuration.getDbRoot()).concat(tableToken);
            }
            final int rootLen = tablePath.size();
            RecoveryCoordinator.replaceAnchorWithEnrolmentMarker(configuration, tablePath, rootLen);
            recordEnrollment(configuration, tableToken, tablePath, rootLen);
        }
    }

    /**
     * Whether the table's live {@code _txn} and {@code _cv} form a cut the {@code TableWriter} constructor
     * would open unchanged: neither live area is torn, and {@code _cv} is at the column version {@code _txn}
     * names. Startup may adopt only such a cut as a generation-zero baseline.
     *
     * <p>A table marked for enrolment by a replica tenure fails this after a crash: the replica applies at
     * NOSYNC grade, so a kill or power loss can leave {@code _cv} one version ahead of {@code _txn}, or tear the
     * live {@code _txn} area. Both readers fall back to the previous area silently, so a successful load alone
     * would let startup copy a torn file into the anchor. The writer repairs either shape, as it does for any
     * NOSYNC table, and publishes the baseline itself afterwards.
     *
     * <p>Reads only, and needs no metadata: a pooled metadata reader opened on a torn {@code _txn} keeps the
     * fallback record and refuses to refresh past it, which would hide the table from WAL apply. Any failure to
     * read reports the cut as not intact; the caller then writes nothing.
     */
    public static boolean isLiveCutIntact(CairoConfiguration configuration, TableToken tableToken) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (
                Path path = new Path();
                TxReader txReader = new TxReader(ff);
                ColumnVersionReader cvReader = new ColumnVersionReader()
        ) {
            final int rootLen = path.of(configuration.getDbRoot()).concat(tableToken).size();
            // Neither the column version nor the area geometry depends on the partitioning or the timestamp type.
            txReader.ofRO(path.concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, PartitionBy.NONE);
            if (!txReader.unsafeLoadAll() || txReader.unsafeIsLiveAreaTorn()) {
                return false;
            }
            cvReader.ofRO(ff, path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
            return cvReader.readSafe()
                    && !cvReader.hasTornLiveArea()
                    && cvReader.getVersion() == txReader.getColumnVersion();
        } catch (CairoException e) {
            return false;
        }
    }

    public static void publishCheckpointRestored(
            CairoConfiguration configuration,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long nowMs
    ) {
        publishBaseline(configuration, tableToken, null, timestampType, partitionBy, nowMs, false);
    }

    private static void publishBaseline(
            CairoConfiguration configuration,
            TableToken tableToken,
            @Nullable Path tableDirPath,
            int timestampType,
            int partitionBy,
            long nowMs,
            boolean requireInitialSeqTxn
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
            if (requireInitialSeqTxn && seqTxn != 0) {
                throw CairoException.critical(0).put("initial adaptive baseline is not at sequencer transaction zero [table=")
                        .put(tableToken.getTableName()).put(", seqTxn=").put(seqTxn).put(']');
            }
            write(configuration, tableToken, tablePath, rootLen, 0, seqTxn, txn, columnVersion, metadataVersion);
            fsyncDirectory(configuration, tablePath, rootLen);
            try (SnapshotMarker marker = new SnapshotMarker(configuration)) {
                tablePath.trimTo(rootLen).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(tablePath.$()).write(seqTxn, txn, nowMs, 0);
            }
            // _snapshot is newly created for generation zero. Its own fsync does not make the parent
            // directory entry durable, so publish that dentry before table-name/sequencer registration.
            fsyncDirectory(configuration, tablePath, rootLen);
            if (requireInitialSeqTxn) {
                // CREATE and the REBASE staging clone only. A checkpoint/backup restore or a replica demote
                // also publishes a baseline here, but those tables are not necessarily adaptive and their
                // record must stay whatever it was: the first writer to open one enrolls it if, and only if,
                // the table actually resolves to ADAPTIVE. Recording it for them would assert lazy state on a
                // table that may never have any.
                recordEnrollment(configuration, tableToken, tablePath, rootLen);
            }
        }
    }

    /**
     * Records in {@code _meta} that the anchor just published covers this table, so the first
     * {@code TableWriter} to open it does not have to enroll it — which would rewrite {@code _meta} and bump
     * the metadata version of every newly created adaptive table.
     *
     * <p>Runs AFTER the marker is published and its directory entry is durable, never before. That ordering
     * is what makes the write safe to do in place: once the anchor exists, EVERY outcome of this 4-byte
     * write is correct. The new value is correct because the anchor it speaks for is on disk. The old value
     * (UNSET) is correct because it only means "not enrolled yet", and the next writer to open the table
     * enrolls it — finding the anchor already naming the live cut, so it publishes no new epoch and merely
     * records. A torn value is correct as whichever of those two it reads as. Recording BEFORE the anchor
     * would have no such property: it would claim durable state that is not on disk yet.
     *
     * <p>The argument holds only because the record is a SINGLE store.
     *
     * <p>The generation-zero payload copy of {@code _meta} predates this write and so still reads UNSET.
     * That is deliberate and harmless: a recovery that restores the payload reverts the record to "not
     * enrolled", and the next writer re-records it against the same, still-valid anchor.
     */
    private static void recordEnrollment(CairoConfiguration configuration, TableToken tableToken, Path tablePath, int rootLen) {
        writeEnrollmentRecord(configuration, tableToken, tablePath, rootLen, CommitMode.ADAPTIVE);
    }

    /**
     * Writes the enrolment record of an EXISTING table in place, for a {@code _meta} that already carries
     * the field. Callers must have made the state the record describes durable first -- see
     * {@link #recordEnrollment}, whose ordering argument this shares.
     *
     * <p>In place, rather than through a metadata rewrite: a rewrite bumps the table's metadataVersion,
     * which is a structural event other subsystems legitimately react to (Enterprise's incremental backup
     * among them). Enrolment is a durability bookkeeping detail and must not masquerade as a schema change.
     */
    public static void recordEnrolledCommitMode(CairoConfiguration configuration, TableToken tableToken, int enrolledCommitMode) {
        try (Path tablePath = new Path()) {
            tablePath.of(configuration.getDbRoot()).concat(tableToken);
            writeEnrollmentRecord(configuration, tableToken, tablePath, tablePath.size(), enrolledCommitMode);
        }
    }

    /**
     * Clears the enrolment record of a table that is being built by COPYING another table's {@code _meta}
     * and is not receiving an anchor — an {@code ALTER TABLE ... REBASE WAL} staging clone whose effective
     * mode is not adaptive.
     *
     * <p>Without this the clone inherits the source's record. The source can legitimately still say ADAPTIVE
     * while resolving to something else (a table left enrolled by a crash, awaiting the reconciliation its
     * next writer performs), and the clone would then be a table claiming lazy state with no anchor to
     * rewind to — which recovery refuses, taking the whole instance down. The clone has no lazy state by
     * construction: its {@code _txn}/{@code _meta} were just reset and its files freshly written.
     */
    public static void clearEnrollmentRecord(CairoConfiguration configuration, TableToken tableToken, Path tablePath, int rootLen) {
        writeEnrollmentRecord(configuration, tableToken, tablePath, rootLen, CommitMode.UNSET);
    }

    private static void writeEnrollmentRecord(
            CairoConfiguration configuration,
            TableToken tableToken,
            Path tablePath,
            int rootLen,
            int enrolledCommitMode
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        tablePath.trimTo(rootLen).concat(TableUtils.META_FILE_NAME);
        final long fd = TableUtils.openRW(ff, tablePath.$(), LOG, configuration.getWriterFileOpenOpts());
        if (fd == -1) {
            throw CairoException.critical(ff.errno())
                    .put("could not open table metadata to record adaptive enrollment [table=")
                    .put(tableToken.getTableName()).put(", path=").put(tablePath).put(']');
        }
        // malloc INSIDE the try: checkAllocLimit throws when rssMemLimit is exceeded, and an allocation
        // that failed outside the guard would strand the _meta descriptor opened just above. The DDL
        // failure is non-critical, so the operator retries and each retry would leak another fd.
        long tempMem = 0;
        try {
            tempMem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_TABLE_WRITER);
            TableUtils.writeIntOrFail(
                    ff,
                    fd,
                    TableUtils.META_OFFSET_ENROLLED_COMMIT_MODE,
                    enrolledCommitMode,
                    tempMem,
                    tablePath
            );
            ff.fsync(fd);
        } finally {
            if (tempMem != 0) {
                Unsafe.free(tempMem, Long.BYTES, MemoryTag.NATIVE_TABLE_WRITER);
            }
            ff.close(fd);
        }
        tablePath.trimTo(rootLen);
    }

    public static boolean isMetadataBound(CairoConfiguration configuration, TableToken tableToken, int generation) {
        if (generation < 0) {
            return false;
        }
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path manifest = new Path()) {
            manifest.of(configuration.getDbRoot()).concat(tableToken).concat(FILE_NAME).put('.').put(generation);
            if (!ff.exists(manifest.$()) || ff.length(manifest.$()) < 16) {
                return false;
            }
            try (MemoryCMR mem = Vm.getCMRInstance(ff, manifest.$(), 16, MemoryTag.MMAP_TABLE_READER)) {
                return mem.getLong(0) == TableUtils.SNAPSHOT_CHECKSUM_MAGIC && mem.getInt(8) >= FORMAT_VERSION;
            }
        } catch (Throwable e) {
            return false;
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
     * Saves the live {@code _txn}, {@code _cv} and {@code _meta} as generation {@code generation}. Truncates the
     * file, writes the three sections after the header, fsyncs them, then writes the header in one call and
     * fsyncs again. A crash before the header lands leaves a file with no valid header, which recovery skips.
     * The caller holds the table, so the live files do not change meanwhile.
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
        // Restored in the finally: this path belongs to the caller (TableWriter passes its own), and leaving it
        // at _epoch.manifest.<generation> made the next concat build a nonexistent directory. See
        // DurableEpochPathRestoreTest.
        try {
            tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
            fd = TableUtils.openRW(ff, tablePath.$(), LOG, CairoConfiguration.O_NONE);
            if (fd == -1) {
                throw CairoException.critical(ff.errno()).put("could not open adaptive epoch manifest [path=").put(tablePath).put(']');
            }
            if (!ff.truncate(fd, 0)) {
                throw CairoException.critical(ff.errno()).put("could not truncate adaptive epoch manifest [path=").put(tablePath).put(']');
            }
            header = Unsafe.calloc(FILE_SIZE, MemoryTag.NATIVE_TABLE_WRITER);
            long offset = FILE_SIZE;
            offset = writeSection(ff, tablePath, rootLen, TableUtils.TXN_FILE_NAME, fd, offset, header + OFFSET_TXN_SIZE);
            offset = writeSection(ff, tablePath, rootLen, TableUtils.COLUMN_VERSION_FILE_NAME, fd, offset, header + OFFSET_CV_SIZE);
            writeSection(ff, tablePath, rootLen, TableUtils.META_FILE_NAME, fd, offset, header + OFFSET_META_SIZE);
            ff.fsync(fd);

            Unsafe.getUnsafe().putLong(header, TableUtils.SNAPSHOT_CHECKSUM_MAGIC);
            Unsafe.getUnsafe().putInt(header + 8, FORMAT_VERSION);
            Unsafe.getUnsafe().putInt(header + 12, generation);
            Unsafe.getUnsafe().putInt(header + 16, tableToken.getTableId());
            Unsafe.getUnsafe().putLong(header + 24, seqTxn);
            Unsafe.getUnsafe().putLong(header + 32, txn);
            Unsafe.getUnsafe().putLong(header + 40, columnVersion);
            Unsafe.getUnsafe().putLong(header + 96, metadataVersion);
            Unsafe.getUnsafe().putLong(header + BODY_SIZE, TableUtils.SNAPSHOT_CHECKSUM_MAGIC);
            Unsafe.getUnsafe().putLong(header + BODY_SIZE + 8, TableUtils.calculateCvAreaChecksum(header, BODY_SIZE));
            tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
            if (ff.write(fd, header, FILE_SIZE, 0) != FILE_SIZE) {
                throw CairoException.critical(ff.errno()).put("could not write adaptive epoch manifest header [path=").put(tablePath).put(']');
            }
            ff.fsync(fd);
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
     * <p>
     * The trim is deliberately OUTSIDE the platform guard and in a {@code finally}. Restoring someone
     * else's path is not a Windows concern, and when it was bracketed with the fsync inside
     * {@code if (!Os.isWindows())} the Windows build silently lost the cleanup too: the caller kept a path
     * still ending in {@code _epoch.manifest.<generation>}, concatenated onto it, and every structural DDL
     * then failed trying to open {@code <table>/_epoch.manifest.1/_meta.swp}. A platform guard must never
     * wrap a side effect that is not platform-specific. See {@code DurableEpochPathRestoreTest}.
     */
    public static void fsyncDirectory(CairoConfiguration configuration, Path tablePath, int rootLen) {
        try {
            if (Os.isWindows()) {
                return; // no directory fsync on Windows -- but the caller still gets its path back
            }
            final FilesFacade ff = configuration.getFilesFacade();
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

    /**
     * Flushes a table's materialized state file by file: every file of every attached partition and the partition
     * directory, then every file in the table root and the root itself, then one device barrier. This is the
     * fallback for platforms where {@link FilesFacade#syncfs(long)} flushes a single file, and it covers the same
     * write set as the writer's first epoch there ({@code TableWriter.fsyncAttachedPartitionFiles}). A read-only
     * partition is a soft link to storage nothing writes, so it holds nothing to flush.
     *
     * <p>The per-file fsync moves each file's bytes to the drive, and the closing {@code syncfs} flushes the drive
     * cache once for all of them: F_FULLFSYNC on macOS acts as a barrier for data fsync'd on the same device before
     * it, and on Windows each file's flush is already durable.
     */
    public static void fsyncTableFiles(CairoConfiguration configuration, TableToken tableToken, int timestampType, int partitionBy) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
            final int rootLen = path.of(configuration.getDbRoot()).concat(tableToken).size();
            txReader.ofRO(path.concat(TableUtils.TXN_FILE_NAME).$(), timestampType, partitionBy);
            if (!txReader.unsafeLoadAll()) {
                throw CairoException.critical(0).put("could not read adaptive _txn to flush the table [table=")
                        .put(tableToken.getTableName()).put(']');
            }
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.isPartitionReadOnly(i)) {
                    continue;
                }
                TableUtils.setPathForNativePartition(
                        path.trimTo(rootLen),
                        timestampType,
                        partitionBy,
                        txReader.getPartitionTimestampByIndex(i),
                        txReader.getPartitionNameTxn(i)
                );
                fsyncDirectoryFiles(configuration, path, path.size());
            }
            fsyncDirectoryFiles(configuration, path, rootLen);
            path.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME);
            final long fd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
            if (fd == -1) {
                throw CairoException.critical(ff.errno()).put("could not open adaptive _txn for the device flush [path=").put(path).put(']');
            }
            try {
                ff.syncfs(fd);
            } finally {
                ff.close(fd);
            }
        }
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

    private static void fsyncDirectoryFiles(CairoConfiguration configuration, Path dirPath, int dirLen) {
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
                    final long fd = TableUtils.openRW(ff, dirPath.$(), LOG, configuration.getWriterFileOpenOpts());
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

    private static boolean liveFileEquals(FilesFacade ff, Path tablePath, int rootLen, String name, long sectionAddr, long sectionSize) {
        tablePath.trimTo(rootLen).concat(name);
        try {
            if (ff.length(tablePath.$()) != sectionSize) {
                return false;
            }
            try (MemoryCMR live = Vm.getCMRInstance(ff, tablePath.$(), sectionSize, MemoryTag.MMAP_TABLE_READER)) {
                return Vect.memeq(live.addressOf(0), sectionAddr, sectionSize);
            }
        } catch (CairoException | CairoError e) {
            // Any failure to compare sends the file down the restore.
            return false;
        } finally {
            tablePath.trimTo(rootLen);
        }
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
            final long fileSize = ff.length(tablePath.$());
            if (!ff.exists(tablePath.$()) || fileSize <= FILE_SIZE) {
                return false;
            }
            try (MemoryCMR mem = Vm.getCMRInstance(ff, tablePath.$(), fileSize, MemoryTag.MMAP_TABLE_READER)) {
                if (mem.getLong(0) != TableUtils.SNAPSHOT_CHECKSUM_MAGIC
                        || mem.getInt(8) != FORMAT_VERSION
                        || mem.getInt(12) != generation
                        || mem.getInt(16) != tableToken.getTableId()
                        || mem.getLong(24) != seqTxn
                        || mem.getLong(32) != txn
                        || (checkVersions && (mem.getLong(40) != columnVersion || mem.getLong(96) != metadataVersion))
                        || mem.getLong(BODY_SIZE) != TableUtils.SNAPSHOT_CHECKSUM_MAGIC
                        || mem.getLong(BODY_SIZE + 8) != TableUtils.calculateCvAreaChecksum(mem.addressOf(0), BODY_SIZE)) {
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
        } catch (Throwable e) {
            return false;
        } finally {
            tablePath.trimTo(rootLen);
        }
    }

    /**
     * Appends the live file {@code name} to the save point file at {@code offset}, records its size and
     * checksum at {@code sizeAddr} (the checksum in the next long), and returns the offset after it.
     */
    private static long writeSection(FilesFacade ff, Path tablePath, int rootLen, String name, long fd, long offset, long sizeAddr) {
        tablePath.trimTo(rootLen).concat(name);
        final long size = ff.length(tablePath.$());
        if (size <= 0) {
            throw CairoException.critical(ff.errno()).put("invalid adaptive epoch section size [path=").put(tablePath).put(", size=").put(size).put(']');
        }
        try (MemoryCMR mem = Vm.getCMRInstance(ff, tablePath.$(), size, MemoryTag.MMAP_TABLE_READER)) {
            if (ff.write(fd, mem.addressOf(0), size, offset) != size) {
                throw CairoException.critical(ff.errno()).put("could not write adaptive epoch section [path=").put(tablePath).put(", size=").put(size).put(']');
            }
            Unsafe.getUnsafe().putLong(sizeAddr, size);
            Unsafe.getUnsafe().putLong(sizeAddr + 8, TableUtils.calculateCvAreaChecksum(mem.addressOf(0), size));
        } finally {
            tablePath.trimTo(rootLen);
        }
        return offset + size;
    }
}
