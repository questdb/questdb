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
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.str.Path;

/** Binds the metadata, transaction, and column-version epoch payloads to one table and marker generation. */
public final class DurableEpochManifest {
    public static final String FILE_NAME = "_epoch.manifest";
    public static final int FILE_SIZE = 120;
    private static final int FORMAT_VERSION = 2;
    private static final int LEGACY_BODY_SIZE = 88;
    private static final int LEGACY_FILE_SIZE = 104;
    private static final int LEGACY_FORMAT_VERSION = 1;
    private static final Log LOG = LogFactory.getLog(DurableEpochManifest.class);
    private static final int BODY_SIZE = 104;

    private DurableEpochManifest() {
    }

    public static void publishInitial(
            CairoConfiguration configuration,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long nowMs
    ) {
        publishBaseline(configuration, tableToken, timestampType, partitionBy, nowMs, true);
    }

    public static void publishCheckpointRestored(
            CairoConfiguration configuration,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long nowMs
    ) {
        publishBaseline(configuration, tableToken, timestampType, partitionBy, nowMs, false);
    }

    private static void publishBaseline(
            CairoConfiguration configuration,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long nowMs,
            boolean requireInitialSeqTxn
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path tablePath = new Path(); Path src = new Path(); Path dst = new Path()) {
            tablePath.of(configuration.getDbRoot()).concat(tableToken);
            final int rootLen = tablePath.size();
            fsyncFile(configuration, tablePath, rootLen, TableUtils.META_FILE_NAME);
            fsyncFile(configuration, tablePath, rootLen, TableUtils.COLUMN_VERSION_FILE_NAME);
            fsyncFile(configuration, tablePath, rootLen, TableUtils.TXN_FILE_NAME);
            copyPayload(configuration, tablePath, src, dst, rootLen, TableUtils.META_FILE_NAME, 0);
            copyPayload(configuration, tablePath, src, dst, rootLen, TableUtils.COLUMN_VERSION_FILE_NAME, 0);
            copyPayload(configuration, tablePath, src, dst, rootLen, TableUtils.TXN_FILE_NAME, 0);

            long seqTxn;
            long txn;
            long columnVersion;
            long metadataVersion;
            try (TxReader txReader = new TxReader(ff); ColumnVersionReader cvReader = new ColumnVersionReader()) {
                src.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(0);
                txReader.ofRO(src.$(), timestampType, partitionBy);
                if (!txReader.unsafeLoadAll()) {
                    throw CairoException.critical(0).put("could not validate initial adaptive _txn baseline [table=").put(tableToken.getTableName()).put(']');
                }
                src.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(0);
                cvReader.ofRO(ff, src.$());
                if (!cvReader.readSafe() || txReader.getColumnVersion() != cvReader.getVersion()) {
                    throw CairoException.critical(0).put("could not validate initial adaptive _cv baseline [table=").put(tableToken.getTableName()).put(']');
                }
                seqTxn = txReader.getSeqTxn();
                txn = txReader.getTxn();
                columnVersion = txReader.getColumnVersion();
                metadataVersion = txReader.getMetadataVersion();
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
        }
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
        if (generation < 0) {
            return false;
        }
        final FilesFacade ff = configuration.getFilesFacade();
        tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
        final long manifestSize = ff.length(tablePath.$());
        if (!ff.exists(tablePath.$()) || manifestSize < LEGACY_FILE_SIZE) {
            return false;
        }
        try (MemoryCMR mem = Vm.getCMRInstance(ff, tablePath.$(), Math.min(manifestSize, FILE_SIZE), MemoryTag.MMAP_TABLE_READER);
             Path payload = new Path()) {
            final int formatVersion = mem.getInt(8);
            final int bodySize;
            if (formatVersion == LEGACY_FORMAT_VERSION) {
                bodySize = LEGACY_BODY_SIZE;
            } else if (formatVersion == FORMAT_VERSION && manifestSize >= FILE_SIZE) {
                bodySize = BODY_SIZE;
            } else {
                return false;
            }
            if (mem.getLong(0) != TableUtils.SNAPSHOT_CHECKSUM_MAGIC
                    || mem.getInt(12) != generation
                    || mem.getInt(16) != tableToken.getTableId()
                    || mem.getLong(24) != seqTxn
                    || mem.getLong(32) != txn
                    || mem.getLong(40) != columnVersion
                    || mem.getLong(bodySize) != TableUtils.SNAPSHOT_CHECKSUM_MAGIC
                    || mem.getLong(bodySize + 8) != TableUtils.calculateCvAreaChecksum(mem.addressOf(0), bodySize)) {
                return false;
            }
            final long txnSize = mem.getLong(48);
            final long txnChecksum = mem.getLong(56);
            final long cvSize = mem.getLong(64);
            final long cvChecksum = mem.getLong(72);
            payload.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME)
                    .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            if (!payloadMatches(ff, payload, txnSize, txnChecksum)) {
                return false;
            }
            payload.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.COLUMN_VERSION_FILE_NAME)
                    .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            if (!payloadMatches(ff, payload, cvSize, cvChecksum)) {
                return false;
            }
            if (formatVersion == LEGACY_FORMAT_VERSION) {
                return true;
            }
            if (mem.getLong(96) != metadataVersion) {
                return false;
            }
            final long metaSize = mem.getLong(80);
            final long metaChecksum = mem.getLong(88);
            payload.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.META_FILE_NAME)
                    .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            return payloadMatches(ff, payload, metaSize, metaChecksum);
        } catch (Throwable e) {
            return false;
        }
    }

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
        try (Path payload = new Path()) {
            payload.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME)
                    .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            final long txnSize = ff.length(payload.$());
            final long txnChecksum = checksum(ff, payload, txnSize);
            payload.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.COLUMN_VERSION_FILE_NAME)
                    .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            final long cvSize = ff.length(payload.$());
            final long cvChecksum = checksum(ff, payload, cvSize);
            payload.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.META_FILE_NAME)
                    .put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            final long metaSize = ff.length(payload.$());
            final long metaChecksum = checksum(ff, payload, metaSize);

            tablePath.trimTo(rootLen).concat(FILE_NAME).put('.').put(generation);
            try (MemoryCMARW mem = Vm.getSmallCMARWInstance(ff, tablePath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                mem.jumpTo(FILE_SIZE);
                mem.putLong(0, TableUtils.SNAPSHOT_CHECKSUM_MAGIC);
                mem.putInt(8, FORMAT_VERSION);
                mem.putInt(12, generation);
                mem.putInt(16, tableToken.getTableId());
                mem.putInt(20, 0);
                mem.putLong(24, seqTxn);
                mem.putLong(32, txn);
                mem.putLong(40, columnVersion);
                mem.putLong(48, txnSize);
                mem.putLong(56, txnChecksum);
                mem.putLong(64, cvSize);
                mem.putLong(72, cvChecksum);
                mem.putLong(80, metaSize);
                mem.putLong(88, metaChecksum);
                mem.putLong(96, metadataVersion);
                mem.putLong(BODY_SIZE, TableUtils.SNAPSHOT_CHECKSUM_MAGIC);
                mem.putLong(BODY_SIZE + 8, TableUtils.calculateCvAreaChecksum(mem.addressOf(0), BODY_SIZE));
                mem.sync(false);
                ff.fsync(mem.getFd());
            }
        }
    }

    public static void fsyncDirectory(CairoConfiguration configuration, Path tablePath, int rootLen) {
        if (Os.isWindows()) {
            return;
        }
        final FilesFacade ff = configuration.getFilesFacade();
        tablePath.trimTo(rootLen).slash$();
        final long fd = TableUtils.openRONoCache(ff, tablePath.$(), LOG);
        if (fd == -1) {
            throw CairoException.critical(ff.errno()).put("could not open adaptive epoch directory for fsync [path=").put(tablePath).put(']');
        }
        ff.fsyncAndClose(fd);
    }

    private static long checksum(FilesFacade ff, Path path, long size) {
        if (size <= 0) {
            throw CairoException.critical(ff.errno()).put("invalid adaptive epoch payload size [path=").put(path).put(", size=").put(size).put(']');
        }
        try (MemoryCMR mem = Vm.getCMRInstance(ff, path.$(), size, MemoryTag.MMAP_TABLE_READER)) {
            return TableUtils.calculateCvAreaChecksum(mem.addressOf(0), size);
        }
    }

    private static void copyPayload(CairoConfiguration configuration, Path tablePath, Path src, Path dst, int rootLen, String name, int generation) {
        final FilesFacade ff = configuration.getFilesFacade();
        src.of(tablePath).trimTo(rootLen).concat(name);
        dst.of(tablePath).trimTo(rootLen).concat(name).put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
        if (ff.copy(src.$(), dst.$()) < 0) {
            throw CairoException.critical(ff.errno()).put("could not create initial adaptive epoch payload [path=").put(dst).put(']');
        }
        final long fd = TableUtils.openRW(ff, dst.$(), LOG, configuration.getWriterFileOpenOpts());
        if (fd == -1) {
            throw CairoException.critical(ff.errno()).put("could not open initial adaptive epoch payload [path=").put(dst).put(']');
        }
        try {
            ff.fsync(fd);
        } finally {
            ff.close(fd);
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

    private static boolean payloadMatches(FilesFacade ff, Path path, long expectedSize, long expectedChecksum) {
        return expectedSize > 0 && ff.exists(path.$()) && ff.length(path.$()) == expectedSize
                && checksum(ff, path, expectedSize) == expectedChecksum;
    }
}
