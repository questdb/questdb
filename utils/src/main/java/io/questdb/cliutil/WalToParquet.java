/*+*****************************************************************************
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
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cliutil;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.wal.seq.SequencerMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// WAL-to-Parquet forensic recovery tool.
//
// Discovers WAL tables under a QuestDB db root and salvages each un-purged
// segment to a Parquet file. No CairoEngine boot, strict read-only access to
// the running DB. One Parquet file per (walId, segmentId), named:
//
//   <tableName>__wal<walId>__seg<segId>__seqTxn<lo>-<hi>.parquet
//
// SYMBOL columns are encoded as Parquet dictionary-encoded BYTE_ARRAY using
// the WAL's own symbol map files (wal<N>/<column>.c/.k/.o), so no
// materialization is needed. All other columns are zero-copy from the
// WalReader's mapped memory.
//
// Live-instance safety:
//   Strict read-only. Every file the tool opens is opened RO. We never write,
//   delete, mmap-as-RW, or hold exclusive locks. Safe to run against a live
//   QuestDB instance under snapshot semantics: anything visible in _txnlog at
//   the time of the walk is in scope; anything appended afterwards is ignored.
//
//   Concretely: we read the _txnlog header (format version + partTransactionCount
//   + maxStructureVersion + maxTxn) ourselves via a transient RO file handle,
//   construct TableTransactionLogV1/V2 without calling open() (which opens RW),
//   and use getCursor() directly. The cursor implementations open their own RO
//   handles internally for record iteration.
public class WalToParquet {

    public static void main(String[] args) {
        LogFactory.enableGuaranteedLogging();

        Args parsed = Args.parse(args);
        if (parsed == null) {
            printUsage();
            System.exit(1);
            return;
        }

        run(parsed);
    }

    private static String buildParquetFileName(TableToken token, SegmentInfo info, boolean tier3) {
        StringBuilder sb = new StringBuilder()
                .append(token.getTableName())
                .append("__wal").append(info.walId)
                .append("__seg").append(info.segmentId);
        if (info.firstSeqTxn >= 0) {
            sb.append("__seqTxn").append(info.firstSeqTxn).append("-").append(info.lastSeqTxn);
        } else {
            sb.append("__tier2");
        }
        if (tier3) {
            sb.append("__tier3");
        }
        return sb.append(".parquet").toString();
    }

    // Walk dbRoot, return every dir that looks like a WAL table directory.
    // Filters out internal files (_tab_index.d, tables.d.*, _query_trace*, etc.)
    // and, by default, system tables (sys.*). A dir is considered a WAL table if
    // it has a txn_seq/_txnlog file inside it.
    private static List<TableInfo> discoverTables(String dbRoot, boolean includeSystem) {
        List<TableInfo> tables = new ArrayList<>();
        File root = new File(dbRoot);
        File[] entries = root.listFiles();
        if (entries == null) {
            return tables;
        }
        for (File entry : entries) {
            if (!entry.isDirectory()) {
                continue;
            }
            String name = entry.getName();
            if (name.startsWith("_") || name.startsWith(".")) {
                continue;
            }
            if (!includeSystem && name.startsWith("sys.")) {
                continue;
            }
            File txnLog = new File(entry, WalUtils.SEQ_DIR + File.separator + WalUtils.TXNLOG_FILE_NAME);
            if (!txnLog.isFile()) {
                continue;
            }
            tables.add(TableInfo.fromDirName(name, hasUnpurgedWal(entry)));
        }
        Collections.sort(tables, (a, b) -> a.dirName.compareTo(b.dirName));
        return tables;
    }

    private static List<SegmentInfo> enumerateSegments(TransactionLogCursor cursor, Manifest manifest) {
        // (walId, segmentId) -> SegmentInfo. Linear scan is fine; segment counts are small in practice.
        List<SegmentInfo> segments = new ArrayList<>();
        while (cursor.hasNext()) {
            int walId = cursor.getWalId();
            int segmentId = cursor.getSegmentId();
            long seqTxn = cursor.getTxn();
            long commitTs = cursor.getCommitTimestamp();

            if (walId == TableTransactionLogFile.STRUCTURAL_CHANGE_WAL_ID) {
                ManifestStructuralChange sc = new ManifestStructuralChange();
                sc.seqTxn = seqTxn;
                sc.commitTimestamp = commitTs;
                manifest.structuralChanges.add(sc);
                System.out.println("  wal=N/A seg=N/A structuralChange seqTxn=" + seqTxn);
                continue;
            }

            SegmentInfo info = findOrCreate(segments, walId, segmentId);
            int segmentTxn = cursor.getSegmentTxn();
            if (info.txnCount == 0) {
                info.firstSeqTxn = seqTxn;
                info.firstCommitTs = commitTs;
            }
            info.lastSeqTxn = seqTxn;
            info.lastCommitTs = commitTs;
            info.lastSegmentTxn = Math.max(info.lastSegmentTxn, segmentTxn);
            info.txnCount++;
            // Park (seqTxn, commitTs) at array index = segmentTxn for O(1) lookup later.
            while (info.seqTxns.size() <= segmentTxn) {
                info.seqTxns.add(-1L);
                info.commitTimestamps.add(-1L);
            }
            info.seqTxns.set(segmentTxn, seqTxn);
            info.commitTimestamps.set(segmentTxn, commitTs);
        }
        return segments;
    }

    private static SegmentInfo findOrCreate(List<SegmentInfo> segments, int walId, int segmentId) {
        for (SegmentInfo s : segments) {
            if (s.walId == walId && s.segmentId == segmentId) {
                return s;
            }
        }
        SegmentInfo s = new SegmentInfo(walId, segmentId);
        segments.add(s);
        return s;
    }

    private static boolean hasUnpurgedWal(File tableDir) {
        File[] children = tableDir.listFiles();
        if (children == null) {
            return false;
        }
        for (File child : children) {
            if (child.isDirectory() && child.getName().startsWith(WalUtils.WAL_NAME_BASE)
                    && child.getName().length() > WalUtils.WAL_NAME_BASE.length()) {
                // Must end with at least one digit after "wal" to be a WAL segment dir.
                char first = child.getName().charAt(WalUtils.WAL_NAME_BASE.length());
                if (first >= '0' && first <= '9') {
                    return true;
                }
            }
        }
        return false;
    }

    // Read _txnlog header (format version + partition size + max txn + max
    // structure version) using a transient RO file handle, then build the
    // matching TableTransactionLogV1/V2 instance with constructor-passed
    // partition size. Caller never invokes open() on the result, so the only
    // file handles touched are RO ones the cursor opens internally.
    private static TxnLogHeader openTxnLogStrictRO(CairoConfiguration config, Path seqDir) {
        FilesFacade ff = config.getFilesFacade();
        int pathLen = seqDir.size();
        long fd = TableUtils.openRO(ff, seqDir.concat(WalUtils.TXNLOG_FILE_NAME).$(), LogFactory.getLog(WalToParquet.class));
        try {
            int formatVersion = ff.readNonNegativeInt(fd, 0);
            if (formatVersion < 0) {
                throw new RuntimeException("cannot read transaction log version at " + seqDir);
            }
            long maxTxn = ff.readNonNegativeLong(fd, TableTransactionLogFile.MAX_TXN_OFFSET_64);
            if (maxTxn < 0) {
                throw new RuntimeException("cannot read max txn at " + seqDir);
            }

            TableTransactionLogFile txnLog;
            long maxStructureVersion = 0;
            switch (formatVersion) {
                case WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1 -> {
                    txnLog = new TableTransactionLogV1(config);
                    if (maxTxn > 0) {
                        long lastRecordOffset = TableTransactionLogFile.HEADER_SIZE
                                + (maxTxn - 1) * TableTransactionLogV1.RECORD_SIZE
                                + TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET;
                        long sv = ff.readNonNegativeLong(fd, lastRecordOffset);
                        if (sv >= 0) {
                            maxStructureVersion = sv;
                        }
                    }
                }
                case WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2 -> {
                    int partTransactionCount = ff.readNonNegativeInt(fd, TableTransactionLogFile.HEADER_SEQ_PART_SIZE_32);
                    if (partTransactionCount < 1) {
                        throw new RuntimeException("invalid V2 partTransactionCount=" + partTransactionCount + " at " + seqDir);
                    }
                    txnLog = new TableTransactionLogV2(config, partTransactionCount, new ReadOnlyWalDirectoryPolicy());
                    // V2's maxStructureVersion lives in the corresponding part file; for our
                    // purposes the walk itself surfaces structural-change records, so we skip
                    // reading it here to avoid opening another file just for the header value.
                }
                default -> throw new RuntimeException("unsupported transaction log format version: " + formatVersion);
            }
            return new TxnLogHeader(txnLog, formatVersion, maxTxn, maxStructureVersion);
        } finally {
            ff.close(fd);
            seqDir.trimTo(pathLen);
        }
    }

    // WalReader stores columns in a flat ObjList: 2 leading null slots, then 2
    // slots per column (primary, aux). The static helper that computes this is
    // WalReader.getPrimaryColumnIndex but it's package-private; we inline the
    // same formula here. If WalReader changes layout, update this in lock-step.
    private static int primaryColumnIndex(int columnIndex) {
        return columnIndex * 2 + 2;
    }

    private static ManifestSegment processSegment(CairoConfiguration config, TableToken token, SegmentInfo info, String outputDir, boolean withShoulder) {
        ManifestSegment entry = new ManifestSegment();
        entry.walId = info.walId;
        entry.segmentId = info.segmentId;
        entry.firstSeqTxn = info.firstSeqTxn;
        entry.lastSeqTxn = info.lastSeqTxn;
        entry.lastSegmentTxn = info.lastSegmentTxn;
        entry.txnCount = info.txnCount;
        entry.firstCommitTs = info.firstCommitTs;
        entry.lastCommitTs = info.lastCommitTs;
        entry.status = "pending";

        String walName = WalUtils.WAL_NAME_BASE + info.walId;
        long rowCount;
        boolean tier3Fallback = false;
        try {
            rowCount = readSegmentCommittedRowCount(config, token, info.walId, info.segmentId, info.lastSegmentTxn);
        } catch (Throwable e) {
            // Tier 3: _event is missing or corrupt. Try to derive a rowCount from
            // the designated timestamp column's .d file size (16B/row in WAL).
            // The result may include un-committed tail rows.
            long heuristic = readSegmentRowCountFromTsFile(config, token, info.walId, info.segmentId);
            if (heuristic <= 0) {
                entry.status = "skipped_event_unreadable";
                entry.reason = e.getMessage();
                System.out.println("  segment wal=" + info.walId + " seg=" + info.segmentId
                        + " seqTxnRange=[" + info.firstSeqTxn + "," + info.lastSeqTxn + "]"
                        + " event read failed: " + e.getMessage());
                return entry;
            }
            rowCount = heuristic;
            tier3Fallback = true;
            entry.reason = "tier-3 fallback: _event unreadable (" + e.getMessage() + "), rowCount derived from ts.d file size; tail may be torn";
            System.out.println("  segment wal=" + info.walId + " seg=" + info.segmentId
                    + " _event unreadable, fell back to ts.d-based rowCount=" + rowCount);
        }
        entry.rowsCommitted = rowCount;

        // Detect missing per-column files before opening WalReader so the
        // manifest records which specific columns are gone, even if the
        // subsequent WalReader open fails because of them.
        recordMissingColumnFiles(config, token, info.walId, info.segmentId, entry);

        System.out.println("  segment wal=" + info.walId + " seg=" + info.segmentId
                + " rows=" + rowCount + " txns=" + info.txnCount
                + " seqTxnRange=[" + info.firstSeqTxn + "," + info.lastSeqTxn + "]"
                + " lastSegmentTxn=" + info.lastSegmentTxn
                + " commitTsRange=[" + info.firstCommitTs + "," + info.lastCommitTs + "]");

        if (rowCount == 0) {
            entry.status = "skipped_no_rows";
            System.out.println("    no committed rows in this segment");
            return entry;
        }

        try (WalReader reader = new WalReader(config, token, walName, info.segmentId, rowCount)) {
            int columnCount = reader.getColumnCount();
            System.out.println("    columns=" + columnCount + " timestampIndex=" + reader.getTimestampIndex());
            for (int i = 0; i < columnCount; i++) {
                int type = reader.getColumnType(i);
                if (type < 0) {
                    continue;
                }
                System.out.println("      [" + i + "] " + reader.getColumnName(i) + " type=" + type);
            }
            if (outputDir != null) {
                writeSegmentToParquet(config, token, reader, info, rowCount, outputDir, entry, withShoulder);
                if (tier3Fallback && "written".equals(entry.status)) {
                    entry.status = "written_tier3_event_fallback";
                }
            } else {
                entry.status = "inspected_no_output";
            }
        } catch (Throwable e) {
            if (tier3Fallback && outputDir != null) {
                // WalReader requires _event to construct (builds in-memory symbol-diff
                // map). When _event is gone, fall back to direct mmap of the WAL's
                // on-disk artefacts. New-in-WAL symbols (lived only in _event) are
                // lost; everything within the base symbol-table snapshot is recovered.
                System.out.println("    WalReader unavailable, trying tier-3 direct-mmap path");
                writeSegmentToParquetTier3(config, token, info, rowCount, outputDir, entry, withShoulder);
            } else {
                entry.status = "skipped_reader_open_failed";
                entry.reason = (entry.reason == null ? "" : entry.reason + " | ") + e.getMessage();
                System.out.println("    open failed: " + e.getMessage());
            }
        }
        return entry;
    }

    private static void writeSegmentToParquet(
            CairoConfiguration config,
            TableToken token,
            WalReader reader,
            SegmentInfo info,
            long rowCount,
            String outputDir,
            ManifestSegment entry,
            boolean withShoulder
    ) {
        String walName = WalUtils.WAL_NAME_BASE + info.walId;
        File outDirFile = new File(outputDir);
        if (!outDirFile.exists() && !outDirFile.mkdirs()) {
            entry.status = "encode_failed";
            entry.reason = "could not create output dir " + outputDir;
            System.out.println("    output dir create failed: " + outputDir);
            return;
        }
        String outFileName = buildParquetFileName(token, info, false);
        Path destPath = new Path();
        destPath.of(outputDir).concat(outFileName).$();

        PartitionDescriptor descriptor = new PartitionDescriptor();
        // Native buffers we allocate ourselves for synthesized SYMBOL dictionaries
        // and the compact timestamp buffer. PartitionEncoder reads from them
        // during encode(); we free after.
        io.questdb.std.LongList synthesizedSymbolValuesAddrs = new io.questdb.std.LongList();
        io.questdb.std.LongList synthesizedSymbolValuesSizes = new io.questdb.std.LongList();
        io.questdb.std.LongList synthesizedSymbolOffsetsAddrs = new io.questdb.std.LongList();
        long compactTimestampAddr = 0;
        long compactTimestampSize = 0;
        // Shoulder columns: 5 native buffers, freed together.
        long shoulderWalIdAddr = 0;
        long shoulderSegmentIdAddr = 0;
        long shoulderSegmentTxnAddr = 0;
        long shoulderSeqTxnAddr = 0;
        long shoulderCommitTsAddr = 0;
        try {
            int timestampIndex = reader.getTimestampIndex();
            descriptor.of(token.getTableName(), rowCount, timestampIndex);

            int columnCount = reader.getColumnCount();
            int writtenColumns = 0;
            int skippedColumns = 0;
            for (int i = 0; i < columnCount; i++) {
                int columnType = reader.getColumnType(i);
                if (columnType <= 0) {
                    // Dropped or never-existed column slot.
                    skippedColumns++;
                    continue;
                }
                CharSequence columnName = reader.getColumnName(i);
                int columnId = i;
                long colTop = 0; // Segments roll on structural changes; no column-top within a segment.
                int parquetEncodingConfig = 0;
                int primaryIdx = primaryColumnIndex(i);
                MemoryCR primaryMem = reader.getColumn(primaryIdx);

                if (ColumnType.isSymbol(columnType)) {
                    long[] addrs = synthesizeSymbolBuffers(reader, i, primaryMem, rowCount);
                    if (addrs == null) {
                        skippedColumns++;
                        entry.skippedColumns.add(columnName + " (symbol: no resolvable values)");
                        System.out.println("    SYMBOL column '" + columnName + "' had no resolvable values - skipped");
                        continue;
                    }
                    synthesizedSymbolValuesAddrs.add(addrs[0]);
                    synthesizedSymbolValuesSizes.add(addrs[1]);
                    synthesizedSymbolOffsetsAddrs.add(addrs[2]);
                    int symbolCount = (int) addrs[3];

                    descriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            colTop,
                            primaryMem.addressOf(0),
                            primaryMem.size(),
                            addrs[0],
                            addrs[1],
                            addrs[2],
                            symbolCount,
                            parquetEncodingConfig
                    );
                } else if (ColumnType.isVarSize(columnType)) {
                    MemoryCR auxMem = reader.getColumn(primaryIdx + 1);
                    descriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            colTop,
                            primaryMem.addressOf(0),
                            primaryMem.size(),
                            auxMem.addressOf(0),
                            auxMem.size(),
                            0,
                            0,
                            parquetEncodingConfig
                    );
                } else if (i == timestampIndex) {
                    // WAL timestamp .d file stores 16 bytes per row: 8 bytes of
                    // real timestamp followed by 8 bytes of sequential rowID for
                    // O3 handling. Parquet expects 8 bytes per row, so allocate
                    // a compact buffer and stride-copy only the timestamp halves.
                    compactTimestampSize = rowCount * Long.BYTES;
                    compactTimestampAddr = io.questdb.std.Unsafe.malloc(compactTimestampSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    long src = primaryMem.addressOf(0);
                    for (long row = 0; row < rowCount; row++) {
                        long ts = io.questdb.std.Unsafe.getUnsafe().getLong(src + row * 16L);
                        io.questdb.std.Unsafe.getUnsafe().putLong(compactTimestampAddr + row * Long.BYTES, ts);
                    }
                    descriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            colTop,
                            compactTimestampAddr,
                            compactTimestampSize,
                            0,
                            0,
                            0,
                            0,
                            parquetEncodingConfig
                    );
                } else {
                    descriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            colTop,
                            primaryMem.addressOf(0),
                            primaryMem.size(),
                            0,
                            0,
                            0,
                            0,
                            parquetEncodingConfig
                    );
                }
                writtenColumns++;
            }

            if (withShoulder) {
                // Build per-row segmentTxn map by replaying the segment's _event file.
                int[] perRowSegmentTxn = readPerRowSegmentTxn(config, token, info.walId, info.segmentId, info.lastSegmentTxn, rowCount);
                int intSize = Integer.BYTES;
                int longSize = Long.BYTES;
                shoulderWalIdAddr = io.questdb.std.Unsafe.malloc(rowCount * intSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderSegmentIdAddr = io.questdb.std.Unsafe.malloc(rowCount * intSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderSegmentTxnAddr = io.questdb.std.Unsafe.malloc(rowCount * intSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderSeqTxnAddr = io.questdb.std.Unsafe.malloc(rowCount * longSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderCommitTsAddr = io.questdb.std.Unsafe.malloc(rowCount * longSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                for (long row = 0; row < rowCount; row++) {
                    int segTxn = perRowSegmentTxn[(int) row];
                    long seqTxn = -1L;
                    long commitTs = -1L;
                    if (segTxn >= 0 && segTxn < info.seqTxns.size()) {
                        seqTxn = info.seqTxns.getQuick(segTxn);
                        commitTs = info.commitTimestamps.getQuick(segTxn);
                    }
                    io.questdb.std.Unsafe.getUnsafe().putInt(shoulderWalIdAddr + row * intSize, info.walId);
                    io.questdb.std.Unsafe.getUnsafe().putInt(shoulderSegmentIdAddr + row * intSize, info.segmentId);
                    io.questdb.std.Unsafe.getUnsafe().putInt(shoulderSegmentTxnAddr + row * intSize, segTxn);
                    io.questdb.std.Unsafe.getUnsafe().putLong(shoulderSeqTxnAddr + row * longSize, seqTxn);
                    io.questdb.std.Unsafe.getUnsafe().putLong(shoulderCommitTsAddr + row * longSize, commitTs);
                }
                int nextColumnId = reader.getColumnCount();
                addShoulderColumn(descriptor, "_wal_id", ColumnType.INT, nextColumnId++, shoulderWalIdAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_segment_id", ColumnType.INT, nextColumnId++, shoulderSegmentIdAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_segment_txn", ColumnType.INT, nextColumnId++, shoulderSegmentTxnAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_seq_txn", ColumnType.LONG, nextColumnId++, shoulderSeqTxnAddr, rowCount * longSize);
                addShoulderColumn(descriptor, "_commit_ts", ColumnType.TIMESTAMP_MICRO, nextColumnId, shoulderCommitTsAddr, rowCount * longSize);
                writtenColumns += 5;
            }

            PartitionEncoder.encode(descriptor, destPath);
            entry.status = "written";
            entry.outputFile = outFileName;
            entry.columnsWritten = writtenColumns;
            entry.columnsSkipped = skippedColumns;
            entry.rowsWritten = rowCount;
            System.out.println("    wrote " + outFileName + " (" + writtenColumns + " columns, " + skippedColumns + " skipped)");
        } catch (Throwable e) {
            entry.status = "encode_failed";
            entry.reason = e.getMessage();
            System.out.println("    parquet encode failed: " + e.getMessage());
        } finally {
            for (int i = 0; i < synthesizedSymbolValuesAddrs.size(); i++) {
                long addr = synthesizedSymbolValuesAddrs.getQuick(i);
                long size = synthesizedSymbolValuesSizes.getQuick(i);
                if (addr != 0) {
                    io.questdb.std.Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                }
            }
            for (int i = 0; i < synthesizedSymbolOffsetsAddrs.size(); i++) {
                long addr = synthesizedSymbolOffsetsAddrs.getQuick(i);
                if (addr != 0) {
                    io.questdb.std.Unsafe.free(addr, 0, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                }
            }
            if (compactTimestampAddr != 0) {
                io.questdb.std.Unsafe.free(compactTimestampAddr, compactTimestampSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
            freeIfAllocated(shoulderWalIdAddr, rowCount * Integer.BYTES);
            freeIfAllocated(shoulderSegmentIdAddr, rowCount * Integer.BYTES);
            freeIfAllocated(shoulderSegmentTxnAddr, rowCount * Integer.BYTES);
            freeIfAllocated(shoulderSeqTxnAddr, rowCount * Long.BYTES);
            freeIfAllocated(shoulderCommitTsAddr, rowCount * Long.BYTES);
            Misc.free(descriptor);
            Misc.free(destPath);
        }
    }

    private static void addShoulderColumn(PartitionDescriptor descriptor, CharSequence name, int columnType, int columnId, long addr, long size) {
        descriptor.addColumn(
                name,
                columnType,
                columnId,
                0,    // colTop
                addr,
                size,
                0, 0, 0, 0,
                0     // parquetEncodingConfig
        );
    }

    private static void freeIfAllocated(long addr, long size) {
        if (addr != 0) {
            io.questdb.std.Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        }
    }

    // Walk the segment's _event file building an int[] where perRowSegmentTxn[r]
    // gives the segmentTxn that wrote row r. Honours the txnlog ceiling
    // (lastSegmentTxn) so we never assign txns beyond our snapshot.
    private static int[] readPerRowSegmentTxn(CairoConfiguration config, TableToken token, int walId, int segmentId, int lastSegmentTxn, long rowCount) {
        int[] result = new int[(int) rowCount];
        java.util.Arrays.fill(result, -1);
        Path path = new Path();
        try {
            path.of(config.getDbRoot()).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
            try (WalEventReader er = new WalEventReader(config)) {
                WalEventCursor c = er.of(path, -1);
                while (c.hasNext()) {
                    long txn = c.getTxn();
                    if (txn > lastSegmentTxn) {
                        break;
                    }
                    if (!WalTxnType.isDataType(c.getType())) {
                        continue;
                    }
                    WalEventCursor.DataInfo di = c.getDataInfo();
                    long start = di.getStartRowID();
                    long end = di.getEndRowID();
                    int segTxn = (int) txn;
                    long upper = Math.min(end, rowCount);
                    for (long r = start; r < upper; r++) {
                        result[(int) r] = segTxn;
                    }
                }
            }
        } finally {
            Misc.free(path);
        }
        return result;
    }

    // Build a symbol dictionary in native memory for one SYMBOL column. Walks
    // the column's .d file to find the maximum referenced key, then resolves
    // each key via WalReader.getSymbolValue (which already merges the WAL's
    // base snapshot with any new symbols added during this segment's commits).
    //
    // Returns long[]{ valuesAddr, valuesSize, offsetsAddr, symbolCount }, or
    // null if no non-null symbol codes are referenced in the column.
    //
    // Layout produced:
    //   values: per symbol key, an int length (UTF-16 code units) followed by
    //           that many UTF-16 LE code units. Length == -1 marks a null entry
    //           (used when a key has no resolvable value).
    //   offsets: long[symbolCount], offsets[k] = byte offset of symbol k's
    //            length prefix in the values buffer.
    private static long[] synthesizeSymbolBuffers(WalReader reader, int columnIndex, MemoryCR primaryMem, long rowCount) {
        int maxKey = -1;
        long codesAddr = primaryMem.addressOf(0);
        for (long row = 0; row < rowCount; row++) {
            int code = io.questdb.std.Unsafe.getUnsafe().getInt(codesAddr + row * Integer.BYTES);
            if (code >= 0 && code > maxKey) {
                maxKey = code;
            }
        }
        if (maxKey < 0) {
            // All rows are SYMBOL null. Still need to emit at least one dictionary
            // entry so the encoder is happy; we synthesise an empty 1-entry dict.
            maxKey = 0;
        }
        int symbolCount = maxKey + 1;

        // First pass to compute total values buffer size.
        long valuesSize = 0;
        String[] resolved = new String[symbolCount];
        for (int k = 0; k < symbolCount; k++) {
            CharSequence cs = reader.getSymbolValue(columnIndex, k);
            String s = cs == null ? null : cs.toString();
            resolved[k] = s;
            valuesSize += Integer.BYTES;
            if (s != null) {
                valuesSize += 2L * s.length();
            }
        }

        long valuesAddr = io.questdb.std.Unsafe.malloc(valuesSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        long offsetsAddr = io.questdb.std.Unsafe.malloc((long) symbolCount * Long.BYTES, io.questdb.std.MemoryTag.NATIVE_DEFAULT);

        long cursor = 0;
        for (int k = 0; k < symbolCount; k++) {
            io.questdb.std.Unsafe.getUnsafe().putLong(offsetsAddr + (long) k * Long.BYTES, cursor);
            String s = resolved[k];
            if (s == null) {
                io.questdb.std.Unsafe.getUnsafe().putInt(valuesAddr + cursor, -1);
                cursor += Integer.BYTES;
            } else {
                io.questdb.std.Unsafe.getUnsafe().putInt(valuesAddr + cursor, s.length());
                cursor += Integer.BYTES;
                for (int i = 0, n = s.length(); i < n; i++) {
                    char c = s.charAt(i);
                    io.questdb.std.Unsafe.getUnsafe().putByte(valuesAddr + cursor, (byte) (c & 0xFF));
                    io.questdb.std.Unsafe.getUnsafe().putByte(valuesAddr + cursor + 1, (byte) ((c >> 8) & 0xFF));
                    cursor += 2;
                }
            }
        }
        return new long[]{valuesAddr, valuesSize, offsetsAddr, symbolCount};
    }


    private static void printUsage() {
        System.out.println("usage: " + WalToParquet.class.getName() + " --db-root <path> [options]");
        System.out.println();
        System.out.println("  --db-root         QuestDB data root (the directory containing per-table dirs)");
        System.out.println("  --output-dir      where Parquet files will be written (required when sink is enabled)");
        System.out.println();
        System.out.println("  Filtering (defaults to: all user tables with un-purged WAL data):");
        System.out.println("  --table-dir       process only this table directory (skips discovery)");
        System.out.println("  --table-name      logical name to log for the single-table mode");
        System.out.println("  --table-id        numeric table id for the single-table mode");
        System.out.println("  --include-system  also process sys.* tables (off by default)");
        System.out.println("  --include-empty   also report tables whose WALs are fully purged (off by default)");
        System.out.println("  --no-shoulder     do not emit per-row provenance columns");
        System.out.println("                    (_wal_id, _segment_id, _segment_txn, _seq_txn, _commit_ts)");
    }

    private static void processTable(CairoConfiguration config, TableInfo table, String outputDir, boolean withShoulder) {
        System.out.println();
        System.out.println("=== table=" + table.tableName + " tableDir=" + table.dirName + " tableId=" + table.tableId
                + (table.hasUnpurgedWal ? "" : " (no un-purged WAL data on disk)") + " ===");
        TableToken token = new TableToken(table.tableName, table.dirName, null, table.tableId, true, false, false);

        Manifest manifest = new Manifest();
        manifest.tool = "WalToParquet";
        manifest.generatedAt = Instant.now().toString();
        manifest.dbRoot = config.getDbRoot().toString();
        manifest.table = table.tableName;
        manifest.tableDir = table.dirName;
        manifest.tableId = table.tableId;
        manifest.txnLog = new ManifestTxnLog();

        Path seqDir = new Path();
        seqDir.of(config.getDbRoot()).concat(table.dirName).concat(WalUtils.SEQ_DIR);

        TxnLogHeader header = null;
        TransactionLogCursor cursor = null;
        try {
            header = openTxnLogStrictRO(config, seqDir);
            manifest.txnLog.status = "ok";
            manifest.txnLog.formatVersion = header.formatVersion;
            manifest.txnLog.maxTxn = header.maxTxn;
            manifest.txnLog.maxStructureVersion = header.maxStructureVersion;
            System.out.println("  formatVersion=" + header.formatVersion
                    + " maxTxn=" + header.maxTxn
                    + (header.formatVersion == WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1
                    ? " maxStructureVersion=" + header.maxStructureVersion : ""));

            cursor = header.txnLog.getCursor(0L, seqDir);
            List<SegmentInfo> segments = enumerateSegments(cursor, manifest);
            System.out.println("  found " + segments.size() + " segment(s) referenced from _txnlog");

            for (SegmentInfo info : segments) {
                ManifestSegment entry = processSegment(config, token, info, outputDir, withShoulder);
                manifest.segments.add(entry);
            }
        } catch (Throwable e) {
            manifest.txnLog.status = "error";
            manifest.txnLog.error = e.getMessage();
            System.out.println("  txnlog read failed: " + e.getMessage());
            System.out.println("  attempting tier-2 filesystem scan");
            // Tier-2: txnlog unreadable. Scan wal*/N/ dirs directly. We lose
            // cross-segment ordering (seqTxn) and have to process every segment
            // we find, applied or not.
            List<SegmentInfo> tier2 = scanWalDirsForSegments(config, table);
            System.out.println("  filesystem scan found " + tier2.size() + " segment(s)");
            for (SegmentInfo info : tier2) {
                ManifestSegment entry = processSegment(config, token, info, outputDir, withShoulder);
                manifest.segments.add(entry);
            }
        } finally {
            Misc.free(cursor);
            if (header != null) {
                Misc.free(header.txnLog);
            }
            Misc.free(seqDir);
        }

        if (outputDir != null) {
            writeManifest(manifest, outputDir);
        }
    }

    // Tier-2 filesystem scan: enumerate wal<N>/<M>/ subdirectories on disk and
    // return one SegmentInfo per segment. Used when _txnlog is unreadable, so we
    // can't derive seqTxn / commitTs - those fields stay sentinel (-1) and the
    // segment's lastSegmentTxn is set to Integer.MAX_VALUE so the downstream
    // _event walk accepts every record.
    private static List<SegmentInfo> scanWalDirsForSegments(CairoConfiguration config, TableInfo table) {
        List<SegmentInfo> result = new ArrayList<>();
        File tableDirFile = new File(config.getDbRoot().toString(), table.dirName);
        File[] entries = tableDirFile.listFiles();
        if (entries == null) {
            return result;
        }
        for (File walEntry : entries) {
            if (!walEntry.isDirectory()) {
                continue;
            }
            String walDirName = walEntry.getName();
            if (!walDirName.startsWith(WalUtils.WAL_NAME_BASE) || walDirName.length() <= WalUtils.WAL_NAME_BASE.length()) {
                continue;
            }
            int walId;
            try {
                walId = Numbers.parseInt(walDirName.substring(WalUtils.WAL_NAME_BASE.length()));
            } catch (NumericException e) {
                continue;
            }
            File[] segs = walEntry.listFiles();
            if (segs == null) {
                continue;
            }
            for (File seg : segs) {
                if (!seg.isDirectory()) {
                    continue;
                }
                int segId;
                try {
                    segId = Numbers.parseInt(seg.getName());
                } catch (NumericException e) {
                    continue;
                }
                SegmentInfo info = new SegmentInfo(walId, segId);
                info.firstSeqTxn = -1;
                info.lastSeqTxn = -1;
                info.lastSegmentTxn = Integer.MAX_VALUE;
                info.firstCommitTs = -1;
                info.lastCommitTs = -1;
                info.txnCount = 0;
                result.add(info);
            }
        }
        Collections.sort(result, (a, b) -> {
            int c = Integer.compare(a.walId, b.walId);
            return c != 0 ? c : Integer.compare(a.segmentId, b.segmentId);
        });
        return result;
    }

    // Walk the segment's _event file to derive the committed row count, honouring
    // the txnlog ceiling (lastSegmentTxn). _event is the canonical source for the
    // commit boundary. Symbol counts are derived separately from the .o file
    // content because columns with no new symbols in the segment may not appear in
    // any SymbolMapDiff and we'd undercount.
    private static long readSegmentCommittedRowCount(CairoConfiguration config, TableToken token, int walId, int segmentId, int lastSegmentTxn) {
        Path path = new Path();
        try {
            path.of(config.getDbRoot()).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
            try (WalEventReader er = new WalEventReader(config)) {
                WalEventCursor c = er.of(path, -1);
                long endRowId = 0;
                while (c.hasNext()) {
                    long txn = c.getTxn();
                    if (txn > lastSegmentTxn) {
                        break;
                    }
                    if (WalTxnType.isDataType(c.getType())) {
                        endRowId = Math.max(endRowId, c.getDataInfo().getEndRowID());
                    }
                }
                return endRowId;
            }
        } finally {
            Misc.free(path);
        }
    }

    // Tier-4 schema substitution: try every other wal*/N/ segment for this
    // table, attempt to open SequencerMetadata, and return the first one that
    // works. The schema may differ from what the corrupt segment had if columns
    // were added or dropped between segments. Returns null if no peer segment
    // has a readable _meta.
    private static SequencerMetadata openMetaFromPeerSegment(CairoConfiguration config, TableToken token, int skipWalId, int skipSegmentId) {
        File tableDirFile = new File(config.getDbRoot().toString(), token.getDirName());
        File[] entries = tableDirFile.listFiles();
        if (entries == null) {
            return null;
        }
        for (File walEntry : entries) {
            if (!walEntry.isDirectory()) {
                continue;
            }
            String walDirName = walEntry.getName();
            if (!walDirName.startsWith(WalUtils.WAL_NAME_BASE) || walDirName.length() <= WalUtils.WAL_NAME_BASE.length()) {
                continue;
            }
            int walId;
            try {
                walId = Numbers.parseInt(walDirName.substring(WalUtils.WAL_NAME_BASE.length()));
            } catch (NumericException e) {
                continue;
            }
            File[] segs = walEntry.listFiles();
            if (segs == null) {
                continue;
            }
            for (File seg : segs) {
                if (!seg.isDirectory()) {
                    continue;
                }
                int segId;
                try {
                    segId = Numbers.parseInt(seg.getName());
                } catch (NumericException e) {
                    continue;
                }
                if (walId == skipWalId && segId == skipSegmentId) {
                    continue;
                }
                SequencerMetadata peerMeta = new SequencerMetadata(config, true);
                Path p = new Path();
                try {
                    p.of(seg.getAbsolutePath());
                    int len = p.size();
                    peerMeta.open(p, len, token);
                    Misc.free(p);
                    return peerMeta;
                } catch (Throwable ignored) {
                    Misc.free(peerMeta);
                    Misc.free(p);
                }
            }
        }
        return null;
    }

    // Tier-3 emission path: opens column data via direct mmap (no WalReader,
    // no _event needed). SYMBOL columns are resolved from the WAL's on-disk
    // <col>.c/.o/.k files (the base table snapshot at WAL open time). Symbol
    // codes >= symbolCount (the new-in-WAL symbols that lived only in _event)
    // are clamped to INT null. Manifest records the partial-symbol situation.
    private static void writeSegmentToParquetTier3(
            CairoConfiguration config,
            TableToken token,
            SegmentInfo info,
            long rowCount,
            String outputDir,
            ManifestSegment entry,
            boolean withShoulder
    ) {
        String walName = WalUtils.WAL_NAME_BASE + info.walId;
        File outDirFile = new File(outputDir);
        if (!outDirFile.exists() && !outDirFile.mkdirs()) {
            entry.status = "encode_failed";
            entry.reason = (entry.reason == null ? "" : entry.reason + " | ") + "could not create output dir " + outputDir;
            return;
        }
        String outFileName = buildParquetFileName(token, info, true);
        Path destPath = new Path();
        destPath.of(outputDir).concat(outFileName).$();

        SequencerMetadata meta = null;
        PartitionDescriptor descriptor = new PartitionDescriptor();
        ObjList<MemoryCMR> columnMemories = new ObjList<>();
        ObjList<SymbolMapReaderImpl> symbolReaders = new ObjList<>();
        long compactTimestampAddr = 0;
        long compactTimestampSize = 0;
        long clampedCodesAddrBatch = 0;
        long clampedCodesSizeBatch = 0;
        // Shoulder buffers (handled at end).
        long shoulderWalIdAddr = 0;
        long shoulderSegmentIdAddr = 0;
        long shoulderSegmentTxnAddr = 0;
        long shoulderSeqTxnAddr = 0;
        long shoulderCommitTsAddr = 0;
        try {
            Path segPath = new Path();
            try {
                segPath.of(config.getDbRoot()).concat(token.getDirName()).concat(walName).slash().put(info.segmentId);
                int segPathLen = segPath.size();
                meta = new SequencerMetadata(config, true);
                try {
                    meta.open(segPath, segPathLen, token);
                } catch (Throwable metaErr) {
                    // Tier-4: own _meta is unreadable. Try to find a peer
                    // segment's _meta as a schema substitute. The schema may
                    // not be identical (e.g., if columns were added or dropped
                    // between segments), so we mark the manifest accordingly.
                    Misc.free(meta);
                    meta = openMetaFromPeerSegment(config, token, info.walId, info.segmentId);
                    if (meta == null) {
                        throw metaErr;
                    }
                    entry.skippedColumns.add("(tier-4: own _meta unreadable, using a peer segment's schema; column set may not match)");
                }
            } finally {
                Misc.free(segPath);
            }

            int columnCount = meta.getColumnCount();
            int tsIndex = meta.getTimestampIndex();
            descriptor.of(token.getTableName(), rowCount, tsIndex);
            int writtenColumns = 0;
            int skippedColumns = 0;

            String segDir = config.getDbRoot()
                    + File.separator + token.getDirName()
                    + File.separator + walName
                    + File.separator + info.segmentId;
            String walDir = config.getDbRoot() + File.separator + token.getDirName() + File.separator + walName;
            io.questdb.std.str.Utf8String walDirUtf8 = new io.questdb.std.str.Utf8String(walDir);

            for (int i = 0; i < columnCount; i++) {
                int columnType = meta.getColumnType(i);
                if (columnType <= 0) {
                    skippedColumns++;
                    continue;
                }
                String columnName = meta.getColumnName(i);
                File df = new File(segDir, columnName + ".d");
                if (!df.isFile()) {
                    skippedColumns++;
                    entry.skippedColumns.add(columnName + " (missing .d)");
                    continue;
                }
                Path colPath = new Path();
                colPath.of(df.getAbsolutePath()).$();
                long dFileSize;
                if (i == tsIndex) {
                    dFileSize = df.length(); // 16B/row
                } else if (ColumnType.isVarSize(columnType)) {
                    dFileSize = df.length();
                } else {
                    int elem = 1 << ColumnType.pow2SizeOf(columnType);
                    dFileSize = Math.min(df.length(), rowCount * (long) elem);
                }
                MemoryCMR mem = Vm.getCMRInstance(config.getFilesFacade(), colPath.$(), dFileSize, io.questdb.std.MemoryTag.MMAP_TABLE_WAL_READER);
                colPath.close();
                columnMemories.add(mem);

                if (ColumnType.isSymbol(columnType)) {
                    int symbolCount = readSymbolCountFromOffsetFile(config.getDbRoot().toString(), token.getDirName(), walName, columnName);
                    if (symbolCount <= 0) {
                        skippedColumns++;
                        entry.skippedColumns.add(columnName + " (symbol: no resolvable entries in wal-level files)");
                        continue;
                    }
                    SymbolMapReaderImpl sr = new SymbolMapReaderImpl();
                    sr.of(config, walDirUtf8, columnName, COLUMN_NAME_TXN_NONE, symbolCount);
                    symbolReaders.add(sr);

                    // Clamp codes >= symbolCount to INT null (they referenced
                    // new-in-WAL symbols that only existed in _event).
                    int clampedSize = (int) (rowCount * Integer.BYTES);
                    long clampedAddr = io.questdb.std.Unsafe.malloc(clampedSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    int lostCodes = 0;
                    long src = mem.addressOf(0);
                    for (long r = 0; r < rowCount; r++) {
                        int code = io.questdb.std.Unsafe.getUnsafe().getInt(src + r * Integer.BYTES);
                        if (code >= 0 && code >= symbolCount) {
                            code = io.questdb.std.Numbers.INT_NULL;
                            lostCodes++;
                        }
                        io.questdb.std.Unsafe.getUnsafe().putInt(clampedAddr + r * Integer.BYTES, code);
                    }
                    if (lostCodes > 0) {
                        entry.skippedColumns.add(columnName + " (tier-3 partial: " + lostCodes + " rows had symbol codes beyond base snapshot; emitted as null)");
                    }
                    clampedCodesAddrBatch = clampedAddr; // tracked individually below
                    clampedCodesSizeBatch = clampedSize;

                    int encodeColumnType = columnType;
                    if (!sr.containsNullValue()) {
                        encodeColumnType |= Integer.MIN_VALUE;
                    }
                    MemoryR valuesMem = sr.getSymbolValuesColumn();
                    MemoryR offsetsMem = sr.getSymbolOffsetsColumn();
                    descriptor.addColumn(
                            columnName,
                            encodeColumnType,
                            i,
                            0,
                            clampedAddr,
                            clampedSize,
                            valuesMem.addressOf(0),
                            valuesMem.size(),
                            offsetsMem.addressOf(SymbolMapWriter.HEADER_SIZE),
                            sr.getSymbolCount(),
                            0
                    );
                } else if (ColumnType.isVarSize(columnType)) {
                    File iFile = new File(segDir, columnName + ".i");
                    if (!iFile.isFile()) {
                        skippedColumns++;
                        entry.skippedColumns.add(columnName + " (missing .i aux file)");
                        continue;
                    }
                    Path iPath = new Path();
                    iPath.of(iFile.getAbsolutePath()).$();
                    MemoryCMR aux = Vm.getCMRInstance(config.getFilesFacade(), iPath.$(), iFile.length(), io.questdb.std.MemoryTag.MMAP_TABLE_WAL_READER);
                    iPath.close();
                    columnMemories.add(aux);
                    descriptor.addColumn(
                            columnName, columnType, i, 0,
                            mem.addressOf(0), mem.size(),
                            aux.addressOf(0), aux.size(),
                            0, 0, 0);
                } else if (i == tsIndex) {
                    compactTimestampSize = rowCount * Long.BYTES;
                    compactTimestampAddr = io.questdb.std.Unsafe.malloc(compactTimestampSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    long src = mem.addressOf(0);
                    for (long row = 0; row < rowCount; row++) {
                        long ts = io.questdb.std.Unsafe.getUnsafe().getLong(src + row * 16L);
                        io.questdb.std.Unsafe.getUnsafe().putLong(compactTimestampAddr + row * Long.BYTES, ts);
                    }
                    descriptor.addColumn(
                            columnName, columnType, i, 0,
                            compactTimestampAddr, compactTimestampSize,
                            0, 0, 0, 0, 0);
                } else {
                    descriptor.addColumn(
                            columnName, columnType, i, 0,
                            mem.addressOf(0), mem.size(),
                            0, 0, 0, 0, 0);
                }
                writtenColumns++;
            }

            if (withShoulder) {
                // No per-row segmentTxn map available without _event; use the
                // segment-level lastSegmentTxn as a constant fallback.
                int intSize = Integer.BYTES;
                int longSize = Long.BYTES;
                shoulderWalIdAddr = io.questdb.std.Unsafe.malloc(rowCount * intSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderSegmentIdAddr = io.questdb.std.Unsafe.malloc(rowCount * intSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderSegmentTxnAddr = io.questdb.std.Unsafe.malloc(rowCount * intSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderSeqTxnAddr = io.questdb.std.Unsafe.malloc(rowCount * longSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderCommitTsAddr = io.questdb.std.Unsafe.malloc(rowCount * longSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                int segTxnFallback = info.lastSegmentTxn;
                long seqTxnFallback = segTxnFallback >= 0 && segTxnFallback < info.seqTxns.size() ? info.seqTxns.getQuick(segTxnFallback) : -1L;
                long commitTsFallback = segTxnFallback >= 0 && segTxnFallback < info.commitTimestamps.size() ? info.commitTimestamps.getQuick(segTxnFallback) : -1L;
                for (long row = 0; row < rowCount; row++) {
                    io.questdb.std.Unsafe.getUnsafe().putInt(shoulderWalIdAddr + row * intSize, info.walId);
                    io.questdb.std.Unsafe.getUnsafe().putInt(shoulderSegmentIdAddr + row * intSize, info.segmentId);
                    io.questdb.std.Unsafe.getUnsafe().putInt(shoulderSegmentTxnAddr + row * intSize, segTxnFallback);
                    io.questdb.std.Unsafe.getUnsafe().putLong(shoulderSeqTxnAddr + row * longSize, seqTxnFallback);
                    io.questdb.std.Unsafe.getUnsafe().putLong(shoulderCommitTsAddr + row * longSize, commitTsFallback);
                }
                int nextColumnId = columnCount;
                addShoulderColumn(descriptor, "_wal_id", ColumnType.INT, nextColumnId++, shoulderWalIdAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_segment_id", ColumnType.INT, nextColumnId++, shoulderSegmentIdAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_segment_txn", ColumnType.INT, nextColumnId++, shoulderSegmentTxnAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_seq_txn", ColumnType.LONG, nextColumnId++, shoulderSeqTxnAddr, rowCount * longSize);
                addShoulderColumn(descriptor, "_commit_ts", ColumnType.TIMESTAMP_MICRO, nextColumnId, shoulderCommitTsAddr, rowCount * longSize);
                writtenColumns += 5;
            }

            PartitionEncoder.encode(descriptor, destPath);
            entry.status = "written_tier3_partial_symbols";
            entry.outputFile = outFileName;
            entry.columnsWritten = writtenColumns;
            entry.columnsSkipped = skippedColumns;
            entry.rowsWritten = rowCount;
            System.out.println("    tier-3 wrote " + outFileName + " (" + writtenColumns + " columns, " + skippedColumns + " skipped, symbols partial)");
        } catch (Throwable e) {
            entry.status = "encode_failed_tier3";
            entry.reason = (entry.reason == null ? "" : entry.reason + " | ") + "tier3: " + e.getMessage();
            System.out.println("    tier-3 encode failed: " + e.getMessage());
        } finally {
            for (int i = 0; i < symbolReaders.size(); i++) {
                Misc.free(symbolReaders.getQuick(i));
            }
            for (int i = 0; i < columnMemories.size(); i++) {
                Misc.free(columnMemories.getQuick(i));
            }
            if (clampedCodesAddrBatch != 0) {
                io.questdb.std.Unsafe.free(clampedCodesAddrBatch, clampedCodesSizeBatch, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
            if (compactTimestampAddr != 0) {
                io.questdb.std.Unsafe.free(compactTimestampAddr, compactTimestampSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
            freeIfAllocated(shoulderWalIdAddr, rowCount * Integer.BYTES);
            freeIfAllocated(shoulderSegmentIdAddr, rowCount * Integer.BYTES);
            freeIfAllocated(shoulderSegmentTxnAddr, rowCount * Integer.BYTES);
            freeIfAllocated(shoulderSeqTxnAddr, rowCount * Long.BYTES);
            freeIfAllocated(shoulderCommitTsAddr, rowCount * Long.BYTES);
            Misc.free(meta);
            Misc.free(descriptor);
            Misc.free(destPath);
        }
    }

    // Pre-check column files for the segment. Adds an entry per missing file to
    // entry.skippedColumns so the operator can see exactly what was lost even if
    // WalReader subsequently fails to construct. Silently no-ops if _meta can't
    // be opened (tier-4 territory, handled elsewhere).
    private static void recordMissingColumnFiles(CairoConfiguration config, TableToken token, int walId, int segmentId, ManifestSegment entry) {
        Path path = new Path();
        SequencerMetadata meta = null;
        try {
            path.of(config.getDbRoot()).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
            int rootLen = path.size();
            meta = new SequencerMetadata(config, true);
            meta.open(path, rootLen, token);
            String segDirPath = config.getDbRoot()
                    + File.separator + token.getDirName()
                    + File.separator + WalUtils.WAL_NAME_BASE + walId
                    + File.separator + segmentId;
            for (int i = 0; i < meta.getColumnCount(); i++) {
                int type = meta.getColumnType(i);
                if (type <= 0) {
                    continue;
                }
                String name = meta.getColumnName(i);
                File df = new File(segDirPath, name + ".d");
                if (!df.isFile()) {
                    entry.skippedColumns.add(name + " (missing .d file)");
                }
                if (ColumnType.isVarSize(type)) {
                    File iFile = new File(segDirPath, name + ".i");
                    if (!iFile.isFile()) {
                        entry.skippedColumns.add(name + " (missing .i aux file)");
                    }
                }
            }
        } catch (Throwable ignored) {
            // _meta unreadable - tier 4 problem, leave skippedColumns empty here.
        } finally {
            Misc.free(meta);
            Misc.free(path);
        }
    }

    // Tier-3 fallback: when _event is unreadable, derive a rowCount from the
    // designated timestamp column's .d file size on disk. In WAL the designated
    // timestamp is stored as 16 bytes per row (ts + rowID pair), so rowCount =
    // fileSize / 16. The result may include un-committed tail rows because we
    // don't have _event's commit boundary, so callers must mark the segment
    // accordingly. Returns 0 if neither own nor peer _meta can be opened.
    private static long readSegmentRowCountFromTsFile(CairoConfiguration config, TableToken token, int walId, int segmentId) {
        SequencerMetadata meta = openSegmentMetaWithPeerFallback(config, token, walId, segmentId);
        if (meta == null) {
            return 0;
        }
        try {
            int tsIdx = meta.getTimestampIndex();
            if (tsIdx < 0) {
                return 0;
            }
            String tsName = meta.getColumnName(tsIdx);
            File tsFile = new File(config.getDbRoot()
                    + File.separator + token.getDirName()
                    + File.separator + WalUtils.WAL_NAME_BASE + walId
                    + File.separator + segmentId,
                    tsName + ".d");
            if (!tsFile.isFile()) {
                return 0;
            }
            return tsFile.length() / 16L;
        } catch (Throwable e) {
            return 0;
        } finally {
            Misc.free(meta);
        }
    }

    // Open SequencerMetadata for (walId, segmentId). If own _meta is unreadable
    // (tier-4), fall back to a peer segment's _meta. Returns null if no
    // metadata can be obtained at all.
    private static SequencerMetadata openSegmentMetaWithPeerFallback(CairoConfiguration config, TableToken token, int walId, int segmentId) {
        SequencerMetadata meta = new SequencerMetadata(config, true);
        Path path = new Path();
        try {
            path.of(config.getDbRoot()).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
            int rootLen = path.size();
            try {
                meta.open(path, rootLen, token);
                return meta;
            } catch (Throwable e) {
                Misc.free(meta);
                return openMetaFromPeerSegment(config, token, walId, segmentId);
            }
        } finally {
            Misc.free(path);
        }
    }

    // Derive symbol count from the .o file content. The offsets file layout is:
    //   bytes 0..HEADER_SIZE-1: SymbolMapWriter header
    //   then one Long per symbol giving its start offset in the .c file, plus a
    //   trailing Long for the next-write position. Beyond that the file is zero-
    //   padded to a page boundary. Scan starting at index 1 (index 0 is always 0
    //   because symbol 0 starts at .c[0]); the first zero entry marks the end.
    private static int readSymbolCountFromOffsetFile(String dbRoot, String tableDir, String walName, CharSequence columnName) {
        File f = new File(dbRoot + File.separator + tableDir + File.separator + walName, columnName + ".o");
        if (!f.isFile()) {
            return 0;
        }
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(f, "r")) {
            long len = raf.length();
            if (len <= SymbolMapWriter.HEADER_SIZE + Long.BYTES) {
                return 0;
            }
            long maxEntries = (len - SymbolMapWriter.HEADER_SIZE) / Long.BYTES;
            raf.seek(SymbolMapWriter.HEADER_SIZE);
            // entry[0] is always 0 (start offset of symbol 0).
            long first = Long.reverseBytes(raf.readLong());
            if (first != 0) {
                // Unexpected layout; bail out conservatively.
                return 0;
            }
            for (long i = 1; i < maxEntries; i++) {
                long v = Long.reverseBytes(raf.readLong());
                if (v == 0) {
                    return (int) (i - 1);
                }
            }
            return (int) (maxEntries - 1);
        } catch (java.io.IOException e) {
            return 0;
        }
    }

    private static void run(Args parsed) {
        CairoConfiguration config = new DefaultCairoConfiguration(parsed.dbRoot);

        List<TableInfo> tables;
        if (parsed.tableDir != null) {
            // Single-table mode (explicit).
            TableInfo single = TableInfo.fromDirName(parsed.tableDir, true);
            // Allow --table-name / --table-id overrides for the explicit case (e.g. for
            // dirs whose name doesn't follow the <name>~<id> convention).
            if (parsed.tableName != null) single.tableName = parsed.tableName;
            if (parsed.tableId != null) single.tableId = parsed.tableId;
            tables = new ArrayList<>();
            tables.add(single);
        } else {
            tables = discoverTables(parsed.dbRoot, parsed.includeSystem);
            if (!parsed.includeEmpty) {
                tables.removeIf(t -> !t.hasUnpurgedWal);
            }
        }

        System.out.println("dbRoot=" + parsed.dbRoot);
        System.out.println("tables to process: " + tables.size());
        for (TableInfo t : tables) {
            System.out.println("  - " + t.dirName + " (name=" + t.tableName + " id=" + t.tableId
                    + (t.hasUnpurgedWal ? " unPurgedWAL=yes" : " unPurgedWAL=no") + ")");
        }

        for (TableInfo t : tables) {
            processTable(config, t, parsed.outputDir, !parsed.noShoulder);
        }
    }

    // Test hook: exposes the filename builder without requiring callers to
    // construct an internal SegmentInfo.
    static String buildParquetFileNameForTest(String tableName, int walId, int segmentId, long firstSeqTxn, long lastSeqTxn, boolean tier3) {
        TableToken tok = new TableToken(tableName, tableName, null, 0, true, false, false);
        SegmentInfo info = new SegmentInfo(walId, segmentId);
        info.firstSeqTxn = firstSeqTxn;
        info.lastSeqTxn = lastSeqTxn;
        return buildParquetFileName(tok, info, tier3);
    }

    // Test hook: parses a directory name like RebuildIndex would, returning
    // {tableId, hasTilde} so tests can assert behaviour without reflection.
    static TableInfo tableInfoFromDirNameForTest(String dirName) {
        return TableInfo.fromDirName(dirName, true);
    }

    static final class Args {
        String dbRoot;
        boolean includeEmpty;
        boolean includeSystem;
        boolean noShoulder;
        String outputDir;
        String tableDir;
        Integer tableId;
        String tableName;

        static Args parse(String[] argv) {
            Args a = new Args();
            for (int i = 0; i < argv.length; i++) {
                String arg = argv[i];
                switch (arg) {
                    case "--db-root":
                        if (++i >= argv.length) return null;
                        a.dbRoot = argv[i];
                        break;
                    case "--output-dir":
                        if (++i >= argv.length) return null;
                        a.outputDir = argv[i];
                        break;
                    case "--table-dir":
                        if (++i >= argv.length) return null;
                        a.tableDir = argv[i];
                        break;
                    case "--table-name":
                        if (++i >= argv.length) return null;
                        a.tableName = argv[i];
                        break;
                    case "--table-id":
                        if (++i >= argv.length) return null;
                        try {
                            a.tableId = Numbers.parseInt(argv[i]);
                        } catch (NumericException e) {
                            System.err.println("invalid --table-id: " + argv[i]);
                            return null;
                        }
                        break;
                    case "--include-system":
                        a.includeSystem = true;
                        break;
                    case "--include-empty":
                        a.includeEmpty = true;
                        break;
                    case "--no-shoulder":
                        a.noShoulder = true;
                        break;
                    default:
                        System.err.println("unknown argument: " + arg);
                        return null;
                }
            }
            if (a.dbRoot == null) {
                System.err.println("missing required argument: --db-root");
                return null;
            }
            return a;
        }
    }

    private static void writeManifest(Manifest manifest, String outputDir) {
        File dir = new File(outputDir);
        if (!dir.exists() && !dir.mkdirs()) {
            System.out.println("  manifest dir create failed: " + outputDir);
            return;
        }
        File out = new File(dir, manifest.table + "__manifest.json");
        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
        try (FileWriter w = new FileWriter(out)) {
            gson.toJson(manifest, w);
            System.out.println("  wrote manifest " + out.getName());
        } catch (IOException e) {
            System.out.println("  manifest write failed: " + e.getMessage());
        }
    }

    private static final class Manifest {
        String dbRoot;
        String generatedAt;
        List<ManifestSegment> segments = new ArrayList<>();
        List<ManifestStructuralChange> structuralChanges = new ArrayList<>();
        String table;
        String tableDir;
        int tableId;
        String tool;
        ManifestTxnLog txnLog;
    }

    private static final class ManifestSegment {
        int columnsSkipped;
        int columnsWritten;
        long firstCommitTs;
        long firstSeqTxn;
        long lastCommitTs;
        int lastSegmentTxn;
        long lastSeqTxn;
        String outputFile;
        String reason;
        long rowsCommitted;
        long rowsWritten;
        int segmentId;
        List<String> skippedColumns = new ArrayList<>();
        String status;
        int txnCount;
        int walId;
    }

    private static final class ManifestStructuralChange {
        long commitTimestamp;
        long seqTxn;
    }

    private static final class ManifestTxnLog {
        String error;
        int formatVersion;
        long maxStructureVersion;
        long maxTxn;
        String status;
    }

    // No-op WalDirectoryPolicy for V2 sequencer read-only access. V2's
    // constructor requires one but only invokes it on the write path, so all
    // methods can be no-ops.
    private static final class ReadOnlyWalDirectoryPolicy implements WalDirectoryPolicy {
        @Override
        public void initDirectory(Path dirPath) {
        }

        @Override
        public boolean isInUse(Path path) {
            return false;
        }

        @Override
        public void rollbackDirectory(Path path) {
        }

        @Override
        public boolean truncateFilesOnClose() {
            return false;
        }
    }

    private static final class SegmentInfo {
        // Parallel arrays for txn records that landed in this segment, in
        // txnlog order. Indexed by segmentTxn for O(1) seqTxn / commitTs lookup
        // when emitting per-row shoulder columns.
        io.questdb.std.LongList commitTimestamps = new io.questdb.std.LongList();
        long firstCommitTs;
        long firstSeqTxn;
        long lastCommitTs;
        int lastSegmentTxn = -1;
        long lastSeqTxn;
        int segmentId;
        io.questdb.std.LongList seqTxns = new io.questdb.std.LongList();
        int txnCount;
        int walId;

        SegmentInfo(int walId, int segmentId) {
            this.walId = walId;
            this.segmentId = segmentId;
        }
    }

    static final class TableInfo {
        String dirName;
        boolean hasUnpurgedWal;
        int tableId;
        String tableName;

        static TableInfo fromDirName(String dirName, boolean hasUnpurgedWal) {
            // QuestDB's WAL table dir convention is "<tableName>~<tableId>".
            // Older dirs (system tables, pre-WAL tables) may not follow that.
            TableInfo info = new TableInfo();
            info.dirName = dirName;
            info.hasUnpurgedWal = hasUnpurgedWal;
            int tilde = dirName.lastIndexOf('~');
            if (tilde > 0 && tilde < dirName.length() - 1) {
                String tail = dirName.substring(tilde + 1);
                try {
                    info.tableId = Numbers.parseInt(tail);
                    info.tableName = dirName.substring(0, tilde);
                    return info;
                } catch (NumericException ignored) {
                }
            }
            info.tableId = 0;
            info.tableName = dirName;
            return info;
        }
    }

    private static final class TxnLogHeader {
        final int formatVersion;
        final long maxStructureVersion;
        final long maxTxn;
        final TableTransactionLogFile txnLog;

        TxnLogHeader(TableTransactionLogFile txnLog, int formatVersion, long maxTxn, long maxStructureVersion) {
            this.txnLog = txnLog;
            this.formatVersion = formatVersion;
            this.maxTxn = maxTxn;
            this.maxStructureVersion = maxStructureVersion;
        }
    }
}
