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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.WalEventCursor;
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
import io.questdb.std.str.Path;

import java.io.File;
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

    private static String describeStructuralChange(SegmentInfo info) {
        return "wal=N/A seg=N/A structuralChange seqTxnRange=[" + info.firstSeqTxn + "," + info.lastSeqTxn + "]";
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

    private static List<SegmentInfo> enumerateSegments(TransactionLogCursor cursor) {
        // (walId, segmentId) -> SegmentInfo. Linear scan is fine; segment counts are small in practice.
        List<SegmentInfo> segments = new ArrayList<>();
        List<SegmentInfo> structuralChanges = new ArrayList<>();
        while (cursor.hasNext()) {
            int walId = cursor.getWalId();
            int segmentId = cursor.getSegmentId();
            long seqTxn = cursor.getTxn();
            long commitTs = cursor.getCommitTimestamp();

            if (walId == TableTransactionLogFile.STRUCTURAL_CHANGE_WAL_ID) {
                SegmentInfo sc = new SegmentInfo(-1, -1);
                sc.firstSeqTxn = seqTxn;
                sc.lastSeqTxn = seqTxn;
                sc.firstCommitTs = commitTs;
                sc.lastCommitTs = commitTs;
                structuralChanges.add(sc);
                continue;
            }

            SegmentInfo info = findOrCreate(segments, walId, segmentId);
            if (info.txnCount == 0) {
                info.firstSeqTxn = seqTxn;
                info.firstCommitTs = commitTs;
            }
            info.lastSeqTxn = seqTxn;
            info.lastCommitTs = commitTs;
            info.lastSegmentTxn = Math.max(info.lastSegmentTxn, cursor.getSegmentTxn());
            info.txnCount++;
        }
        // For now stage 1 just prints structural changes inline; stage 2 will weave them into the manifest.
        for (SegmentInfo sc : structuralChanges) {
            System.out.println("  " + describeStructuralChange(sc));
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

    private static void processSegment(CairoConfiguration config, TableToken token, SegmentInfo info, String outputDir) {
        String walName = WalUtils.WAL_NAME_BASE + info.walId;
        long rowCount;
        try {
            rowCount = readSegmentCommittedRowCount(config, token, info.walId, info.segmentId, info.lastSegmentTxn);
        } catch (Throwable e) {
            System.out.println("  segment wal=" + info.walId + " seg=" + info.segmentId
                    + " seqTxnRange=[" + info.firstSeqTxn + "," + info.lastSeqTxn + "]"
                    + " event read failed: " + e.getMessage());
            return;
        }

        System.out.println("  segment wal=" + info.walId + " seg=" + info.segmentId
                + " rows=" + rowCount + " txns=" + info.txnCount
                + " seqTxnRange=[" + info.firstSeqTxn + "," + info.lastSeqTxn + "]"
                + " lastSegmentTxn=" + info.lastSegmentTxn
                + " commitTsRange=[" + info.firstCommitTs + "," + info.lastCommitTs + "]");

        if (rowCount == 0) {
            System.out.println("    no committed rows in this segment");
            return;
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
                writeSegmentToParquet(config, token, reader, info, rowCount, outputDir);
            }
        } catch (Throwable e) {
            System.out.println("    open failed: " + e.getMessage());
        }
    }

    private static void writeSegmentToParquet(
            CairoConfiguration config,
            TableToken token,
            WalReader reader,
            SegmentInfo info,
            long rowCount,
            String outputDir
    ) {
        String walName = WalUtils.WAL_NAME_BASE + info.walId;
        File outDirFile = new File(outputDir);
        if (!outDirFile.exists() && !outDirFile.mkdirs()) {
            System.out.println("    output dir create failed: " + outputDir);
            return;
        }
        String outFileName = token.getTableName()
                + "__wal" + info.walId
                + "__seg" + info.segmentId
                + "__seqTxn" + info.firstSeqTxn + "-" + info.lastSeqTxn
                + ".parquet";
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

            PartitionEncoder.encode(descriptor, destPath);
            System.out.println("    wrote " + outFileName + " (" + writtenColumns + " columns, " + skippedColumns + " skipped)");
        } catch (Throwable e) {
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
            Misc.free(descriptor);
            Misc.free(destPath);
        }
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
    }

    private static void processTable(CairoConfiguration config, TableInfo table, String outputDir) {
        System.out.println();
        System.out.println("=== table=" + table.tableName + " tableDir=" + table.dirName + " tableId=" + table.tableId
                + (table.hasUnpurgedWal ? "" : " (no un-purged WAL data on disk)") + " ===");
        TableToken token = new TableToken(table.tableName, table.dirName, null, table.tableId, true, false, false);

        Path seqDir = new Path();
        seqDir.of(config.getDbRoot()).concat(table.dirName).concat(WalUtils.SEQ_DIR);

        TxnLogHeader header = null;
        TransactionLogCursor cursor = null;
        try {
            header = openTxnLogStrictRO(config, seqDir);
            System.out.println("  formatVersion=" + header.formatVersion
                    + " maxTxn=" + header.maxTxn
                    + (header.formatVersion == WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1
                    ? " maxStructureVersion=" + header.maxStructureVersion : ""));

            cursor = header.txnLog.getCursor(0L, seqDir);
            List<SegmentInfo> segments = enumerateSegments(cursor);
            System.out.println("  found " + segments.size() + " segment(s) referenced from _txnlog");

            for (SegmentInfo info : segments) {
                processSegment(config, token, info, outputDir);
            }
        } catch (Throwable e) {
            System.out.println("  table processing failed: " + e.getMessage());
        } finally {
            Misc.free(cursor);
            if (header != null) {
                Misc.free(header.txnLog);
            }
            Misc.free(seqDir);
        }
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
            processTable(config, t, parsed.outputDir);
        }
    }

    private static final class Args {
        String dbRoot;
        boolean includeEmpty;
        boolean includeSystem;
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

    private static final class SegmentInfo {
        long firstCommitTs;
        long firstSeqTxn;
        long lastCommitTs;
        int lastSegmentTxn = -1;
        long lastSeqTxn;
        int segmentId;
        int txnCount;
        int walId;

        SegmentInfo(int walId, int segmentId) {
            this.walId = walId;
            this.segmentId = segmentId;
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

    private static final class TableInfo {
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
}
