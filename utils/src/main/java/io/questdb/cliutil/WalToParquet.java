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
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.SequencerMetadata;
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
            // Hidden dirs (e.g., .snapshot, .checkpoint) are never user tables.
            // Don't filter by leading `_` - QuestDB allows user table names that
            // start with an underscore (TableUtils.isValidTableName permits it),
            // and non-table directories are weeded out by the txn_seq/_txnlog
            // presence check below.
            if (name.startsWith(".")) {
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
        tables.sort((a, b) -> a.dirName.compareTo(b.dirName));
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
            // Pull per-txn row count if the cursor format supports it (V2);
            // V1 throws UnsupportedOperationException, in which case we leave
            // the slot at -1 and tier-3 will refuse to fabricate row counts
            // from on-disk file lengths. Any other throwable (transient I/O,
            // unexpected cursor state) must surface, so the catch is narrow.
            long txnRowCount = -1L;
            try {
                txnRowCount = cursor.getTxnRowCount();
            } catch (UnsupportedOperationException ignored) {
            }
            // Park (seqTxn, commitTs, txnRowCount) at array index = segmentTxn
            // for O(1) lookup later.
            while (info.seqTxns.size() <= segmentTxn) {
                info.seqTxns.add(-1L);
                info.commitTimestamps.add(-1L);
                info.txnRowCounts.add(-1L);
            }
            info.seqTxns.set(segmentTxn, seqTxn);
            info.commitTimestamps.set(segmentTxn, commitTs);
            info.txnRowCounts.set(segmentTxn, txnRowCount);
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

    // Walk the segment's _event file collecting every non-DATA event (UPDATE,
    // TRUNCATE, view definition, mat-view invalidate). Adds an entry per event
    // to the manifest's sqlStatements list, keyed back to wal/segment/segmentTxn
    // and (via SegmentInfo) the seqTxn / commitTimestamp from the txnlog.
    //
    // Failure handling is per-event rather than per-segment so a single corrupt
    // or unrecognised event (e.g., Enterprise variants, future framings) does
    // not silently abandon the rest of the segment's events. Note that WAL
    // EventCursor.hasNext() reads the full record including dispatch on the
    // type byte, so a type byte the OSS build doesn't know about throws from
    // hasNext() itself - we never observe the type or the segmentTxn for that
    // event:
    //   - Body parse failure (type known, header valid, body unreadable):
    //     the event is recorded with full header context plus the parse error
    //     in the "error" field. Cursor position is intact; iteration continues.
    //   - hasNext()/getType()/getTxn() failure (typically unknown type byte
    //     or framing corruption): one UNKNOWN_EVENT_UNREADABLE entry is
    //     recorded with segmentTxn=-1 and we stop. Cursor state is undefined
    //     past this point.
    //   - _event file unopenable: a single UNKNOWN_EVENT_UNREADABLE entry
    //     scoped to the segment is recorded.
    private static void collectNonDataEvents(CairoConfiguration config, TableToken token, SegmentInfo info, Manifest manifest) {
        Path path = new Path();
        try {
            path.of(config.getDbRoot()).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(info.walId).slash().put(info.segmentId);
            // Use a single try-with-resources so the WalEventReader is closed
            // whether construction or er.of() throws. Going through the
            // close-on-exception path of try-with-resources requires that the
            // resource is bound before the throwing call, so we always assign
            // er = new WalEventReader(...) first, then attempt of() under
            // the resource scope.
            try (WalEventReader er = new WalEventReader(config)) {
                WalEventCursor c;
                try {
                    c = er.of(path, -1);
                } catch (Throwable e) {
                    // _event isn't openable. The data-path already reports the
                    // file-not-found / open failure in entry.reason - we just
                    // record a single placeholder so the sql_log preserves the
                    // fact that there may have been unrecoverable events here.
                    recordUnreadableEvent(manifest, info, -1, "open _event failed: " + e.getMessage());
                    return;
                }
                while (true) {
                    long txn;
                    byte type;
                    try {
                        if (!c.hasNext()) {
                            break;
                        }
                        txn = c.getTxn();
                        type = c.getType();
                    } catch (Throwable headerErr) {
                        // Cursor position is now untrusted; stop after one entry.
                        recordUnreadableEvent(manifest, info, -1, "wal event header read failed: " + headerErr.getMessage());
                        break;
                    }
                    if (txn > info.lastSegmentTxn) {
                        break;
                    }
                    if (WalTxnType.isDataType(type)) {
                        continue;
                    }
                    ManifestSqlStatement st = new ManifestSqlStatement();
                    st.walId = info.walId;
                    st.segmentId = info.segmentId;
                    st.segmentTxn = (int) txn;
                    int idx = (int) txn;
                    st.seqTxn = idx >= 0 && idx < info.seqTxns.size() ? info.seqTxns.getQuick(idx) : -1L;
                    st.commitTimestamp = idx >= 0 && idx < info.commitTimestamps.size() ? info.commitTimestamps.getQuick(idx) : -1L;
                    st.type = walTxnTypeName(type);
                    if (type == WalTxnType.SQL) {
                        try {
                            CharSequence sql = c.getSqlInfo().getSql();
                            if (sql != null) {
                                st.sql = sql.toString();
                            }
                        } catch (Throwable bodyErr) {
                            // Body unreadable but the header was valid, so the
                            // cursor stays aligned and we keep iterating. The
                            // entry preserves type + position + error context.
                            st.error = "SQL body parse failed: " + bodyErr.getMessage();
                        }
                    }
                    manifest.sqlStatements.add(st);
                }
            }
        } catch (Throwable e) {
            recordUnreadableEvent(manifest, info, -1, "unexpected error during event walk: " + e.getMessage());
        } finally {
            Misc.free(path);
        }
    }

    // Build a placeholder ManifestSqlStatement for an event we could not read
    // far enough to identify properly. Keeps walId/segmentId so operators can
    // narrow the loss to a specific segment.
    private static void recordUnreadableEvent(Manifest manifest, SegmentInfo info, int segmentTxn, String error) {
        ManifestSqlStatement st = new ManifestSqlStatement();
        st.walId = info.walId;
        st.segmentId = info.segmentId;
        st.segmentTxn = segmentTxn;
        st.seqTxn = -1L;
        st.commitTimestamp = -1L;
        st.type = "UNKNOWN_EVENT_UNREADABLE";
        st.error = error;
        manifest.sqlStatements.add(st);
    }

    // Map a WalTxnType byte to a human-readable name. Only types that the OSS
    // WalEventCursor knows how to parse can actually surface here - the cursor
    // throws inside hasNext() for an unknown type byte before we ever observe
    // it - so the default branch is defensive in case the cursor learns to
    // skip past unknowns in a future version. Today, genuinely unrecognised
    // type bytes become UNKNOWN_EVENT_UNREADABLE via the catch in
    // collectNonDataEvents.
    private static String walTxnTypeName(byte type) {
        return switch (type) {
            case WalTxnType.DATA -> "DATA";
            case WalTxnType.SQL -> "SQL";
            case WalTxnType.TRUNCATE -> "TRUNCATE";
            case WalTxnType.MAT_VIEW_DATA -> "MAT_VIEW_DATA";
            case WalTxnType.MAT_VIEW_INVALIDATE -> "MAT_VIEW_INVALIDATE";
            case WalTxnType.VIEW_DEFINITION -> "VIEW_DEFINITION";
            default -> "UNKNOWN_" + type;
        };
    }

    private static ManifestSegment processSegment(CairoConfiguration config, TableToken token, SegmentInfo info, String outputDir, boolean withShoulder, long appliedSeqTxn) {
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
            // Tier 3: _event is missing or corrupt. The only trustworthy
            // fallback row-count source is the txnlog's per-txn row count
            // (V2 only). WAL column files are mmap-preallocated, so .d/.i
            // lengths cannot be used - they would fabricate tens of
            // thousands of bogus zero rows.
            long fallback = sumTxnRowCounts(info);
            if (fallback < 0) {
                entry.status = "skipped_event_unreadable_no_row_count";
                entry.reason = e.getMessage()
                        + " (no trustworthy row count: txnlog format does not carry per-txn row counts, "
                        + "and WAL column files are preallocated so file lengths cannot be used)";
                System.out.println("  segment wal=" + info.walId + " seg=" + info.segmentId
                        + " seqTxnRange=[" + info.firstSeqTxn + "," + info.lastSeqTxn + "]"
                        + " event read failed and no fallback row count: " + e.getMessage());
                return entry;
            }
            if (fallback == 0) {
                entry.status = "skipped_event_unreadable_zero_rows";
                entry.reason = e.getMessage() + " (txnlog row count sum is 0)";
                System.out.println("  segment wal=" + info.walId + " seg=" + info.segmentId
                        + " _event unreadable, txnlog sum is 0 - nothing to recover");
                return entry;
            }
            rowCount = fallback;
            tier3Fallback = true;
            entry.reason = "tier-3 fallback: _event unreadable (" + e.getMessage() + "), rowCount=" + rowCount + " from txnlog per-txn row counts";
            System.out.println("  segment wal=" + info.walId + " seg=" + info.segmentId
                    + " _event unreadable, fell back to txnlog-based rowCount=" + rowCount);
        }
        entry.rowsCommitted = rowCount;

        // Record the structureVersion so __schemas.json can be cross-referenced
        // even on the happy path. The expensive per-column file existence scan
        // is deferred to the WalReader-failure branch below, because the
        // happy path will succeed (and reveal column-level problems via
        // WalReader's own error message) in the overwhelming common case.
        recordSegmentStructureVersion(config, token, info.walId, info.segmentId, entry);

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
                writeSegmentToParquet(config, token, reader, info, rowCount, outputDir, entry, withShoulder, appliedSeqTxn);
                if (tier3Fallback && "written".equals(entry.status)) {
                    entry.status = "written_tier3_event_fallback";
                }
            } else {
                entry.status = "inspected_no_output";
            }
        } catch (Throwable e) {
            // WalReader failed: now that we know recovery requires the
            // failure-path detail, do the per-column file existence scan so
            // the operator can see which specific files are missing.
            recordMissingColumnFiles(config, token, info.walId, info.segmentId, entry);
            if (tier3Fallback && outputDir != null) {
                // WalReader requires _event to construct (builds in-memory symbol-diff
                // map). When _event is gone, fall back to direct mmap of the WAL's
                // on-disk artefacts. New-in-WAL symbols (lived only in _event) are
                // lost; everything within the base symbol-table snapshot is recovered.
                System.out.println("    WalReader unavailable, trying tier-3 direct-mmap path");
                writeSegmentToParquetTier3(config, token, info, rowCount, outputDir, entry, withShoulder, appliedSeqTxn);
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
            boolean withShoulder,
            long appliedSeqTxn
    ) {
        // Per-row buffers and shoulder columns index by `(int) row`. Refuse
        // to emit a segment larger than Integer.MAX_VALUE rows rather than
        // silently truncating row indices into wrong array slots.
        if (rowCount > Integer.MAX_VALUE) {
            entry.status = "skipped_row_count_overflow";
            entry.reason = "rowCount=" + rowCount + " exceeds Integer.MAX_VALUE";
            System.out.println("    skipping: rowCount " + rowCount + " exceeds 2^31-1");
            return;
        }
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
        io.questdb.std.LongList synthesizedSymbolOffsetsSizes = new io.questdb.std.LongList();
        io.questdb.std.LongList synthesizedSymbolDataAddrs = new io.questdb.std.LongList();
        io.questdb.std.LongList synthesizedSymbolDataSizes = new io.questdb.std.LongList();
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

            // Per-row segmentTxn is needed both by shoulder columns AND per-txn
            // SYMBOL resolution, so we always compute it (cheap relative to
            // everything else).
            int[] perRowSegmentTxn = readPerRowSegmentTxn(config, token, info.walId, info.segmentId, info.lastSegmentTxn, rowCount);

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
                    long[] addrs = synthesizeSymbolBuffersPerTxn(
                            config, token, info, i, columnName.toString(), primaryMem, rowCount, perRowSegmentTxn, entry
                    );
                    // Layout: { remappedDataAddr, remappedDataSize, valuesAddr, valuesSize, offsetsAddr, dictSize }
                    long remappedDataAddr = addrs[0];
                    long remappedDataSize = addrs[1];
                    long valuesAddr = addrs[2];
                    long valuesSize = addrs[3];
                    long offsetsAddr = addrs[4];
                    int dictSize = (int) addrs[5];
                    // Each pair-tracked allocation atomically registers
                    // (addr, size) and frees its own buffer on a
                    // resize-OOM. But across multiple sequential calls,
                    // an earlier failure leaves the still-untracked
                    // tail buffers orphaned: the throwing helper freed
                    // ITS pair, but the buffers from later calls were
                    // never reached. Track per-call success and free any
                    // orphans before letting the throw propagate.
                    boolean dataTracked = false;
                    boolean valuesTracked = false;
                    long offsetsSizeBytes = (long) dictSize * Long.BYTES;
                    try {
                        trackNativeAllocation(synthesizedSymbolDataAddrs, synthesizedSymbolDataSizes, remappedDataAddr, remappedDataSize);
                        dataTracked = true;
                        trackNativeAllocation(synthesizedSymbolValuesAddrs, synthesizedSymbolValuesSizes, valuesAddr, valuesSize);
                        valuesTracked = true;
                        trackNativeAllocation(synthesizedSymbolOffsetsAddrs, synthesizedSymbolOffsetsSizes, offsetsAddr, offsetsSizeBytes);
                    } catch (Throwable t) {
                        // The throwing trackNativeAllocation freed its own pair's
                        // buffer. Free the still-untracked tail buffers; the
                        // already-tracked ones get freed by the outer finally.
                        if (!dataTracked) {
                            // Call #1 threw. valuesAddr and offsetsAddr are untracked.
                            io.questdb.std.Unsafe.free(valuesAddr, valuesSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                            io.questdb.std.Unsafe.free(offsetsAddr, offsetsSizeBytes, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                        } else if (!valuesTracked) {
                            // Call #2 threw. offsetsAddr is untracked.
                            io.questdb.std.Unsafe.free(offsetsAddr, offsetsSizeBytes, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                        }
                        // else: call #3 threw; its own buffer was freed by trackNativeAllocation.
                        throw t;
                    }

                    descriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            colTop,
                            remappedDataAddr,
                            remappedDataSize,
                            valuesAddr,
                            valuesSize,
                            offsetsAddr,
                            dictSize,
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
                int intSize = Integer.BYTES;
                int longSize = Long.BYTES;
                shoulderWalIdAddr = io.questdb.std.Unsafe.malloc(rowCount * intSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderSegmentIdAddr = io.questdb.std.Unsafe.malloc(rowCount * intSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderSegmentTxnAddr = io.questdb.std.Unsafe.malloc(rowCount * intSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderSeqTxnAddr = io.questdb.std.Unsafe.malloc(rowCount * longSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                shoulderCommitTsAddr = io.questdb.std.Unsafe.malloc(rowCount * longSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                int[] perRowStatus = new int[(int) rowCount];
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
                    perRowStatus[(int) row] = recoveryStatusForRow(seqTxn, appliedSeqTxn);
                }
                long[] statusAddrs = makeRecoveryStatusColumn(rowCount, perRowStatus);
                // statusAddrs layout: { codesAddr, codesSize, valuesAddr, valuesSize, offsetsAddr, dictSize }
                // Three-call registration with orphan-cleanup; same pattern
                // and rationale as the SYMBOL synthesis above.
                boolean statusDataTracked = false;
                boolean statusValuesTracked = false;
                long statusOffsetsSize = (long) statusAddrs[5] * Long.BYTES;
                try {
                    trackNativeAllocation(synthesizedSymbolDataAddrs, synthesizedSymbolDataSizes, statusAddrs[0], statusAddrs[1]);
                    statusDataTracked = true;
                    trackNativeAllocation(synthesizedSymbolValuesAddrs, synthesizedSymbolValuesSizes, statusAddrs[2], statusAddrs[3]);
                    statusValuesTracked = true;
                    trackNativeAllocation(synthesizedSymbolOffsetsAddrs, synthesizedSymbolOffsetsSizes, statusAddrs[4], statusOffsetsSize);
                } catch (Throwable t) {
                    if (!statusDataTracked) {
                        io.questdb.std.Unsafe.free(statusAddrs[2], statusAddrs[3], io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                        io.questdb.std.Unsafe.free(statusAddrs[4], statusOffsetsSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    } else if (!statusValuesTracked) {
                        io.questdb.std.Unsafe.free(statusAddrs[4], statusOffsetsSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    }
                    throw t;
                }

                int nextColumnId = reader.getColumnCount();
                addShoulderColumn(descriptor, "_wal_id", ColumnType.INT, nextColumnId++, shoulderWalIdAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_segment_id", ColumnType.INT, nextColumnId++, shoulderSegmentIdAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_segment_txn", ColumnType.INT, nextColumnId++, shoulderSegmentTxnAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_txnSeq_", ColumnType.LONG, nextColumnId++, shoulderSeqTxnAddr, rowCount * longSize);
                addShoulderColumn(descriptor, "_commit_ts", ColumnType.TIMESTAMP_MICRO, nextColumnId++, shoulderCommitTsAddr, rowCount * longSize);
                descriptor.addColumn(
                        "_recovery_status_",
                        ColumnType.SYMBOL,
                        nextColumnId,
                        0,
                        statusAddrs[0],
                        statusAddrs[1],
                        statusAddrs[2],
                        statusAddrs[3],
                        statusAddrs[4],
                        (int) statusAddrs[5],
                        0
                );
                writtenColumns += 6;
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
            for (int i = 0; i < synthesizedSymbolDataAddrs.size(); i++) {
                long addr = synthesizedSymbolDataAddrs.getQuick(i);
                long size = synthesizedSymbolDataSizes.getQuick(i);
                if (addr != 0) {
                    io.questdb.std.Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                }
            }
            for (int i = 0; i < synthesizedSymbolValuesAddrs.size(); i++) {
                long addr = synthesizedSymbolValuesAddrs.getQuick(i);
                long size = synthesizedSymbolValuesSizes.getQuick(i);
                if (addr != 0) {
                    io.questdb.std.Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                }
            }
            for (int i = 0; i < synthesizedSymbolOffsetsAddrs.size(); i++) {
                long addr = synthesizedSymbolOffsetsAddrs.getQuick(i);
                long size = synthesizedSymbolOffsetsSizes.getQuick(i);
                if (addr != 0) {
                    io.questdb.std.Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
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

    // Atomically append (addr, size) to a paired addr/size LongList pair.
    // If either LongList.add throws (backing-array resize OOM), roll back
    // any partial registration and free the NATIVE_DEFAULT allocation so
    // the caller cannot leak the buffer between malloc and registration.
    private static void trackNativeAllocation(io.questdb.std.LongList addrs, io.questdb.std.LongList sizes, long addr, long size) {
        boolean addrAdded = false;
        try {
            addrs.add(addr);
            addrAdded = true;
            sizes.add(size);
        } catch (Throwable t) {
            if (addrAdded) {
                addrs.setPos(addrs.size() - 1);
            }
            io.questdb.std.Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            throw t;
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

    // Build a symbol dictionary in native memory for one SYMBOL column, with
    // correct per-txn resolution. WAL writers may reset their local symbol
    // space between commits in the same segment (codes 0,1,2 in batch 1 ->
    // BTC/ETH/XRP, then codes 0,1,2 in batch 2 -> NEW-AAA/BBB/CCC), so a
    // single dictionary indexed by raw WAL code is ambiguous. We:
    //   1. Walk _event to build a per-txn frozen snapshot: at the end of
    //      each segmentTxn T, what does code -> string look like for this
    //      column? (Each diff's entries are applied on top of the running
    //      accumulator; we snapshot after each txn.)
    //   2. For every row, resolve (rowTxn, rawCode) -> string using that
    //      txn's snapshot.
    //   3. Deduplicate strings into a global dictionary with newly assigned
    //      dense codes, and write a remapped .d buffer.
    //
    // Returns long[]{
    //     remappedDataAddr, remappedDataSize,
    //     valuesAddr, valuesSize,
    //     offsetsAddr, dictSize
    // } or null if no non-null codes were referenced.
    private static long[] synthesizeSymbolBuffersPerTxn(
            CairoConfiguration config,
            TableToken token,
            SegmentInfo info,
            int columnIndex,
            String columnName,
            MemoryCR primaryMem,
            long rowCount,
            int[] perRowSegmentTxn,
            ManifestSegment entry
    ) {
        // 1. Build per-txn frozen snapshots for this column.
        java.util.HashMap<Integer, java.util.HashMap<Integer, String>> perTxnSnapshot = new java.util.HashMap<>();
        java.util.HashMap<Integer, String> accumulator = new java.util.HashMap<>();
        boolean baseLoaded = false;

        Path path = new Path();
        try {
            path.of(config.getDbRoot()).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(info.walId).slash().put(info.segmentId);
            try (WalEventReader er = new WalEventReader(config)) {
                WalEventCursor c = er.of(path, -1);
                while (c.hasNext()) {
                    long txn = c.getTxn();
                    if (txn > info.lastSegmentTxn) {
                        break;
                    }
                    if (!WalTxnType.isDataType(c.getType())) {
                        continue;
                    }
                    WalEventCursor.DataInfo di = c.getDataInfo();
                    io.questdb.cairo.wal.SymbolMapDiff diff = di.nextSymbolMapDiff();
                    while (diff != null) {
                        if (diff.getColumnIndex() == columnIndex) {
                            int cleanCount = diff.getCleanSymbolCount();
                            if (!baseLoaded && cleanCount > 0) {
                                String loadErr = loadBaseSymbols(config, token, info.walId, columnName, cleanCount, accumulator);
                                if (loadErr != null) {
                                    // Surface the failure so the operator sees that this column's
                                    // base symbol dictionary could not be recovered. Without this,
                                    // unresolvable rows silently become NULL with no manifest signal.
                                    String msg = columnName + " (base symbol load failed: " + loadErr + ")";
                                    entry.skippedColumns.add(msg);
                                    System.out.println("    WARN " + msg);
                                }
                                baseLoaded = true;
                            }
                            io.questdb.cairo.wal.SymbolMapDiffEntry diffEntry = diff.nextEntry();
                            while (diffEntry != null) {
                                accumulator.put(diffEntry.getKey(), diffEntry.getSymbol().toString());
                                diffEntry = diff.nextEntry();
                            }
                        } else {
                            // Drain entries to advance cursor to the next diff.
                            while (diff.nextEntry() != null) {
                                // no-op
                            }
                        }
                        diff = di.nextSymbolMapDiff();
                    }
                    // Snapshot accumulator AFTER this txn's diff is applied.
                    perTxnSnapshot.put((int) txn, new java.util.HashMap<>(accumulator));
                }
            }
        } finally {
            Misc.free(path);
        }

        // 2. Resolve every row, build the global dictionary, write remapped codes.
        // The three Unsafe.malloc calls below execute serially; if a later one
        // throws (e.g., OOM), earlier buffers must be freed before rethrowing,
        // otherwise the caller cannot see them in its tracking LongLists.
        long codesAddr = primaryMem.addressOf(0);
        java.util.LinkedHashMap<String, Integer> globalDict = new java.util.LinkedHashMap<>();
        long remappedDataSize = rowCount * Integer.BYTES;
        long remappedDataAddr = io.questdb.std.Unsafe.malloc(remappedDataSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        long valuesAddr = 0;
        long valuesSize = 0;
        long offsetsAddr = 0;
        long offsetsSize = 0;
        try {
            for (long row = 0; row < rowCount; row++) {
                int oldCode = io.questdb.std.Unsafe.getUnsafe().getInt(codesAddr + row * Integer.BYTES);
                int newCode;
                if (oldCode < 0) {
                    newCode = io.questdb.std.Numbers.INT_NULL;
                } else {
                    int rowTxn = perRowSegmentTxn[(int) row];
                    java.util.HashMap<Integer, String> txnMap = perTxnSnapshot.get(rowTxn);
                    String resolved = txnMap == null ? null : txnMap.get(oldCode);
                    if (resolved == null) {
                        // Fallback: try latest accumulator. If still null, emit NULL.
                        resolved = accumulator.get(oldCode);
                    }
                    if (resolved == null) {
                        newCode = io.questdb.std.Numbers.INT_NULL;
                    } else {
                        Integer cached = globalDict.get(resolved);
                        if (cached == null) {
                            newCode = globalDict.size();
                            globalDict.put(resolved, newCode);
                        } else {
                            newCode = cached;
                        }
                    }
                }
                io.questdb.std.Unsafe.getUnsafe().putInt(remappedDataAddr + row * Integer.BYTES, newCode);
            }

            int dictSize = globalDict.size();
            if (dictSize == 0) {
                // The encoder needs at least one entry; we synthesise a placeholder.
                dictSize = 1;
                globalDict.put("", 0);
            }

            // 3. Allocate chars+offsets buffers for the global dictionary.
            String[] orderedStrings = new String[dictSize];
            for (java.util.Map.Entry<String, Integer> e : globalDict.entrySet()) {
                String s = e.getKey();
                int idx = e.getValue();
                orderedStrings[idx] = s;
                valuesSize += Integer.BYTES + 2L * s.length();
            }

            valuesAddr = io.questdb.std.Unsafe.malloc(valuesSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            offsetsSize = (long) dictSize * Long.BYTES;
            offsetsAddr = io.questdb.std.Unsafe.malloc(offsetsSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);

            long cursor = 0;
            for (int k = 0; k < dictSize; k++) {
                io.questdb.std.Unsafe.getUnsafe().putLong(offsetsAddr + (long) k * Long.BYTES, cursor);
                String s = orderedStrings[k];
                io.questdb.std.Unsafe.getUnsafe().putInt(valuesAddr + cursor, s.length());
                cursor += Integer.BYTES;
                for (int i = 0, n = s.length(); i < n; i++) {
                    char ch = s.charAt(i);
                    io.questdb.std.Unsafe.getUnsafe().putByte(valuesAddr + cursor, (byte) (ch & 0xFF));
                    io.questdb.std.Unsafe.getUnsafe().putByte(valuesAddr + cursor + 1, (byte) ((ch >> 8) & 0xFF));
                    cursor += 2;
                }
            }
            return new long[]{remappedDataAddr, remappedDataSize, valuesAddr, valuesSize, offsetsAddr, dictSize};
        } catch (Throwable t) {
            if (offsetsAddr != 0) {
                io.questdb.std.Unsafe.free(offsetsAddr, offsetsSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
            if (valuesAddr != 0) {
                io.questdb.std.Unsafe.free(valuesAddr, valuesSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
            io.questdb.std.Unsafe.free(remappedDataAddr, remappedDataSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            throw t;
        }
    }

    // Load the first `count` entries from the WAL's per-column symbol files into
    // `into`. Files may live at the WAL dir level (older layout) or at the
    // table root (newer layout). Tries both.
    // Returns null on success, or an error message describing why every
    // attempted path failed. The caller is expected to surface non-null
    // returns in the manifest so the operator does not silently see a
    // column whose symbol values were lost.
    private static String loadBaseSymbols(CairoConfiguration config, TableToken token, int walId, String columnName, int count, java.util.HashMap<Integer, String> into) {
        String walDirPath = config.getDbRoot() + File.separator + token.getDirName() + File.separator + WalUtils.WAL_NAME_BASE + walId;
        String walErr = loadBaseSymbolsFrom(config, walDirPath, columnName, count, into);
        if (walErr == null) {
            return null;
        }
        String tableDirPath = config.getDbRoot() + File.separator + token.getDirName();
        String tableErr = loadBaseSymbolsFrom(config, tableDirPath, columnName, count, into);
        if (tableErr == null) {
            return null;
        }
        return "wal=" + walErr + "; table=" + tableErr;
    }

    // Returns null on success, or an error message string on failure
    // (file missing, malformed, or unreadable). Callers must propagate
    // the message; silent failure here is a forensic-data-loss bug.
    private static String loadBaseSymbolsFrom(CairoConfiguration config, String dirPath, String columnName, int count, java.util.HashMap<Integer, String> into) {
        File offsetFile = new File(dirPath, columnName + ".o");
        if (!offsetFile.isFile()) {
            return "no .o file at " + dirPath;
        }
        SymbolMapReaderImpl reader = new SymbolMapReaderImpl();
        try {
            reader.of(config, new io.questdb.std.str.Utf8String(dirPath), columnName, COLUMN_NAME_TXN_NONE, count);
            for (int k = 0; k < count; k++) {
                CharSequence v = reader.valueOf(k);
                if (v != null) {
                    into.put(k, v.toString());
                }
            }
            return null;
        } catch (Throwable e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        } finally {
            Misc.free(reader);
        }
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
        System.out.println("                    (_wal_id, _segment_id, _segment_txn, _txnSeq_,");
        System.out.println("                     _commit_ts, _recovery_status_)");
    }

    // Read the applied seqTxn watermark from _txn at the table root.
    // _txn is double-buffered: a 64-byte base header at the start contains a
    // version field and pointers into one of two record copies (A/B). The
    // active record is selected by version parity, the same way TxReader.
    // unsafeLoadBaseOffset() does it. Returns -1 when _txn is missing or
    // unreadable so the recovery-status column collapses to "unknown".
    private static long readAppliedSeqTxn(CairoConfiguration config, String tableDir) {
        FilesFacade ff = config.getFilesFacade();
        Path path = new Path();
        try {
            path.of(config.getDbRoot()).concat(tableDir).concat(TableUtils.TXN_FILE_NAME);
            long fd = -1;
            long buf = io.questdb.std.Unsafe.malloc(Long.BYTES, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            try {
                fd = TableUtils.openRO(ff, path.$(), LogFactory.getLog(WalToParquet.class));

                // Determine active record by version parity.
                long version = ff.readNonNegativeLong(fd, TableUtils.TX_BASE_OFFSET_VERSION_64);
                if (version < 0) {
                    return -1;
                }
                boolean isA = (version & 1) == 0;
                long basePtrOffset = isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32;
                if (ff.read(fd, buf, Integer.BYTES, basePtrOffset) != Integer.BYTES) {
                    return -1;
                }
                int baseOffset = io.questdb.std.Unsafe.getUnsafe().getInt(buf);
                if (baseOffset < 0) {
                    return -1;
                }

                long seqTxn = ff.readNonNegativeLong(fd, baseOffset + TableUtils.TX_OFFSET_SEQ_TXN_64);
                if (seqTxn < 0) {
                    return -1;
                }
                // lagTxnCount can be stored negative as an "unordered lag" flag;
                // we want the count regardless of sign.
                if (ff.read(fd, buf, Integer.BYTES, baseOffset + TableUtils.TX_OFFSET_LAG_TXN_COUNT_32) != Integer.BYTES) {
                    return seqTxn;
                }
                int lagRaw = io.questdb.std.Unsafe.getUnsafe().getInt(buf);
                return seqTxn + Math.abs(lagRaw);
            } finally {
                if (fd > -1) {
                    ff.close(fd);
                }
                io.questdb.std.Unsafe.free(buf, Long.BYTES, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
        } catch (Throwable e) {
            return -1;
        } finally {
            Misc.free(path);
        }
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
        manifest.appliedSeqTxn = readAppliedSeqTxn(config, table.dirName);

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
            // The V1/V2 cursor re-reads MAX_TXN_OFFSET_64 from disk inside
            // hasNext() and remaps as new records appear, so the cursor may
            // walk past header.maxTxn (a writer extending the txnlog) or
            // arrive short of it (a purge running mid-read). The cursor's
            // actual walk is authoritative. Refresh the manifest watermark
            // unconditionally from the last seqTxn observed; only fall back
            // to header.maxTxn if no data segments were visited at all
            // (e.g., the cursor only saw structural-change records, which
            // do not contribute to any SegmentInfo).
            long observedMaxTxn = 0;
            for (SegmentInfo info : segments) {
                if (info.lastSeqTxn > observedMaxTxn) {
                    observedMaxTxn = info.lastSeqTxn;
                }
            }
            if (observedMaxTxn > 0) {
                manifest.txnLog.maxTxn = observedMaxTxn;
            }
            // else: keep header.maxTxn as a diagnostic floor. Note: if the
            // cursor only saw structural-change records (no data segments),
            // manifest.txnLog.maxTxn carries the header value which DOES
            // count structural-change txns. Operators reading this field as
            // "data through txn N exists" should also check
            // manifest.segments[].rowsWritten before drawing conclusions.
            System.out.println("  found " + segments.size() + " segment(s) referenced from _txnlog");

            for (SegmentInfo info : segments) {
                ManifestSegment entry = processSegment(config, token, info, outputDir, withShoulder, manifest.appliedSeqTxn);
                manifest.segments.add(entry);
                collectNonDataEvents(config, token, info, manifest);
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
                ManifestSegment entry = processSegment(config, token, info, outputDir, withShoulder, manifest.appliedSeqTxn);
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
            writeSqlLog(manifest, outputDir);
            writeSchemasFile(config, token, manifest, outputDir);
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
        result.sort((a, b) -> {
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

    // _recovery_status_ shoulder column dictionary. The strings in
    // RECOVERY_STATUS_STRINGS are indexed by the constants below; this
    // single source of truth prevents a reorder of either from silently
    // mislabelling every Parquet row. The recoveryStatusForRow function
    // and makeRecoveryStatusColumn both rely on this coupling.
    private static final String[] RECOVERY_STATUS_STRINGS = {"unapplied", "applied_unpurged", "unknown"};
    private static final int RECOVERY_STATUS_UNAPPLIED = 0;
    private static final int RECOVERY_STATUS_APPLIED_UNPURGED = 1;
    private static final int RECOVERY_STATUS_UNKNOWN = 2;

    // Pin the constant <-> string mapping at class load. _recovery_status_
    // is a user-facing column on every emitted Parquet, so a silent drift
    // between the constants and the dictionary would corrupt every row of
    // every recovery output. Use a real runtime check (not `assert`) so
    // the guard fires in production JVMs without `-ea`. Cost: a handful
    // of equality checks once per JVM start.
    private static final int RECOVERY_STATUS_DICT_VALIDATED = validateRecoveryStatusDict();

    private static int validateRecoveryStatusDict() {
        if (RECOVERY_STATUS_STRINGS.length != 3
                || !"unapplied".equals(RECOVERY_STATUS_STRINGS[RECOVERY_STATUS_UNAPPLIED])
                || !"applied_unpurged".equals(RECOVERY_STATUS_STRINGS[RECOVERY_STATUS_APPLIED_UNPURGED])
                || !"unknown".equals(RECOVERY_STATUS_STRINGS[RECOVERY_STATUS_UNKNOWN])) {
            throw new IllegalStateException("RECOVERY_STATUS_STRINGS dictionary does not match RECOVERY_STATUS_* constants; reorder broken");
        }
        return 0;
    }

    // Build the per-row recovery status code given each row's seqTxn (from
    // perRowSeqTxn) and the appliedSeqTxn watermark for the table. seqTxn = -1
    // (tier-2 mode where _txnlog is unreadable) or appliedSeqTxn = -1 (no _txn
    // readable) collapse to "unknown".
    private static int recoveryStatusForRow(long rowSeqTxn, long appliedSeqTxn) {
        if (appliedSeqTxn < 0 || rowSeqTxn < 0) {
            return RECOVERY_STATUS_UNKNOWN;
        }
        return rowSeqTxn > appliedSeqTxn ? RECOVERY_STATUS_UNAPPLIED : RECOVERY_STATUS_APPLIED_UNPURGED;
    }

    // Allocate the native buffers for a fixed 3-entry SYMBOL column carrying
    // the per-row recovery status. The dictionary order matches the constants
    // above: 0 = "unapplied", 1 = "applied_unpurged", 2 = "unknown".
    // Returns { codesAddr, codesSize, valuesAddr, valuesSize, offsetsAddr, dictSize=3 }.
    private static long[] makeRecoveryStatusColumn(long rowCount, int[] perRowStatus) {
        long codesSize = rowCount * Integer.BYTES;
        long codesAddr = io.questdb.std.Unsafe.malloc(codesSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        long valuesAddr = 0;
        long valuesSize = 0;
        long offsetsAddr = 0;
        long offsetsSize = 0;
        try {
            for (long r = 0; r < rowCount; r++) {
                io.questdb.std.Unsafe.getUnsafe().putInt(codesAddr + r * Integer.BYTES, perRowStatus[(int) r]);
            }
            // Use the shared RECOVERY_STATUS_STRINGS so the code constants
            // and dictionary indices cannot drift independently.
            String[] strings = RECOVERY_STATUS_STRINGS;
            for (String s : strings) {
                valuesSize += Integer.BYTES + 2L * s.length();
            }
            valuesAddr = io.questdb.std.Unsafe.malloc(valuesSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            offsetsSize = (long) strings.length * Long.BYTES;
            offsetsAddr = io.questdb.std.Unsafe.malloc(offsetsSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            long cursor = 0;
            for (int k = 0; k < strings.length; k++) {
                io.questdb.std.Unsafe.getUnsafe().putLong(offsetsAddr + (long) k * Long.BYTES, cursor);
                String s = strings[k];
                io.questdb.std.Unsafe.getUnsafe().putInt(valuesAddr + cursor, s.length());
                cursor += Integer.BYTES;
                for (int i = 0, n = s.length(); i < n; i++) {
                    char ch = s.charAt(i);
                    io.questdb.std.Unsafe.getUnsafe().putByte(valuesAddr + cursor, (byte) (ch & 0xFF));
                    io.questdb.std.Unsafe.getUnsafe().putByte(valuesAddr + cursor + 1, (byte) ((ch >> 8) & 0xFF));
                    cursor += 2;
                }
            }
            return new long[]{codesAddr, codesSize, valuesAddr, valuesSize, offsetsAddr, strings.length};
        } catch (Throwable t) {
            if (offsetsAddr != 0) {
                io.questdb.std.Unsafe.free(offsetsAddr, offsetsSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
            if (valuesAddr != 0) {
                io.questdb.std.Unsafe.free(valuesAddr, valuesSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
            io.questdb.std.Unsafe.free(codesAddr, codesSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            throw t;
        }
    }

    // Walk every segment under the table and collect the schema observed at
    // each distinct structureVersion. Operators replaying a recovery dump need
    // to know what columns existed at each version because structural changes
    // (ADD COLUMN, DROP COLUMN, RENAME) reshape the Parquet files between
    // segments. The metadata change operations themselves (the ALTER SQL that
    // produced each version) are not extractable without booting CairoEngine,
    // so we capture the resulting schemas instead - the operator can recreate
    // the table at any version directly from this file.
    private static java.util.LinkedHashMap<Long, java.util.List<ManifestSchemaColumn>> collectSchemasByStructureVersion(CairoConfiguration config, TableToken token) {
        java.util.LinkedHashMap<Long, java.util.List<ManifestSchemaColumn>> byVersion = new java.util.LinkedHashMap<>();
        File tableDirFile = new File(config.getDbRoot().toString(), token.getDirName());
        File[] entries = tableDirFile.listFiles();
        if (entries == null) {
            return byVersion;
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
                SequencerMetadata meta = openSegmentMetaWithPeerFallback(config, token, walId, segId);
                if (meta == null) {
                    continue;
                }
                try {
                    long structureVersion = meta.getMetadataVersion();
                    if (byVersion.containsKey(structureVersion)) {
                        continue; // already captured from another segment
                    }
                    java.util.List<ManifestSchemaColumn> columns = new java.util.ArrayList<>();
                    int tsIdx = meta.getTimestampIndex();
                    for (int i = 0; i < meta.getColumnCount(); i++) {
                        int type = meta.getColumnType(i);
                        if (type <= 0) {
                            continue;
                        }
                        ManifestSchemaColumn col = new ManifestSchemaColumn();
                        col.name = meta.getColumnName(i);
                        col.type = ColumnType.nameOf(type);
                        col.writerIndex = i;
                        col.isDesignatedTimestamp = i == tsIdx;
                        columns.add(col);
                    }
                    byVersion.put(structureVersion, columns);
                } finally {
                    Misc.free(meta);
                }
            }
        }
        return byVersion;
    }

    private static void writeSchemasFile(CairoConfiguration config, TableToken token, Manifest manifest, String outputDir) {
        java.util.LinkedHashMap<Long, java.util.List<ManifestSchemaColumn>> byVersion = collectSchemasByStructureVersion(config, token);
        if (byVersion.isEmpty()) {
            return;
        }
        File dir = new File(outputDir);
        if (!dir.exists() && !dir.mkdirs()) {
            System.out.println("  schemas dir create failed: " + outputDir);
            return;
        }
        File out = new File(dir, manifest.table + "__schemas.json");
        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().disableHtmlEscaping().create();
        try (FileWriter w = new FileWriter(out)) {
            SchemasFile wrapper = new SchemasFile();
            wrapper.table = manifest.table;
            wrapper.tableDir = manifest.tableDir;
            wrapper.generatedAt = manifest.generatedAt;
            wrapper.note = "Schema observed at each distinct structureVersion across all the table's WAL segments. " +
                    "Use this to recreate the table at the version a given Parquet file corresponds to before " +
                    "loading its rows. The original ALTER SQL statements that produced each transition are not " +
                    "currently extracted; the resulting schemas in this file are the authoritative source.";
            // File.listFiles() order is filesystem-dependent. Sort entries by
            // numeric structureVersion so the schemas file is deterministic
            // and reflects schema evolution order.
            wrapper.structureVersions = new java.util.LinkedHashMap<>();
            java.util.List<Long> sortedKeys = new java.util.ArrayList<>(byVersion.keySet());
            java.util.Collections.sort(sortedKeys);
            for (Long v : sortedKeys) {
                wrapper.structureVersions.put(v.toString(), byVersion.get(v));
            }
            gson.toJson(wrapper, w);
            System.out.println("  wrote " + out.getName() + " (" + byVersion.size() + " structureVersion(s))");
        } catch (IOException e) {
            System.out.println("  schemas write failed: " + e.getMessage());
        }
    }

    private static final class SchemasFile {
        String generatedAt;
        String note;
        java.util.LinkedHashMap<String, java.util.List<ManifestSchemaColumn>> structureVersions;
        String table;
        String tableDir;
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
            boolean withShoulder,
            long appliedSeqTxn
    ) {
        // Same guard as the happy path: tier-3 also indexes per-row buffers
        // by (int) row, so refuse to truncate.
        if (rowCount > Integer.MAX_VALUE) {
            entry.status = "skipped_row_count_overflow";
            entry.reason = (entry.reason == null ? "" : entry.reason + " | ") + "rowCount=" + rowCount + " exceeds Integer.MAX_VALUE";
            System.out.println("    tier-3 skipped: rowCount " + rowCount + " exceeds 2^31-1");
            return;
        }
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
        // Per-SYMBOL-column clamped .d buffers, all tracked so multi-SYMBOL
        // tier-3 segments don't leak the earlier columns' allocations.
        io.questdb.std.LongList clampedCodesAddrs = new io.questdb.std.LongList();
        io.questdb.std.LongList clampedCodesSizes = new io.questdb.std.LongList();
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
            // Record the structureVersion of whatever _meta we ended up using
            // (own or peer-fallback). recordMissingColumnFiles() may have set
            // this earlier when reading our own _meta succeeded; here we cover
            // the case where the earlier pass left it at -1 and tier-3 just
            // recovered the version via the peer.
            entry.structureVersion = meta.getMetadataVersion();

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
                long dFileSize;
                if (i == tsIndex) {
                    dFileSize = df.length(); // 16B/row
                } else if (ColumnType.isVarSize(columnType)) {
                    dFileSize = df.length();
                } else {
                    int elem = 1 << ColumnType.pow2SizeOf(columnType);
                    dFileSize = Math.min(df.length(), rowCount * (long) elem);
                }
                // Vm.getCMRInstance may throw if the live writer purged the
                // file mid-scan. Register the resulting mmap into
                // columnMemories under a paired try/catch: if the ObjList
                // backing array needs to resize and that resize itself
                // OOMs, free the mmap before propagating so it cannot leak.
                MemoryCMR mem;
                Path colPath = new Path();
                try {
                    colPath.of(df.getAbsolutePath()).$();
                    mem = Vm.getCMRInstance(config.getFilesFacade(), colPath.$(), dFileSize, io.questdb.std.MemoryTag.MMAP_TABLE_WAL_READER);
                    try {
                        columnMemories.add(mem);
                    } catch (Throwable t) {
                        Misc.free(mem);
                        throw t;
                    }
                } finally {
                    Misc.free(colPath);
                }

                if (ColumnType.isSymbol(columnType)) {
                    int symbolCount = readSymbolCountFromOffsetFile(config.getDbRoot().toString(), token.getDirName(), walName, columnName);
                    if (symbolCount <= 0) {
                        skippedColumns++;
                        entry.skippedColumns.add(columnName + " (symbol: no resolvable entries in wal-level files)");
                        continue;
                    }
                    // Register the reader BEFORE configuring it. sr.of() opens
                    // files and mmaps the symbol table; corrupt symbol files
                    // are exactly the tier-3 trigger, so sr.of() throws here
                    // are realistic. SymbolMapReaderImpl's fields are
                    // initialized at declaration to empty native containers,
                    // so Misc.free on an unconfigured instance is safe.
                    SymbolMapReaderImpl sr = new SymbolMapReaderImpl();
                    symbolReaders.add(sr);
                    sr.of(config, walDirUtf8, columnName, COLUMN_NAME_TXN_NONE, symbolCount);

                    // Clamp codes >= symbolCount to INT null (they referenced
                    // new-in-WAL symbols that only existed in _event).
                    // clampedSize must be long: rowCount can reach Integer.MAX_VALUE
                    // (the M4 guard upper bound), and rowCount * 4 overflows int
                    // for any rowCount > 2^29.
                    long clampedSize = rowCount * Integer.BYTES;
                    long clampedAddr = io.questdb.std.Unsafe.malloc(clampedSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    try {
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
                        // Register the (addr, size) pair atomically. If the
                        // first add succeeds but the second throws (LongList
                        // resize OOM), the buffer would leak through the
                        // size-mismatch hole; roll the addr list back so the
                        // pair is undone before freeing the malloc.
                        clampedCodesAddrs.add(clampedAddr);
                        try {
                            clampedCodesSizes.add(clampedSize);
                        } catch (Throwable t) {
                            clampedCodesAddrs.setPos(clampedCodesAddrs.size() - 1);
                            throw t;
                        }
                    } catch (Throwable t) {
                        io.questdb.std.Unsafe.free(clampedAddr, clampedSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                        throw t;
                    }

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
                    MemoryCMR aux;
                    Path iPath = new Path();
                    try {
                        iPath.of(iFile.getAbsolutePath()).$();
                        aux = Vm.getCMRInstance(config.getFilesFacade(), iPath.$(), iFile.length(), io.questdb.std.MemoryTag.MMAP_TABLE_WAL_READER);
                        try {
                            columnMemories.add(aux);
                        } catch (Throwable t) {
                            Misc.free(aux);
                            throw t;
                        }
                    } finally {
                        Misc.free(iPath);
                    }
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
                // Reconstruct per-row segTxn from the txnlog-supplied row counts.
                // Each row's segTxn is the cumulative-sum bucket: rows
                // [0, rc[0]) belong to txn 0, [rc[0], rc[0]+rc[1]) to txn 1, etc.
                // Slots with count <= 0 are skipped without consuming row
                // capacity. Writes happen directly into the native shoulder
                // buffers as we walk the buckets; only perRowStatusT3 stays
                // on the Java heap because makeRecoveryStatusColumn consumes
                // it row-indexed. The earlier sumTxnRowCounts() guard
                // already proved every visited slot carries a valid count;
                // if anything still slips through the remaining rows get
                // sentinel values padded after the bucket walk.
                int[] perRowStatusT3 = new int[(int) rowCount];
                long rowCursor = 0;
                for (int segTxn = 0; segTxn < info.txnRowCounts.size() && rowCursor < rowCount; segTxn++) {
                    long count = info.txnRowCounts.getQuick(segTxn);
                    if (count <= 0) {
                        continue;
                    }
                    long seqTxnForBucket = segTxn < info.seqTxns.size() ? info.seqTxns.getQuick(segTxn) : -1L;
                    long commitTsForBucket = segTxn < info.commitTimestamps.size() ? info.commitTimestamps.getQuick(segTxn) : -1L;
                    int statusForBucket = recoveryStatusForRow(seqTxnForBucket, appliedSeqTxn);
                    long bucketEnd = Math.min(rowCursor + count, rowCount);
                    for (long r = rowCursor; r < bucketEnd; r++) {
                        io.questdb.std.Unsafe.getUnsafe().putInt(shoulderWalIdAddr + r * intSize, info.walId);
                        io.questdb.std.Unsafe.getUnsafe().putInt(shoulderSegmentIdAddr + r * intSize, info.segmentId);
                        io.questdb.std.Unsafe.getUnsafe().putInt(shoulderSegmentTxnAddr + r * intSize, segTxn);
                        io.questdb.std.Unsafe.getUnsafe().putLong(shoulderSeqTxnAddr + r * longSize, seqTxnForBucket);
                        io.questdb.std.Unsafe.getUnsafe().putLong(shoulderCommitTsAddr + r * longSize, commitTsForBucket);
                        perRowStatusT3[(int) r] = statusForBucket;
                    }
                    rowCursor = bucketEnd;
                }
                // Pad any unattributed tail with sentinels (-1 / UNKNOWN).
                // If we reach this branch the txnlog-supplied counts did not
                // cover rowCount, which contradicts sumTxnRowCounts'
                // guarantee. Surface the divergence in the manifest so the
                // operator can investigate rather than silently trust the
                // sentinel rows.
                if (rowCursor < rowCount) {
                    entry.skippedColumns.add("(tier-3: txnlog row-count reconstruction covered "
                            + rowCursor + " of " + rowCount
                            + " rows; remaining tagged _recovery_status_=unknown)");
                    for (long r = rowCursor; r < rowCount; r++) {
                        io.questdb.std.Unsafe.getUnsafe().putInt(shoulderWalIdAddr + r * intSize, info.walId);
                        io.questdb.std.Unsafe.getUnsafe().putInt(shoulderSegmentIdAddr + r * intSize, info.segmentId);
                        io.questdb.std.Unsafe.getUnsafe().putInt(shoulderSegmentTxnAddr + r * intSize, -1);
                        io.questdb.std.Unsafe.getUnsafe().putLong(shoulderSeqTxnAddr + r * longSize, -1L);
                        io.questdb.std.Unsafe.getUnsafe().putLong(shoulderCommitTsAddr + r * longSize, -1L);
                        perRowStatusT3[(int) r] = RECOVERY_STATUS_UNKNOWN;
                    }
                }
                long[] statusAddrs = makeRecoveryStatusColumn(rowCount, perRowStatusT3);
                // Three-call registration with orphan cleanup. The throwing
                // trackNativeAllocation frees its own pair's buffer; we
                // must free the tail buffers that later calls never reached.
                boolean t3StatusDataTracked = false;
                boolean t3StatusValuesTracked = false;
                long t3StatusOffsetsSize = (long) statusAddrs[5] * Long.BYTES;
                try {
                    trackNativeAllocation(clampedCodesAddrs, clampedCodesSizes, statusAddrs[0], statusAddrs[1]);
                    t3StatusDataTracked = true;
                    trackNativeAllocation(clampedCodesAddrs, clampedCodesSizes, statusAddrs[2], statusAddrs[3]);
                    t3StatusValuesTracked = true;
                    trackNativeAllocation(clampedCodesAddrs, clampedCodesSizes, statusAddrs[4], t3StatusOffsetsSize);
                } catch (Throwable t) {
                    if (!t3StatusDataTracked) {
                        io.questdb.std.Unsafe.free(statusAddrs[2], statusAddrs[3], io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                        io.questdb.std.Unsafe.free(statusAddrs[4], t3StatusOffsetsSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    } else if (!t3StatusValuesTracked) {
                        io.questdb.std.Unsafe.free(statusAddrs[4], t3StatusOffsetsSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    }
                    throw t;
                }

                int nextColumnId = columnCount;
                addShoulderColumn(descriptor, "_wal_id", ColumnType.INT, nextColumnId++, shoulderWalIdAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_segment_id", ColumnType.INT, nextColumnId++, shoulderSegmentIdAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_segment_txn", ColumnType.INT, nextColumnId++, shoulderSegmentTxnAddr, rowCount * intSize);
                addShoulderColumn(descriptor, "_txnSeq_", ColumnType.LONG, nextColumnId++, shoulderSeqTxnAddr, rowCount * longSize);
                addShoulderColumn(descriptor, "_commit_ts", ColumnType.TIMESTAMP_MICRO, nextColumnId++, shoulderCommitTsAddr, rowCount * longSize);
                descriptor.addColumn(
                        "_recovery_status_",
                        ColumnType.SYMBOL,
                        nextColumnId,
                        0,
                        statusAddrs[0],
                        statusAddrs[1],
                        statusAddrs[2],
                        statusAddrs[3],
                        statusAddrs[4],
                        (int) statusAddrs[5],
                        0
                );
                writtenColumns += 6;
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
            for (int i = 0; i < clampedCodesAddrs.size(); i++) {
                long addr = clampedCodesAddrs.getQuick(i);
                long size = clampedCodesSizes.getQuick(i);
                if (addr != 0) {
                    io.questdb.std.Unsafe.free(addr, size, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
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
            Misc.free(meta);
            Misc.free(descriptor);
            Misc.free(destPath);
        }
    }

    // Cheap version-only read: open _meta, record structureVersion, close.
    // Called unconditionally on every segment because the manifest needs the
    // structureVersion to map the resulting Parquet back to a schema in
    // __schemas.json. Silent on _meta failure; tier-4 paths recover the
    // version via peer-segment fallback elsewhere.
    private static void recordSegmentStructureVersion(CairoConfiguration config, TableToken token, int walId, int segmentId, ManifestSegment entry) {
        Path path = new Path();
        SequencerMetadata meta = null;
        try {
            path.of(config.getDbRoot()).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
            int rootLen = path.size();
            meta = new SequencerMetadata(config, true);
            meta.open(path, rootLen, token);
            entry.structureVersion = meta.getMetadataVersion();
        } catch (Throwable ignored) {
            // _meta unreadable - tier 4 problem, leave structureVersion at -1.
        } finally {
            Misc.free(meta);
            Misc.free(path);
        }
    }

    // Full column-file scan: only called when WalReader has already failed,
    // so the cost (one stat() per column for .d, plus another for .i on
    // var-size columns) is paid only when the operator actually needs to
    // know which specific columns are missing. The happy path (which is the
    // overwhelming common case in healthy WALs) avoids the per-column stat
    // fan-out entirely.
    private static void recordMissingColumnFiles(CairoConfiguration config, TableToken token, int walId, int segmentId, ManifestSegment entry) {
        Path path = new Path();
        SequencerMetadata meta = null;
        try {
            path.of(config.getDbRoot()).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
            int rootLen = path.size();
            meta = new SequencerMetadata(config, true);
            meta.open(path, rootLen, token);
            if (entry.structureVersion < 0) {
                entry.structureVersion = meta.getMetadataVersion();
            }
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

    // Tier-3 fallback: when _event is unreadable, derive a rowCount by summing
    // the per-txn row counts the txnlog already gave us. This is the ONLY
    // trustworthy fallback source - WAL column files are mmap-preallocated,
    // so .d/.i length reports capacity, not committed appends, which would
    // fabricate tens of thousands of zero rows.
    //
    // Returns the summed row count or -1 if any segmentTxn lacks a valid row
    // count (V1 txnlog format, which doesn't carry per-txn row counts; or
    // tier-2 mode where the txnlog itself was unreadable). Callers must
    // refuse to write Parquet and mark the segment unrecoverable when this
    // returns -1.
    private static long sumTxnRowCounts(SegmentInfo info) {
        if (info.txnRowCounts.size() == 0) {
            return -1;
        }
        long total = 0;
        for (int i = 0; i < info.txnRowCounts.size(); i++) {
            long c = info.txnRowCounts.getQuick(i);
            // -1 markers are slots beyond what the txnlog walked (sparse
            // population), OR an entire format with no row counts (V1).
            // Either way, we cannot trust the sum. Distinguish "empty
            // slot in sparse list" from "format doesn't support it" by
            // looking at the matching seqTxn slot: a -1 seqTxn means
            // "no txn referenced this segmentTxn", so skip; a non-negative
            // seqTxn means "txn exists but row count is unknown", which
            // is unrecoverable.
            if (c < 0) {
                long seqTxn = i < info.seqTxns.size() ? info.seqTxns.getQuick(i) : -1L;
                if (seqTxn >= 0) {
                    return -1;
                }
                continue;
            }
            total += c;
        }
        return total;
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

    // Write a focused sidecar with every non-DATA transaction (UPDATEs and
    // other SQL statements, TRUNCATEs, view definitions, mat-view events)
    // observed in the WAL. The data of these events doesn't materialise as
    // rows in any Parquet file, so they'd otherwise be silently lost.
    private static void writeSqlLog(Manifest manifest, String outputDir) {
        if (manifest.sqlStatements.isEmpty()) {
            return;
        }
        File dir = new File(outputDir);
        if (!dir.exists() && !dir.mkdirs()) {
            System.out.println("  sql-log dir create failed: " + outputDir);
            return;
        }
        File out = new File(dir, manifest.table + "__sql_log.json");
        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().disableHtmlEscaping().create();
        try (FileWriter w = new FileWriter(out)) {
            SqlLog wrapper = new SqlLog();
            wrapper.table = manifest.table;
            wrapper.tableDir = manifest.tableDir;
            wrapper.generatedAt = manifest.generatedAt;
            wrapper.statements = manifest.sqlStatements;
            gson.toJson(wrapper, w);
            System.out.println("  wrote " + out.getName() + " (" + manifest.sqlStatements.size() + " statement(s))");
        } catch (IOException e) {
            System.out.println("  sql-log write failed: " + e.getMessage());
        }
    }

    private static final class SqlLog {
        String generatedAt;
        List<ManifestSqlStatement> statements;
        String table;
        String tableDir;
    }

    private static void writeManifest(Manifest manifest, String outputDir) {
        File dir = new File(outputDir);
        if (!dir.exists() && !dir.mkdirs()) {
            System.out.println("  manifest dir create failed: " + outputDir);
            return;
        }
        File out = new File(dir, manifest.table + "__manifest.json");
        Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().disableHtmlEscaping().create();
        try (FileWriter w = new FileWriter(out)) {
            gson.toJson(manifest, w);
            System.out.println("  wrote manifest " + out.getName());
        } catch (IOException e) {
            System.out.println("  manifest write failed: " + e.getMessage());
        }
    }

    private static final class Manifest {
        // Applied seqTxn watermark read from _txn at the table root. Rows whose
        // _txnSeq_ is <= this value were already applied to the committed
        // partitions (but may not have been purged from the WAL yet). -1 means
        // _txn was unreadable and applied status is unknown.
        long appliedSeqTxn = -1;
        String dbRoot;
        String generatedAt;
        List<ManifestSegment> segments = new ArrayList<>();
        List<ManifestSqlStatement> sqlStatements = new ArrayList<>();
        List<ManifestStructuralChange> structuralChanges = new ArrayList<>();
        String table;
        String tableDir;
        int tableId;
        String tool;
        ManifestTxnLog txnLog;
    }

    private static final class ManifestSchemaColumn {
        boolean isDesignatedTimestamp;
        String name;
        String type;
        int writerIndex;
    }

    private static final class ManifestSqlStatement {
        long commitTimestamp;
        // Populated when an event was observed but its body or header
        // couldn't be parsed (corrupt segment, unknown framing variant in
        // a newer or Enterprise build, etc). Distinguishes "we don't have
        // SQL text for this event" from "we couldn't read this event at
        // all" - the latter shows up here as a message.
        String error;
        int segmentId;
        int segmentTxn;
        long seqTxn;
        String sql;
        String type;
        int walId;
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
        // structureVersion this segment was written under. Use it to look up
        // the column list in <tableName>__schemas.json so operators can
        // recreate the table at the correct schema before loading this
        // segment's Parquet file. -1 means the segment's _meta could not be
        // opened (peer-fallback also failed).
        long structureVersion = -1;
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
        // Row counts per segmentTxn pulled from the txnlog cursor. V2 format
        // carries getTxnRowCount() so this is populated; V1 throws
        // UnsupportedOperationException, in which case entries stay at -1.
        // Used as the fallback row-count source when _event is unreadable.
        // We deliberately do NOT trust column-file lengths because WAL columns
        // are mmap-preallocated and length() reports capacity, not committed
        // appends.
        io.questdb.std.LongList txnRowCounts = new io.questdb.std.LongList();
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
