/*******************************************************************************
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

package io.questdb.recovery;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Os;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;

import java.io.PrintStream;

/**
 * Formats and prints recovery output to the console. Renders tables, partitions,
 * columns, transaction state, column check results, and issues with optional
 * ANSI coloring. Also provides timestamp and byte-size formatting utilities.
 */
public class ConsoleRenderer {
    private final AnsiColor color;

    public ConsoleRenderer() {
        this(AnsiColor.NONE);
    }

    public ConsoleRenderer(AnsiColor color) {
        this.color = color;
    }

    public AnsiColor getColor() {
        return color;
    }

    public void printColumnDetail(
            MetaColumnState col, int columnIndex,
            long columnTop, long columnNameTxn, long effectiveRows,
            long expectedDataSize, long actualDataSize,
            long expectedAuxSize, long actualAuxSize, boolean inPartition,
            PrintStream out
    ) {
        out.println("column: " + col.getName());
        out.println("index: " + columnIndex);
        out.println("type: " + col.getTypeName());
        out.println("indexed: " + (col.isIndexed() ? "yes" : "no"));
        if (!inPartition) {
            out.println("status: not in partition (column added later)");
            return;
        }
        out.println("columnTop: " + columnTop);
        out.println("columnNameTxn: " + columnNameTxn);
        out.println("effectiveRows: " + effectiveRows);
        out.println("expected data size: " + formatBytes(expectedDataSize));
        out.println("actual data size: " + formatBytes(actualDataSize));
        if (expectedAuxSize >= 0) {
            out.println("expected aux size: " + formatBytes(expectedAuxSize));
        }
        if (actualAuxSize >= 0) {
            out.println("actual aux size: " + formatBytes(actualAuxSize));
        }
    }

    public void printCheckResult(ColumnCheckResult result, String tableName, long rowCount, PrintStream out) {
        out.printf("checking %s, partition %s (rows=%d):%n", tableName, result.getPartitionDirName(), rowCount);
        out.printf("  %s %s %s %s %s %s %s%n",
                color.bold(String.format("%-5s", "idx")),
                color.bold(String.format("%-30s", "column")),
                color.bold(String.format("%-15s", "type")),
                color.bold(String.format("%-10s", "status")),
                color.bold(String.format("%-10s", "colTop")),
                color.bold(String.format("%-15s", "expected")),
                color.bold(String.format("%-15s", "actual")));

        ObjList<ColumnCheckEntry> entries = result.getEntries();
        for (int i = 0, n = entries.size(); i < n; i++) {
            ColumnCheckEntry entry = entries.getQuick(i);
            String colTopStr = entry.getColumnTop() >= 0 ? Long.toString(entry.getColumnTop()) : "-";
            String expectedStr = entry.getExpectedSize() >= 0 ? formatBytes(entry.getExpectedSize()) : "-";
            String actualStr = entry.getActualSize() >= 0 ? formatBytes(entry.getActualSize()) : "-";
            String statusStr = colorCheckStatus(entry.getStatus());

            out.printf("  %-5d %-30s %-15s %s %-10s %-15s %-15s%n",
                    entry.getColumnIndex(),
                    entry.getColumnName(),
                    entry.getColumnTypeName(),
                    statusStr,
                    colTopStr,
                    expectedStr,
                    actualStr
            );

            if (entry.getMessage() != null && !entry.getMessage().isEmpty()) {
                out.println("        " + color.red(entry.getMessage()));
            }
        }
        out.println();
    }

    public void printCheckSkipped(String partitionName, String reason, PrintStream out) {
        out.println(color.cyan("skipping partition " + partitionName + ": " + reason));
    }

    public void printCheckSummary(int checked, int errors, int warnings, int skipped, PrintStream out) {
        String errorsStr = errors > 0 ? color.red(errors + " errors") : errors + " errors";
        String warningsStr = warnings > 0 ? color.yellow(warnings + " warnings") : warnings + " warnings";
        out.println("check complete: " + checked + " partitions checked, "
                + errorsStr + ", " + warningsStr + ", " + skipped + " skipped");
    }

    public void printColumns(MetaState metaState, long[] columnTops, PrintStream out) {
        ObjList<MetaColumnState> columns = metaState.getColumns();
        if (columns.size() == 0) {
            out.println("No columns.");
        } else {
            out.printf("%s %s %s %s %s%n",
                    color.bold(String.format("%-5s", "idx")),
                    color.bold(String.format("%-30s", "column_name")),
                    color.bold(String.format("%-15s", "type")),
                    color.bold(String.format("%-8s", "indexed")),
                    color.bold("note"));
            for (int i = 0, n = columns.size(); i < n; i++) {
                MetaColumnState col = columns.getQuick(i);
                String note = "";
                if (col.getType() < 0) {
                    note = color.cyan("dropped");
                } else if (columnTops != null && i < columnTops.length && columnTops[i] == -1) {
                    note = color.yellow("not in partition");
                }
                out.printf(
                        "%-5d %-30s %-15s %-8s %s%n",
                        i,
                        col.getName(),
                        col.getTypeName(),
                        col.isIndexed() ? "yes" : "no",
                        note
                );
            }
        }

        printIssues("meta issues", metaState.getIssues(), out);
    }

    public void printHelp(PrintStream out) {
        out.println("commands:");
        out.println("  ls                     list tables / partitions / columns / WAL dirs");
        out.println("  cd <name|index>        enter a table, partition, column, or WAL dir");
        out.println("  cd wal                 enter WAL navigation (from table level)");
        out.println("  cd ..                  go up one level");
        out.println("  cd /                   return to root");
        out.println("  pwd                    print current path");
        out.println("  tables                 discover and list tables");
        out.println("  show [<name|index>]    show _txn state / WAL details / event detail");
        out.println("  show <N>               show WAL event detail (segment) / seqTxn detail (WAL root)");
        out.println("  show timeline          chronological view of all WAL transactions (WAL root)");
        out.println("  print <rowNo>          print value at row (column level only)");
        out.println("  truncate <rowCount>    shrink partition to given row count (partition level)");
        out.println("  check columns          validate column files against metadata");
        out.println("  wal status             show WAL/sequencer status (table level)");
        out.println("  help                   show help");
        out.println("  quit|exit              leave recovery mode");
    }

    public void printPartitionScan(
            ObjList<PartitionScanEntry> entries,
            TxnState txnState,
            MetaState metaState,
            PrintStream out
    ) {
        if (entries.size() == 0) {
            out.println("No partitions.");
        } else {
            out.printf("%s %s %s %s %s %s%n",
                    color.bold(String.format("%-5s", "idx")),
                    color.bold(String.format("%-30s", "dir")),
                    color.bold(String.format("%-12s", "rows")),
                    color.bold(String.format("%-10s", "format")),
                    color.bold(String.format("%-10s", "readOnly")),
                    color.bold(String.format("%-10s", "status")));
            for (int i = 0, n = entries.size(); i < n; i++) {
                PartitionScanEntry entry = entries.getQuick(i);
                TxnPartitionState part = entry.getTxnPartition();
                String statusStr = switch (entry.getStatus()) {
                    case MATCHED -> "";
                    case ORPHAN -> color.yellow("ORPHAN");
                    case MISSING -> color.red("MISSING");
                };
                if (part != null) {
                    out.printf(
                            "%-5d %-30s %-12d %-10s %-10s %-10s%n",
                            i,
                            entry.getDirName(),
                            entry.getRowCount(),
                            part.isParquetFormat() ? "parquet" : "native",
                            part.isReadOnly(),
                            statusStr
                    );
                } else {
                    out.printf(
                            "%-5d %-30s %-12s %-10s %-10s %-10s%n",
                            i,
                            entry.getDirName(),
                            "-",
                            "-",
                            "-",
                            statusStr
                    );
                }
            }
        }

        if (metaState != null) {
            printIssues("meta issues", metaState.getIssues(), out);
        }
        if (txnState != null) {
            printIssues("txn issues", txnState.getIssues(), out);
        }
    }

    public void printSeqTxnLog(SeqTxnLogState seqState, TxnState txnState, PrintStream out, int limit) {
        if (seqState == null) {
            out.println("no sequencer txnlog state available");
            return;
        }

        long tableSeqTxn = txnState != null ? txnState.getSeqTxn() : TxnState.UNSET_LONG;

        ObjList<SeqTxnRecord> records = seqState.getRecords();
        if (records.size() == 0) {
            out.println("no sequencer txnlog records");
            return;
        }

        out.printf("%s %s %s %s %s %s %s %s%n",
                color.bold(String.format("%-8s", "seqTxn")),
                color.bold(String.format("%-10s", "structVer")),
                color.bold(String.format("%-8s", "walId")),
                color.bold(String.format("%-6s", "seg")),
                color.bold(String.format("%-8s", "segTxn")),
                color.bold(String.format("%-26s", "commitTime")),
                color.bold(String.format("%-10s", "rows")),
                color.bold(String.format("%-12s", "status")));

        int count = Math.min(records.size(), limit);
        for (int i = 0; i < count; i++) {
            SeqTxnRecord rec = records.getQuick(i);
            String statusStr = formatSeqTxnStatus(rec, tableSeqTxn);
            String walIdStr;
            if (rec.isDdlChange()) {
                walIdStr = color.cyan("(DDL)");
            } else if (rec.isTableDrop()) {
                walIdStr = color.red("(DROP)");
            } else {
                walIdStr = String.valueOf(rec.getWalId());
            }

            String rowsStr = rec.getRowCount() != TxnState.UNSET_LONG
                    ? Long.toString(rec.getRowCount())
                    : "-";

            out.printf("%-8d %-10d %-8s %-6s %-8s %-26s %-10s %s%n",
                    rec.getTxn(),
                    rec.getStructureVersion(),
                    walIdStr,
                    rec.isDdlChange() || rec.isTableDrop() ? "-" : String.valueOf(rec.getSegmentId()),
                    rec.isDdlChange() || rec.isTableDrop() ? "-" : String.valueOf(rec.getSegmentTxn()),
                    formatTimestamp(rec.getCommitTimestamp()),
                    rowsStr,
                    statusStr
            );
        }

        if (records.size() > limit) {
            out.println("... " + (records.size() - limit) + " more records (showing first " + limit + ")");
        }

        printIssues("sequencer txnlog issues", seqState.getIssues(), out);
    }

    public void printWalDirDetail(WalDirEntry walEntry, SeqTxnLogState seqState, PrintStream out) {
        out.println("walId: " + walEntry.getWalId());
        out.println("status: " + walEntry.getStatus());
        out.println("segments: " + walEntry.getSegments().size());
        out.println("txnlog references: " + walEntry.getTxnlogReferenceCount());

        if (seqState != null) {
            int dataCount = 0;
            ObjList<SeqTxnRecord> records = seqState.getRecords();
            for (int i = 0, n = records.size(); i < n; i++) {
                if (records.getQuick(i).getWalId() == walEntry.getWalId()) {
                    dataCount++;
                }
            }
            out.println("data txns in this wal: " + dataCount);
        }
    }

    public void printWalEventDetail(
            WalEventEntry entry, SeqTxnLogState seqState, MetaState metaState,
            int walId, int segmentId, PrintStream out
    ) {
        out.println("txn: " + entry.getTxn());
        out.println("type: " + entry.getTypeName());
        out.println("offset: " + entry.getRawOffset());
        out.println("length: " + entry.getRawLength());

        if (WalTxnType.isDataType(entry.getType())) {
            out.println("startRowID: " + entry.getStartRowID());
            out.println("endRowID: " + entry.getEndRowID());
            long rows = entry.getEndRowID() - entry.getStartRowID();
            out.println("rows: " + (rows >= 0 ? rows : "N/A"));
            out.println("minTimestamp: " + formatTimestamp(entry.getMinTimestamp()));
            out.println("maxTimestamp: " + formatTimestamp(entry.getMaxTimestamp()));
            out.println("outOfOrder: " + (entry.isOutOfOrder() ? "yes" : "no"));
            if (metaState != null) {
                String partitions = formatPartitionRange(
                        metaState.getPartitionBy(), metaState.getTimestampColumnType(),
                        entry.getMinTimestamp(), entry.getMaxTimestamp()
                );
                out.println("partitions: " + partitions);
            }
        } else if (entry.getType() == WalTxnType.SQL) {
            out.println("cmdType: " + entry.getCmdType());
            out.println("sql: " + (entry.getSqlText() != null ? entry.getSqlText() : "N/A"));
        } else if (entry.getType() == WalTxnType.VIEW_DEFINITION) {
            out.println("viewSql: " + (entry.getSqlText() != null ? entry.getSqlText() : "N/A"));
        }

        long seqTxn = findSeqTxn(seqState, walId, segmentId, entry.getTxn());
        if (seqTxn >= 0) {
            out.println("seqTxn: " + seqTxn);
        }
    }

    public void printWalEvents(
            WalEventState eventState, SeqTxnLogState seqState, TxnState txnState, MetaState metaState,
            int walId, int segmentId, PrintStream out
    ) {
        if (eventState == null) {
            out.println("no event state available");
            return;
        }

        ObjList<WalEventEntry> events = eventState.getEvents();
        if (events.size() == 0) {
            out.println("No events in wal" + walId + "/" + segmentId + ".");
            printIssues("event issues", eventState.getIssues(), out);
            return;
        }

        out.printf("%s %s %s %s %s %s %s %s %s%n",
                color.bold(String.format("%-6s", "txn")),
                color.bold(String.format("%-20s", "type")),
                color.bold(String.format("%-10s", "rows")),
                color.bold(String.format("%-26s", "minTimestamp")),
                color.bold(String.format("%-26s", "maxTimestamp")),
                color.bold(String.format("%-5s", "ooo")),
                color.bold(String.format("%-20s", "partitions")),
                color.bold(String.format("%-8s", "seqTxn")),
                color.bold(String.format("%-10s", "status")));

        for (int i = 0, n = events.size(); i < n; i++) {
            WalEventEntry entry = events.getQuick(i);
            String typeStr = entry.getTypeName();
            if (entry.getType() == WalTxnType.SQL && entry.getSqlText() != null) {
                String sqlPreview = entry.getSqlText();
                if (sqlPreview.length() > 30) {
                    sqlPreview = sqlPreview.substring(0, 27) + "...";
                }
                typeStr = "SQL (" + sqlPreview + ")";
            }

            String rowsStr = "-";
            String minTsStr = "-";
            String maxTsStr = "-";
            String oooStr = "-";
            String partStr = "-";

            if (WalTxnType.isDataType(entry.getType())) {
                long rows = entry.getEndRowID() - entry.getStartRowID();
                rowsStr = rows >= 0 ? Long.toString(rows) : "?";
                minTsStr = formatTimestamp(entry.getMinTimestamp());
                maxTsStr = formatTimestamp(entry.getMaxTimestamp());
                oooStr = entry.isOutOfOrder() ? "yes" : "no";
                if (metaState != null) {
                    partStr = formatPartitionRange(
                            metaState.getPartitionBy(), metaState.getTimestampColumnType(),
                            entry.getMinTimestamp(), entry.getMaxTimestamp()
                    );
                    if (partStr.length() > 20) {
                        partStr = partStr.substring(0, 17) + "...";
                    }
                }
            }

            long seqTxn = findSeqTxn(seqState, walId, segmentId, entry.getTxn());
            String seqTxnStr = seqTxn >= 0 ? Long.toString(seqTxn) : "-";
            long tableSeqTxn = txnState != null ? txnState.getSeqTxn() : TxnState.UNSET_LONG;
            String statusStr = "";
            if (seqTxn >= 0 && seqState != null) {
                ObjList<SeqTxnRecord> records = seqState.getRecords();
                for (int j = 0, m = records.size(); j < m; j++) {
                    SeqTxnRecord rec = records.getQuick(j);
                    if (rec.getTxn() == seqTxn) {
                        statusStr = formatSeqTxnStatus(rec, tableSeqTxn);
                        break;
                    }
                }
            }

            out.printf("%-6d %-20s %-10s %-26s %-26s %-5s %-20s %-8s %s%n",
                    entry.getTxn(),
                    typeStr.length() > 20 ? typeStr.substring(0, 17) + "..." : typeStr,
                    rowsStr,
                    minTsStr,
                    maxTsStr,
                    oooStr,
                    partStr,
                    seqTxnStr,
                    statusStr
            );
        }

        printIssues("event issues", eventState.getIssues(), out);
    }

    public void printWalSegmentDetail(
            WalEventState eventState, SeqTxnLogState seqState,
            int walId, int segmentId, PrintStream out
    ) {
        out.println("segment: " + segmentId + " in wal" + walId);

        if (eventState == null) {
            out.println("no event state available");
            return;
        }

        out.println("maxTxn: " + formatInt(eventState.getMaxTxn()));
        out.println("formatVersion: " + formatInt(eventState.getFormatVersion()));
        out.println("_event size: " + formatBytes(eventState.getEventFileSize()));
        out.println("_event.i size: " + formatBytes(eventState.getEventIndexFileSize()));

        ObjList<WalEventEntry> events = eventState.getEvents();
        out.println("events: " + events.size());

        // count by type
        int dataCount = 0;
        int sqlCount = 0;
        int truncateCount = 0;
        int matViewDataCount = 0;
        int matViewInvalidateCount = 0;
        int viewDefCount = 0;
        int unknownCount = 0;

        for (int i = 0, n = events.size(); i < n; i++) {
            switch (events.getQuick(i).getType()) {
                case WalTxnType.DATA -> dataCount++;
                case WalTxnType.SQL -> sqlCount++;
                case WalTxnType.TRUNCATE -> truncateCount++;
                case WalTxnType.MAT_VIEW_DATA -> matViewDataCount++;
                case WalTxnType.MAT_VIEW_INVALIDATE -> matViewInvalidateCount++;
                case WalTxnType.VIEW_DEFINITION -> viewDefCount++;
                default -> unknownCount++;
            }
        }

        if (dataCount > 0) out.println("  DATA: " + dataCount);
        if (sqlCount > 0) out.println("  SQL: " + sqlCount);
        if (truncateCount > 0) out.println("  TRUNCATE: " + truncateCount);
        if (matViewDataCount > 0) out.println("  MAT_VIEW_DATA: " + matViewDataCount);
        if (matViewInvalidateCount > 0) out.println("  MAT_VIEW_INVALIDATE: " + matViewInvalidateCount);
        if (viewDefCount > 0) out.println("  VIEW_DEFINITION: " + viewDefCount);
        if (unknownCount > 0) out.println("  UNKNOWN: " + unknownCount);

        printIssues("event issues", eventState.getIssues(), out);
    }

    public void printShow(DiscoveredTable table, TxnState state, PrintStream out) {
        out.println("table: " + table.getTableName());
        out.println("dir: " + table.getDirName());
        out.println("state: " + table.getState());
        out.println("walEnabled: " + (table.isWalEnabledKnown() ? table.isWalEnabled() : "unknown"));
        out.println("_txn path: " + state.getTxnPath());
        out.println("_txn file size: " + formatBytes(state.getFileSize()));
        out.println("txn: " + formatLong(state.getTxn()));
        out.println("seqTxn: " + formatLong(state.getSeqTxn()));
        out.println("transientRowCount: " + formatLong(state.getTransientRowCount()));
        out.println("fixedRowCount: " + formatLong(state.getFixedRowCount()));
        out.println("rowCount: " + formatLong(state.getRowCount()));
        out.println("minTimestamp: " + formatTimestamp(state.getMinTimestamp()));
        out.println("maxTimestamp: " + formatTimestamp(state.getMaxTimestamp()));
        out.println("dataVersion: " + formatLong(state.getDataVersion()));
        out.println("structureVersion: " + formatLong(state.getStructureVersion()));
        out.println("partitionTableVersion: " + formatLong(state.getPartitionTableVersion()));
        out.println("columnVersion: " + formatLong(state.getColumnVersion()));
        out.println("truncateVersion: " + formatLong(state.getTruncateVersion()));
        out.println("lagTxnCount: " + formatInt(state.getLagTxnCount()));
        out.println("lagRowCount: " + formatInt(state.getLagRowCount()));
        out.println("lagMinTimestamp: " + formatTimestamp(state.getLagMinTimestamp()));
        out.println("lagMaxTimestamp: " + formatTimestamp(state.getLagMaxTimestamp()));

        out.println("symbols: " + state.getSymbols().size());
        for (int i = 0, n = state.getSymbols().size(); i < n; i++) {
            TxnSymbolState symbol = state.getSymbols().getQuick(i);
            out.printf("  [%d] count=%d transient=%d%n", symbol.getIndex(), symbol.getCount(), symbol.getTransientCount());
        }

        int partitionCount = state.getPartitions().size();
        out.println("partitions: " + partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            TxnPartitionState partition = state.getPartitions().getQuick(i);
            // last partition's row count is stored in transientRowCount, not in the entry
            boolean isLast = (i == partitionCount - 1);
            long rowCount = (isLast && state.getTransientRowCount() != TxnState.UNSET_LONG)
                    ? state.getTransientRowCount()
                    : partition.getRowCount();
            if (partition.isParquetFormat()) {
                out.printf(
                        "  [%d] %s  rows=%d  parquet=true  readOnly=%s  parquetFileSize=%s%n",
                        partition.getIndex(),
                        formatTimestamp(partition.getTimestampLo()),
                        rowCount,
                        partition.isReadOnly(),
                        formatBytes(partition.getParquetFileSize())
                );
            } else {
                out.printf(
                        "  [%d] %s  rows=%d  parquet=false  readOnly=%s%n",
                        partition.getIndex(),
                        formatTimestamp(partition.getTimestampLo()),
                        rowCount,
                        partition.isReadOnly()
                );
            }
        }

        printIssues("table issues", table.getIssues(), out);
        printIssues("txn issues", state.getIssues(), out);
    }

    public void printTimeline(SeqTxnLogState seqState, TxnState txnState, MetaState metaState, PrintStream out) {
        if (seqState == null) {
            out.println("no sequencer txnlog state available");
            return;
        }

        ObjList<SeqTxnRecord> records = seqState.getRecords();
        if (records.size() == 0) {
            out.println("no records");
            return;
        }

        long tableSeqTxn = txnState != null ? txnState.getSeqTxn() : TxnState.UNSET_LONG;

        out.printf("%s %s %s %s %s %s %s%n",
                color.bold(String.format("%-8s", "seqTxn")),
                color.bold(String.format("%-26s", "commitTime")),
                color.bold(String.format("%-12s", "wal/seg")),
                color.bold(String.format("%-10s", "type")),
                color.bold(String.format("%-10s", "rows")),
                color.bold(String.format("%-20s", "partitions")),
                color.bold(String.format("%-10s", "status")));

        for (int i = 0, n = records.size(); i < n; i++) {
            SeqTxnRecord rec = records.getQuick(i);

            String walSegStr;
            String typeStr;
            if (rec.isDdlChange()) {
                walSegStr = "(DDL)";
                typeStr = "DDL";
            } else if (rec.isTableDrop()) {
                walSegStr = "(DROP)";
                typeStr = "DROP";
            } else {
                walSegStr = "wal" + rec.getWalId() + "/" + rec.getSegmentId();
                typeStr = "DATA";
            }

            String rowsStr = "-";
            String partStr = "-";
            if (!rec.isDdlChange() && !rec.isTableDrop()) {
                if (rec.getRowCount() != TxnState.UNSET_LONG) {
                    rowsStr = Long.toString(rec.getRowCount());
                }
                if (metaState != null) {
                    String pr = formatPartitionRange(
                            metaState.getPartitionBy(), metaState.getTimestampColumnType(),
                            rec.getMinTimestamp(), rec.getMaxTimestamp()
                    );
                    if (!"n/a".equals(pr)) {
                        partStr = pr;
                        if (partStr.length() > 20) {
                            partStr = partStr.substring(0, 17) + "...";
                        }
                    }
                }
            }

            String statusStr = formatSeqTxnStatus(rec, tableSeqTxn);

            out.printf("%-8d %-26s %-12s %-10s %-10s %-20s %s%n",
                    rec.getTxn(),
                    formatTimestamp(rec.getCommitTimestamp()),
                    walSegStr,
                    typeStr,
                    rowsStr,
                    partStr,
                    statusStr
            );
        }
    }

    public void printWalStatus(SeqTxnLogState seqState, TxnState txnState, MetaState metaState, PrintStream out) {
        if (seqState == null) {
            out.println("no sequencer txnlog state available");
            return;
        }

        printIssues("sequencer txnlog issues", seqState.getIssues(), out);

        int version = seqState.getVersion();
        out.println("walEnabled: true");
        out.println("txnlog version: " + formatTxnlogVersion(version));
        out.println("txnlog path: " + seqState.getTxnlogPath());
        out.println("sequencer txns: " + formatLong(seqState.getMaxTxn()));
        out.println("createTimestamp: " + formatTimestamp(seqState.getCreateTimestamp()));

        long seqTxn = txnState != null ? txnState.getSeqTxn() : TxnState.UNSET_LONG;
        out.println("table seqTxn: " + formatLong(seqTxn));

        long maxTxn = seqState.getMaxTxn();
        if (maxTxn != TxnState.UNSET_LONG && seqTxn != TxnState.UNSET_LONG && maxTxn > seqTxn) {
            long pending = maxTxn - seqTxn;
            out.println(color.yellow("pending: " + pending + " transaction" + (pending != 1 ? "s" : "")));

            ObjList<SeqTxnRecord> records = seqState.getRecords();
            for (int i = 0, n = records.size(); i < n; i++) {
                SeqTxnRecord rec = records.getQuick(i);
                if (rec.getTxn() <= seqTxn) {
                    continue;
                }
                StringBuilder sb = new StringBuilder();
                sb.append("  seqTxn ").append(rec.getTxn()).append(": ");
                if (rec.isDdlChange()) {
                    sb.append(color.cyan("DDL"));
                    sb.append(" (structVer ").append(rec.getStructureVersion()).append(')');
                } else if (rec.isTableDrop()) {
                    sb.append(color.red("DROP TABLE"));
                } else {
                    sb.append("wal").append(rec.getWalId())
                            .append('/').append(rec.getSegmentId())
                            .append(" txn=").append(rec.getSegmentTxn());
                    if (rec.getRowCount() != TxnState.UNSET_LONG) {
                        sb.append(" (").append(rec.getRowCount()).append(" rows)");
                    }
                }
                out.println(sb);
            }

            // aggregate pending stats (V2 only)
            long pendingRows = 0;
            boolean hasRowData = false;
            long pendingMinTs = Long.MAX_VALUE;
            long pendingMaxTs = Long.MIN_VALUE;
            boolean hasTsData = false;
            long lastAppliedCommitTs = TxnState.UNSET_LONG;

            for (int i = 0, n = records.size(); i < n; i++) {
                SeqTxnRecord rec = records.getQuick(i);
                if (rec.getTxn() <= seqTxn) {
                    if (rec.getCommitTimestamp() != TxnState.UNSET_LONG) {
                        lastAppliedCommitTs = rec.getCommitTimestamp();
                    }
                    continue;
                }
                if (rec.isDdlChange() || rec.isTableDrop()) {
                    continue;
                }
                if (rec.getRowCount() != TxnState.UNSET_LONG) {
                    pendingRows += rec.getRowCount();
                    hasRowData = true;
                }
                if (rec.getMinTimestamp() != TxnState.UNSET_LONG && rec.getMinTimestamp() < pendingMinTs) {
                    pendingMinTs = rec.getMinTimestamp();
                    hasTsData = true;
                }
                if (rec.getMaxTimestamp() != TxnState.UNSET_LONG && rec.getMaxTimestamp() > pendingMaxTs) {
                    pendingMaxTs = rec.getMaxTimestamp();
                    hasTsData = true;
                }
            }

            if (hasRowData) {
                out.println("pending rows: " + pendingRows);
            }
            if (hasTsData && metaState != null) {
                String partitions = formatPartitionRange(
                        metaState.getPartitionBy(), metaState.getTimestampColumnType(),
                        pendingMinTs, pendingMaxTs
                );
                out.println("affected partitions: " + partitions);
            }
            if (lastAppliedCommitTs != TxnState.UNSET_LONG) {
                long elapsed = Os.currentTimeMicros() - lastAppliedCommitTs;
                if (elapsed >= 0) {
                    out.println("time since last applied: " + formatDuration(elapsed));
                }
            }
        } else if (maxTxn != TxnState.UNSET_LONG && seqTxn != TxnState.UNSET_LONG) {
            out.println("pending: 0 transactions");
        }

        out.println("records loaded: " + seqState.getRecords().size());
    }

    public void printWalDirectories(WalScanState walScan, SeqTxnLogState seqState, TxnState txnState, MetaState metaState, PrintStream out) {
        if (walScan == null) {
            out.println("no WAL scan state available");
            return;
        }

        ObjList<WalDirEntry> entries = walScan.getEntries();
        if (entries.size() == 0) {
            out.println("No WAL directories.");
            return;
        }

        out.printf("%s %s %s %s %s %s %s%n",
                color.bold(String.format("%-5s", "idx")),
                color.bold(String.format("%-12s", "wal")),
                color.bold(String.format("%-10s", "segments")),
                color.bold(String.format("%-8s", "refs")),
                color.bold(String.format("%-10s", "rows")),
                color.bold(String.format("%-20s", "partitions")),
                color.bold(String.format("%-12s", "status")));

        for (int i = 0, n = entries.size(); i < n; i++) {
            WalDirEntry entry = entries.getQuick(i);
            String statusStr = switch (entry.getStatus()) {
                case REFERENCED -> "";
                case ORPHAN -> color.cyan("unreferenced");
                case MISSING -> isWalFullyApplied(entry.getWalId(), seqState, txnState)
                        ? "purged"
                        : color.red("MISSING");
            };

            String rowsStr = "-";
            String partStr = "-";
            if (seqState != null) {
                long totalRows = 0;
                boolean hasRowData = false;
                long minTs = Long.MAX_VALUE;
                long maxTs = Long.MIN_VALUE;
                boolean hasTsData = false;

                ObjList<SeqTxnRecord> records = seqState.getRecords();
                for (int j = 0, m = records.size(); j < m; j++) {
                    SeqTxnRecord rec = records.getQuick(j);
                    if (rec.getWalId() == entry.getWalId()) {
                        if (rec.getRowCount() != TxnState.UNSET_LONG) {
                            totalRows += rec.getRowCount();
                            hasRowData = true;
                        }
                        if (rec.getMinTimestamp() != TxnState.UNSET_LONG && rec.getMinTimestamp() < minTs) {
                            minTs = rec.getMinTimestamp();
                            hasTsData = true;
                        }
                        if (rec.getMaxTimestamp() != TxnState.UNSET_LONG && rec.getMaxTimestamp() > maxTs) {
                            maxTs = rec.getMaxTimestamp();
                            hasTsData = true;
                        }
                    }
                }

                if (hasRowData) {
                    rowsStr = Long.toString(totalRows);
                }
                if (hasTsData && metaState != null) {
                    partStr = formatPartitionRange(
                            metaState.getPartitionBy(), metaState.getTimestampColumnType(),
                            minTs, maxTs
                    );
                    if (partStr.length() > 20) {
                        partStr = partStr.substring(0, 17) + "...";
                    }
                }
            }

            out.printf("%-5d %-12s %-10d %-8d %-10s %-20s %s%n",
                    i,
                    "wal" + entry.getWalId(),
                    entry.getSegments().size(),
                    entry.getTxnlogReferenceCount(),
                    rowsStr,
                    partStr,
                    statusStr
            );
        }

        printIssues("WAL scan issues", walScan.getIssues(), out);
    }

    public void printWalSegments(
            ObjList<WalSegmentEntry> segments,
            SeqTxnLogState seqState,
            TxnState txnState,
            MetaState metaState,
            int walId,
            PrintStream out
    ) {
        if (segments == null || segments.size() == 0) {
            out.println("No segments in wal" + walId + ".");
            return;
        }

        long tableSeqTxn = txnState != null ? txnState.getSeqTxn() : TxnState.UNSET_LONG;

        out.printf("%s %s %s %s %s %s %s %s%n",
                color.bold(String.format("%-5s", "idx")),
                color.bold(String.format("%-10s", "segment")),
                color.bold(String.format("%-10s", "_event")),
                color.bold(String.format("%-10s", "_event.i")),
                color.bold(String.format("%-10s", "_meta")),
                color.bold(String.format("%-10s", "rows")),
                color.bold(String.format("%-20s", "partitions")),
                color.bold(String.format("%-10s", "status")));

        for (int i = 0, n = segments.size(); i < n; i++) {
            WalSegmentEntry seg = segments.getQuick(i);

            if (!seg.isComplete()) {
                out.printf("%-5d %-10d %-10s %-10s %-10s %-10s %-20s %s%n",
                        i, seg.getSegmentId(),
                        seg.hasEventFile() ? "yes" : color.red("NO"),
                        seg.hasEventIndexFile() ? "yes" : color.red("NO"),
                        seg.hasMetaFile() ? "yes" : color.red("NO"),
                        "-", "-",
                        color.yellow("incomplete"));
                continue;
            }

            String rowsStr = "-";
            String partStr = "-";
            String statusStr = "";
            int appliedCount = 0;
            int pendingCount = 0;

            if (seqState != null) {
                long totalRows = 0;
                boolean hasRowData = false;
                long minTs = Long.MAX_VALUE;
                long maxTs = Long.MIN_VALUE;
                boolean hasTsData = false;

                ObjList<SeqTxnRecord> records = seqState.getRecords();
                for (int j = 0, m = records.size(); j < m; j++) {
                    SeqTxnRecord rec = records.getQuick(j);
                    if (rec.getWalId() == walId && rec.getSegmentId() == seg.getSegmentId()) {
                        if (rec.getRowCount() != TxnState.UNSET_LONG) {
                            totalRows += rec.getRowCount();
                            hasRowData = true;
                        }
                        if (rec.getMinTimestamp() != TxnState.UNSET_LONG && rec.getMinTimestamp() < minTs) {
                            minTs = rec.getMinTimestamp();
                            hasTsData = true;
                        }
                        if (rec.getMaxTimestamp() != TxnState.UNSET_LONG && rec.getMaxTimestamp() > maxTs) {
                            maxTs = rec.getMaxTimestamp();
                            hasTsData = true;
                        }
                        if (tableSeqTxn != TxnState.UNSET_LONG) {
                            if (rec.getTxn() <= tableSeqTxn) {
                                appliedCount++;
                            } else {
                                pendingCount++;
                            }
                        }
                    }
                }

                if (hasRowData) {
                    rowsStr = Long.toString(totalRows);
                }
                if (hasTsData && metaState != null) {
                    partStr = formatPartitionRange(metaState.getPartitionBy(), metaState.getTimestampColumnType(), minTs, maxTs);
                    if (partStr.length() > 20) {
                        partStr = partStr.substring(0, 17) + "...";
                    }
                }
            }

            if (appliedCount > 0 && pendingCount == 0) {
                statusStr = "applied";
            } else if (pendingCount > 0 && appliedCount == 0) {
                statusStr = color.yellow("PENDING");
            } else if (appliedCount > 0) {
                statusStr = color.yellow("partial (" + pendingCount + " pending)");
            } else if (seqState != null && tableSeqTxn != TxnState.UNSET_LONG) {
                statusStr = color.cyan("unreferenced");
            }

            out.printf("%-5d %-10d %-10s %-10s %-10s %-10s %-20s %s%n",
                    i,
                    seg.getSegmentId(),
                    seg.hasEventFile() ? "yes" : color.red("NO"),
                    seg.hasEventIndexFile() ? "yes" : color.red("NO"),
                    seg.hasMetaFile() ? "yes" : color.red("NO"),
                    rowsStr,
                    partStr,
                    statusStr
            );
        }
    }

    public void printTables(ObjList<DiscoveredTable> tables, RegistryState registryState, PrintStream out) {
        if (tables.size() == 0) {
            out.println("No tables discovered.");
        } else {
            out.printf("%s %s %s %s %s %s %s %s%n",
                    color.bold(String.format("%-5s", "idx")),
                    color.bold(String.format("%-36s", "table_name")),
                    color.bold(String.format("%-36s", "dir_name")),
                    color.bold(String.format("%-10s", "type")),
                    color.bold(String.format("%-10s", "state")),
                    color.bold(String.format("%-10s", "wal")),
                    color.bold(String.format("%-12s", "registry")),
                    color.bold(String.format("%-8s", "issues")));
            for (int i = 0, n = tables.size(); i < n; i++) {
                DiscoveredTable table = tables.getQuick(i);
                String registryStatus = getRegistryStatus(table);
                out.printf(
                        "%-5d %-36s %-36s %-10s %-10s %-10s %-12s %-8d%n",
                        i + 1,
                        table.getTableName(),
                        table.getDirName(),
                        table.getTableTypeName(),
                        table.getState(),
                        table.isWalEnabledKnown() ? table.isWalEnabled() : "unknown",
                        registryStatus,
                        table.getIssues().size()
                );
            }
        }

        if (registryState != null) {
            printRegistryMissing(tables, registryState, out);
            printIssues("registry issues", registryState.getIssues(), out);
        }
    }

    private void printRegistryMissing(ObjList<DiscoveredTable> tables, RegistryState registryState, PrintStream out) {
        ObjList<RegistryEntry> entries = registryState.getEntries();
        if (entries.size() == 0) {
            return;
        }

        CharSequenceHashSet discoveredDirs = new CharSequenceHashSet();
        for (int i = 0, n = tables.size(); i < n; i++) {
            discoveredDirs.add(tables.getQuick(i).getDirName());
        }

        boolean headerPrinted = false;
        for (int i = 0, n = entries.size(); i < n; i++) {
            RegistryEntry entry = entries.getQuick(i);
            if (!discoveredDirs.contains(entry.getDirName())) {
                if (!headerPrinted) {
                    out.println("registry entries with missing directories:");
                    out.printf("  %-36s %-36s %-8s %-8s%n", "table_name", "dir_name", "tableId", "type");
                    headerPrinted = true;
                }
                out.printf(
                        "  %-36s %-36s %-8d %-8s%n",
                        entry.getTableName(),
                        entry.getDirName(),
                        entry.getTableId(),
                        formatTableType(entry.getTableType())
                );
            }
        }
    }

    private String getRegistryStatus(DiscoveredTable table) {
        RegistryEntry entry = table.getRegistryEntry();
        if (entry == null) {
            // tables.d only contains WAL tables; non-WAL tables are never registered
            if (table.isWalEnabledKnown() && !table.isWalEnabled()) {
                return "";
            }
            return color.yellow("NOT_IN_REG");
        }
        if (!entry.getTableName().equals(table.getTableName())) {
            return color.red("MISMATCH");
        }
        return "";
    }

    static long findSeqTxn(SeqTxnLogState seqState, int walId, int segmentId, long segTxn) {
        if (seqState == null) {
            return -1;
        }
        ObjList<SeqTxnRecord> records = seqState.getRecords();
        for (int i = 0, n = records.size(); i < n; i++) {
            SeqTxnRecord rec = records.getQuick(i);
            if (rec.getWalId() == walId && rec.getSegmentId() == segmentId && rec.getSegmentTxn() == segTxn) {
                return rec.getTxn();
            }
        }
        return -1;
    }

    private String formatSeqTxnStatus(SeqTxnRecord rec, long tableSeqTxn) {
        if (tableSeqTxn == TxnState.UNSET_LONG) {
            return "";
        }
        if (rec.getTxn() <= tableSeqTxn) {
            return "applied";
        }
        return color.yellow("PENDING");
    }

    /**
     * Returns true if the WAL directory is absent because all its txns have been applied.
     * When tableSeqTxn or seqState is unknown, returns false (assume worst case).
     */
    private static boolean isWalFullyApplied(int walId, SeqTxnLogState seqState, TxnState txnState) {
        if (txnState == null || seqState == null) {
            return false;
        }
        long tableSeqTxn = txnState.getSeqTxn();
        if (tableSeqTxn == TxnState.UNSET_LONG) {
            return false;
        }
        ObjList<SeqTxnRecord> records = seqState.getRecords();
        for (int i = 0, n = records.size(); i < n; i++) {
            SeqTxnRecord rec = records.getQuick(i);
            if (rec.getWalId() == walId && rec.getTxn() > tableSeqTxn) {
                return false; // at least one pending txn
            }
        }
        return true;
    }

    static String formatBytes(long bytes) {
        if (bytes < 0) {
            return "N/A";
        }
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }

    public static String formatDuration(long micros) {
        if (micros < 0) {
            return "n/a";
        }
        long totalSeconds = micros / 1_000_000L;
        if (totalSeconds < 60) {
            return totalSeconds + "s";
        }
        long totalMinutes = totalSeconds / 60;
        long seconds = totalSeconds % 60;
        if (totalMinutes < 60) {
            return totalMinutes + "m " + seconds + "s";
        }
        long totalHours = totalMinutes / 60;
        long minutes = totalMinutes % 60;
        if (totalHours < 24) {
            return totalHours + "h " + minutes + "m";
        }
        long days = totalHours / 24;
        long hours = totalHours % 24;
        return days + "d " + hours + "h";
    }

    static String formatInt(int value) {
        return value == TxnState.UNSET_INT ? "N/A" : Integer.toString(value);
    }

    static String formatTxnlogVersion(int version) {
        if (version == TxnState.UNSET_INT) {
            return "N/A";
        }
        return switch (version) {
            case 0 -> "V1";
            case 1 -> "V2";
            default -> "unknown (" + version + ")";
        };
    }

    static String formatLong(long value) {
        return value == TxnState.UNSET_LONG ? "N/A" : Long.toString(value);
    }

    private static final ThreadLocal<StringSink> TIMESTAMP_SINK = ThreadLocal.withInitial(StringSink::new);

    static String formatTimestamp(long micros) {
        if (micros == TxnState.UNSET_LONG || micros == Long.MAX_VALUE) {
            return "N/A";
        }
        StringSink sink = TIMESTAMP_SINK.get();
        sink.clear();
        MicrosFormatUtils.appendDateTime(sink, micros);
        return sink.toString();
    }

    static String formatTableType(int tableType) {
        return switch (tableType) {
            case TableUtils.TABLE_TYPE_NON_WAL, TableUtils.TABLE_TYPE_WAL -> "table";
            case TableUtils.TABLE_TYPE_MAT -> "matview";
            case TableUtils.TABLE_TYPE_VIEW -> "view";
            default -> String.valueOf(tableType);
        };
    }

    static String formatPartitionDirName(String partitionName, long nameTxn) {
        return nameTxn > -1L ? partitionName + "." + nameTxn : partitionName;
    }

    public static String formatPartitionRange(int partitionBy, int timestampType, long minTimestamp, long maxTimestamp) {
        if (partitionBy == TxnState.UNSET_INT) {
            return "n/a";
        }
        if (minTimestamp == TxnState.UNSET_LONG || maxTimestamp == TxnState.UNSET_LONG
                || minTimestamp == Long.MAX_VALUE || maxTimestamp == Long.MAX_VALUE
                || minTimestamp == Long.MIN_VALUE || maxTimestamp == Long.MIN_VALUE) {
            return "n/a";
        }
        if (partitionBy == PartitionBy.NONE) {
            return "default";
        }
        try {
            TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
            TimestampDriver.TimestampFloorMethod floor = driver.getPartitionFloorMethod(partitionBy);
            TimestampDriver.PartitionAddMethod add = driver.getPartitionAddMethod(partitionBy);

            long cur = floor.floor(minTimestamp);
            long maxFloor = floor.floor(maxTimestamp);

            // count partitions first
            int count = 0;
            long tmp = cur;
            while (tmp <= maxFloor) {
                count++;
                if (count > 100) {
                    break;
                }
                tmp = add.calculate(tmp, 1);
            }

            if (count > 20) {
                String first = formatPartitionName(partitionBy, timestampType, cur);
                String last = formatPartitionName(partitionBy, timestampType, maxFloor);
                return first + " .. " + last + " (" + count + " partitions)";
            }

            StringBuilder sb = new StringBuilder();
            while (cur <= maxFloor) {
                if (!sb.isEmpty()) {
                    sb.append(", ");
                }
                sb.append(formatPartitionName(partitionBy, timestampType, cur));
                cur = add.calculate(cur, 1);
            }
            return sb.toString();
        } catch (Exception e) {
            return "n/a";
        }
    }

    static String formatPartitionName(int partitionBy, int timestampType, long timestampLo) {
        if (partitionBy == TxnState.UNSET_INT || partitionBy == PartitionBy.NONE) {
            return partitionBy == PartitionBy.NONE ? "default" : Long.toString(timestampLo);
        }
        try {
            StringSink sink = new StringSink();
            PartitionBy.setSinkForPartition(sink, timestampType, partitionBy, timestampLo);
            return sink.toString();
        } catch (Exception e) {
            return Long.toString(timestampLo);
        }
    }

    private String colorCheckStatus(ColumnCheckStatus status) {
        return switch (status) {
            case ERROR -> color.red(String.format("%-10s", status));
            case OK -> color.green(String.format("%-10s", status));
            case SKIPPED -> color.cyan(String.format("%-10s", status));
            case WARNING -> color.yellow(String.format("%-10s", status));
        };
    }

    private String colorSeverity(RecoveryIssueSeverity severity) {
        return switch (severity) {
            case ERROR -> color.red("[" + severity + "]");
            case WARN -> color.yellow("[" + severity + "]");
            case INFO -> color.cyan("[" + severity + "]");
        };
    }

    private void printIssues(String title, ObjList<ReadIssue> issues, PrintStream out) {
        if (issues.size() == 0) {
            return;
        }
        out.println(title + ":");
        for (int i = 0, n = issues.size(); i < n; i++) {
            ReadIssue issue = issues.getQuick(i);
            out.println("  " + colorSeverity(issue.getSeverity()) + " " + issue.getCode() + " " + issue.getMessage());
        }
    }
}
