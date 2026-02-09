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

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
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
        out.println("  ls                     list tables / partitions / columns");
        out.println("  cd <name|index>        enter a table, partition, or column");
        out.println("  cd ..                  go up one level");
        out.println("  cd /                   return to root");
        out.println("  pwd                    print current path");
        out.println("  tables                 discover and list tables");
        out.println("  show [<name|index>]    show _txn state (views have no _txn)");
        out.println("  print <rowNo>          print value at row (column level only)");
        out.println("  truncate <rowCount>    shrink partition to given row count (partition level)");
        out.println("  check columns          validate column files against metadata");
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

        java.util.HashSet<String> discoveredDirs = new java.util.HashSet<>();
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

    static String formatInt(int value) {
        return value == TxnState.UNSET_INT ? "N/A" : Integer.toString(value);
    }

    static String formatLong(long value) {
        return value == TxnState.UNSET_LONG ? "N/A" : Long.toString(value);
    }

    static String formatTimestamp(long micros) {
        if (micros == TxnState.UNSET_LONG || micros == Long.MAX_VALUE) {
            return "N/A";
        }
        StringSink sink = new StringSink();
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
