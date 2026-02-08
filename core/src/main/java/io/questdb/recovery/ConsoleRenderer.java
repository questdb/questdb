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
import io.questdb.std.str.StringSink;

import java.io.PrintStream;

public class ConsoleRenderer {
    public void printColumns(MetaState metaState, PrintStream out) {
        ObjList<MetaColumnState> columns = metaState.getColumns();
        if (columns.size() == 0) {
            out.println("No columns.");
        } else {
            out.printf("%-5s %-30s %-15s %-8s%n", "idx", "column_name", "type", "indexed");
            for (int i = 0, n = columns.size(); i < n; i++) {
                MetaColumnState col = columns.getQuick(i);
                out.printf(
                        "%-5d %-30s %-15s %-8s%n",
                        i,
                        col.getName(),
                        col.getTypeName(),
                        col.isIndexed() ? "yes" : "no"
                );
            }
        }

        printIssues("meta issues", metaState.getIssues(), out);
    }

    public void printHelp(PrintStream out) {
        out.println("commands:");
        out.println("  ls                     list tables / partitions / columns");
        out.println("  cd <name|index>        enter a table or partition");
        out.println("  cd ..                  go up one level");
        out.println("  cd /                   return to root");
        out.println("  pwd                    print current path");
        out.println("  tables                 discover and list tables");
        out.println("  show [<name|index>]    show _txn state (views have no _txn)");
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
            out.printf("%-5s %-30s %-12s %-10s %-10s %-10s%n", "idx", "dir", "rows", "format", "readOnly", "status");
            for (int i = 0, n = entries.size(); i < n; i++) {
                PartitionScanEntry entry = entries.getQuick(i);
                TxnPartitionState part = entry.getTxnPartition();
                String statusStr = switch (entry.getStatus()) {
                    case MATCHED -> "";
                    case ORPHAN -> "ORPHAN";
                    case MISSING -> "MISSING";
                };
                if (part != null) {
                    out.printf(
                            "%-5d %-30s %-12d %-10s %-10s %-10s%n",
                            i,
                            entry.getDirName(),
                            part.getRowCount(),
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
        out.println("_txn file size: " + state.getFileSize());
        out.println("txn: " + state.getTxn());
        out.println("seqTxn: " + state.getSeqTxn());
        out.println("transientRowCount: " + state.getTransientRowCount());
        out.println("fixedRowCount: " + state.getFixedRowCount());
        out.println("rowCount: " + state.getRowCount());
        out.println("minTimestamp: " + state.getMinTimestamp());
        out.println("maxTimestamp: " + state.getMaxTimestamp());
        out.println("dataVersion: " + state.getDataVersion());
        out.println("structureVersion: " + state.getStructureVersion());
        out.println("partitionTableVersion: " + state.getPartitionTableVersion());
        out.println("columnVersion: " + state.getColumnVersion());
        out.println("truncateVersion: " + state.getTruncateVersion());
        out.println("lagTxnCount: " + state.getLagTxnCount());
        out.println("lagRowCount: " + state.getLagRowCount());
        out.println("lagMinTimestamp: " + state.getLagMinTimestamp());
        out.println("lagMaxTimestamp: " + state.getLagMaxTimestamp());

        out.println("symbols: " + state.getSymbols().size());
        for (int i = 0, n = state.getSymbols().size(); i < n; i++) {
            TxnSymbolState symbol = state.getSymbols().getQuick(i);
            out.printf("  [%d] count=%d transient=%d%n", symbol.getIndex(), symbol.getCount(), symbol.getTransientCount());
        }

        out.println("partitions: " + state.getPartitions().size());
        for (int i = 0, n = state.getPartitions().size(); i < n; i++) {
            TxnPartitionState partition = state.getPartitions().getQuick(i);
            out.printf(
                    "  [%d] ts=%d size=%d nameTxn=%d parquet=%s readOnly=%s parquetFileSize=%d%n",
                    partition.getIndex(),
                    partition.getTimestampLo(),
                    partition.getRowCount(),
                    partition.getNameTxn(),
                    partition.isParquetFormat(),
                    partition.isReadOnly(),
                    partition.getParquetFileSize()
            );
        }

        printIssues("table issues", table.getIssues(), out);
        printIssues("txn issues", state.getIssues(), out);
    }

    public void printTables(ObjList<DiscoveredTable> tables, RegistryState registryState, PrintStream out) {
        if (tables.size() == 0) {
            out.println("No tables discovered.");
        } else {
            out.printf("%-5s %-36s %-36s %-10s %-10s %-10s %-12s %-8s%n", "idx", "table_name", "dir_name", "type", "state", "wal", "registry", "issues");
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

    private static String getRegistryStatus(DiscoveredTable table) {
        RegistryEntry entry = table.getRegistryEntry();
        if (entry == null) {
            // tables.d only contains WAL tables; non-WAL tables are never registered
            if (table.isWalEnabledKnown() && !table.isWalEnabled()) {
                return "";
            }
            return "NOT_IN_REG";
        }
        if (!entry.getTableName().equals(table.getTableName())) {
            return "MISMATCH";
        }
        return "";
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

    private void printIssues(String title, ObjList<ReadIssue> issues, PrintStream out) {
        if (issues.size() == 0) {
            return;
        }
        out.println(title + ":");
        for (int i = 0, n = issues.size(); i < n; i++) {
            ReadIssue issue = issues.getQuick(i);
            out.println("  [" + issue.getSeverity() + "] " + issue.getCode() + " " + issue.getMessage());
        }
    }
}
