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
        out.println("  show [<name|index>]    show _txn state (current table if inside one)");
        out.println("  help                   show help");
        out.println("  quit|exit              leave recovery mode");
    }

    public void printPartitions(TxnState txnState, MetaState metaState, PrintStream out) {
        ObjList<TxnPartitionState> partitions = txnState.getPartitions();
        if (partitions.size() == 0) {
            out.println("No partitions.");
        } else {
            int partitionBy = metaState != null ? metaState.getPartitionBy() : TxnState.UNSET_INT;
            int timestampType = metaState != null ? metaState.getTimestampColumnType() : ColumnType.TIMESTAMP;

            out.printf("%-5s %-24s %-12s %-10s %-10s %-10s%n", "idx", "partition", "rows", "nameTxn", "format", "readOnly");
            for (int i = 0, n = partitions.size(); i < n; i++) {
                TxnPartitionState part = partitions.getQuick(i);
                String partName = formatPartitionName(partitionBy, timestampType, part.getTimestampLo());
                out.printf(
                        "%-5d %-24s %-12d %-10d %-10s %-10s%n",
                        i,
                        partName,
                        part.getRowCount(),
                        part.getNameTxn(),
                        part.isParquetFormat() ? "parquet" : "native",
                        part.isReadOnly()
                );
            }
        }

        if (metaState != null) {
            printIssues("meta issues", metaState.getIssues(), out);
        }
        printIssues("txn issues", txnState.getIssues(), out);
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

    public void printTables(ObjList<DiscoveredTable> tables, PrintStream out) {
        if (tables.size() == 0) {
            out.println("No tables discovered.");
            return;
        }

        out.printf("%-5s %-36s %-36s %-10s %-10s %-8s%n", "idx", "table_name", "dir_name", "state", "wal", "issues");
        for (int i = 0, n = tables.size(); i < n; i++) {
            DiscoveredTable table = tables.getQuick(i);
            out.printf(
                    "%-5d %-36s %-36s %-10s %-10s %-8d%n",
                    i + 1,
                    table.getTableName(),
                    table.getDirName(),
                    table.getState(),
                    table.isWalEnabledKnown() ? table.isWalEnabled() : "unknown",
                    table.getIssues().size()
            );
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
