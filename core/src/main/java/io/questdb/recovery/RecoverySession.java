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

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;

public class RecoverySession {
    private ColumnVersionState cachedCvState;
    private MetaState cachedMetaState;
    private ObjList<PartitionScanEntry> cachedPartitionScan;
    private RegistryState cachedRegistryState;
    private TxnState cachedTxnState;
    private final ColumnCheckService columnCheckService;
    private final AnsiColor color;
    private final ColumnVersionStateService columnVersionStateService;
    private int currentPartitionIndex = -1;
    private DiscoveredTable currentTable;
    private final CharSequence dbRoot;
    private ObjList<DiscoveredTable> lastDiscoveredTables = new ObjList<>();
    private final MetaStateService metaStateService;
    private final PartitionScanService partitionScanService;
    private final RegistryStateService registryStateService;
    private final ConsoleRenderer renderer;
    private final TableDiscoveryService tableDiscoveryService;
    private final TxnStateService txnStateService;

    public RecoverySession(
            CharSequence dbRoot,
            ColumnCheckService columnCheckService,
            ColumnVersionStateService columnVersionStateService,
            MetaStateService metaStateService,
            PartitionScanService partitionScanService,
            RegistryStateService registryStateService,
            TableDiscoveryService tableDiscoveryService,
            TxnStateService txnStateService,
            ConsoleRenderer renderer
    ) {
        this.color = renderer.getColor();
        this.dbRoot = dbRoot;
        this.columnCheckService = columnCheckService;
        this.columnVersionStateService = columnVersionStateService;
        this.metaStateService = metaStateService;
        this.partitionScanService = partitionScanService;
        this.registryStateService = registryStateService;
        this.tableDiscoveryService = tableDiscoveryService;
        this.txnStateService = txnStateService;
        this.renderer = renderer;
    }

    public int run(BufferedReader in, PrintStream out, PrintStream err) throws IOException {
        out.println("QuestDB offline recovery mode");
        out.println("dbRoot=" + dbRoot);
        renderer.printHelp(out);

        String line;
        while (true) {
            out.print(buildPrompt());
            out.flush();
            line = in.readLine();
            if (line == null) {
                return 0;
            }

            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            if ("quit".equalsIgnoreCase(line) || "exit".equalsIgnoreCase(line)) {
                return 0;
            }

            if ("help".equalsIgnoreCase(line)) {
                renderer.printHelp(out);
                continue;
            }

            try {
                if ("tables".equalsIgnoreCase(line)) {
                    lastDiscoveredTables = tableDiscoveryService.discoverTables(dbRoot);
                    cachedRegistryState = registryStateService.readRegistryState(dbRoot);
                    tableDiscoveryService.crossReferenceRegistry(lastDiscoveredTables, cachedRegistryState);
                    renderer.printTables(lastDiscoveredTables, cachedRegistryState, out);
                    continue;
                }

                if ("ls".equalsIgnoreCase(line)) {
                    ls(out, err);
                    continue;
                }

                if ("pwd".equalsIgnoreCase(line)) {
                    pwd(out);
                    continue;
                }

                if ("cd".equalsIgnoreCase(line) || "cd /".equalsIgnoreCase(line)) {
                    cdRoot();
                    continue;
                }

                if ("cd ..".equalsIgnoreCase(line)) {
                    cdUp();
                    continue;
                }

                if (line.regionMatches(true, 0, "cd ", 0, 3)) {
                    cd(line.substring(3).trim(), err);
                    continue;
                }

                if ("show".equalsIgnoreCase(line)) {
                    if (currentTable != null) {
                        show(currentTable.getTableName(), out, err);
                    } else {
                        err.println("show requires a table name or index (or cd into a table first)");
                    }
                    continue;
                }

                if (line.regionMatches(true, 0, "show ", 0, 5)) {
                    show(line.substring(5).trim(), out, err);
                    continue;
                }

                if ("check columns".equalsIgnoreCase(line)) {
                    checkColumns(out, err);
                    continue;
                }

                err.println("Unknown command: " + line);
                renderer.printHelp(out);
            } catch (Throwable th) {
                err.println("command failed: " + th.getMessage());
            }
        }
    }

    private String buildPrompt() {
        StringBuilder sb = new StringBuilder("recover:");
        if (currentTable == null) {
            sb.append("/");
        } else {
            sb.append("/").append(currentTable.getTableName());
            if (currentPartitionIndex >= 0) {
                sb.append("/").append(formatPartitionName(currentPartitionIndex));
            }
        }
        sb.append("> ");
        return sb.toString();
    }

    private void cd(String target, PrintStream err) {
        if (target.isEmpty() || "/".equals(target)) {
            cdRoot();
            return;
        }

        if ("..".equals(target)) {
            cdUp();
            return;
        }

        if (currentTable == null) {
            cdIntoTable(target, err);
        } else if (currentPartitionIndex < 0) {
            cdIntoPartition(target, err);
        } else {
            err.println("already at leaf level (partition); cd .. to go up");
        }
    }

    private void cdIntoPartition(String target, PrintStream err) {
        if (cachedPartitionScan == null || cachedPartitionScan.size() == 0) {
            err.println("no partitions available");
            return;
        }

        // try numeric index (0-based)
        try {
            int index = Numbers.parseInt(target);
            if (index >= 0 && index < cachedPartitionScan.size()) {
                currentPartitionIndex = index;
                return;
            }
            // fall through to try by formatted partition name (e.g. YEAR partitions like "2024")
        } catch (NumericException ignore) {
            // not a number, try by name
        }

        // try by partition name, then by raw dir name
        for (int i = 0, n = cachedPartitionScan.size(); i < n; i++) {
            PartitionScanEntry entry = cachedPartitionScan.getQuick(i);
            if (target.equals(entry.getPartitionName())) {
                currentPartitionIndex = i;
                return;
            }
        }
        for (int i = 0, n = cachedPartitionScan.size(); i < n; i++) {
            PartitionScanEntry entry = cachedPartitionScan.getQuick(i);
            if (target.equals(entry.getDirName())) {
                currentPartitionIndex = i;
                return;
            }
        }

        err.println("partition not found: " + target);
    }

    private void cdIntoTable(String target, PrintStream err) {
        if (lastDiscoveredTables.size() == 0) {
            lastDiscoveredTables = tableDiscoveryService.discoverTables(dbRoot);
        }

        DiscoveredTable table = findTable(target);
        if (table == null) {
            err.println("table not found: " + target);
            return;
        }

        currentTable = table;
        currentPartitionIndex = -1;
        cachedTxnState = txnStateService.readTxnState(dbRoot, table);
        cachedMetaState = metaStateService.readMetaState(dbRoot, table);
        cachedCvState = columnVersionStateService.readColumnVersionState(dbRoot, table);
        try (Path path = new Path()) {
            path.of(dbRoot).concat(table.getDirName()).$();
            cachedPartitionScan = partitionScanService.scan(
                    path.toString(),
                    cachedTxnState, cachedMetaState
            );
        }
    }

    private void cdRoot() {
        currentTable = null;
        currentPartitionIndex = -1;
        cachedCvState = null;
        cachedMetaState = null;
        cachedPartitionScan = null;
        cachedTxnState = null;
    }

    private void cdUp() {
        if (currentPartitionIndex >= 0) {
            currentPartitionIndex = -1;
        } else if (currentTable != null) {
            cdRoot();
        }
    }

    private void checkAllPartitions(
            String tableName,
            String tableDir,
            ObjList<PartitionScanEntry> partitionScan,
            MetaState metaState,
            ColumnVersionState cvState,
            TxnState txnState,
            PrintStream out
    ) {
        int checked = 0;
        int errors = 0;
        int warnings = 0;
        int skipped = 0;

        for (int i = 0, n = partitionScan.size(); i < n; i++) {
            PartitionScanEntry entry = partitionScan.getQuick(i);

            if (entry.getStatus() == PartitionScanStatus.MISSING) {
                renderer.printCheckSkipped(entry.getPartitionName(), "MISSING", out);
                skipped++;
                continue;
            }

            TxnPartitionState txnPart = entry.getTxnPartition();
            if (txnPart == null) {
                renderer.printCheckSkipped(entry.getPartitionName(), "ORPHAN (no row count)", out);
                skipped++;
                continue;
            }

            if (txnPart.isParquetFormat()) {
                renderer.printCheckSkipped(entry.getPartitionName(), "parquet", out);
                skipped++;
                continue;
            }

            long rowCount = resolvePartitionRowCount(txnPart, i, partitionScan.size(), txnState);
            ColumnCheckResult result = columnCheckService.checkPartition(
                    tableDir,
                    entry.getDirName(),
                    txnPart.getTimestampLo(),
                    rowCount,
                    metaState,
                    cvState
            );
            renderer.printCheckResult(result, tableName, rowCount, out);
            checked++;

            for (int j = 0, m = result.getEntries().size(); j < m; j++) {
                ColumnCheckEntry checkEntry = result.getEntries().getQuick(j);
                switch (checkEntry.getStatus()) {
                    case ERROR -> errors++;
                    case WARNING -> warnings++;
                }
            }
        }

        renderer.printCheckSummary(tableName, checked, errors, warnings, skipped, out);
    }

    private void checkColumns(PrintStream out, PrintStream err) {
        if (currentTable == null) {
            checkColumnsFromRoot(out, err);
        } else if (currentPartitionIndex < 0) {
            checkColumnsForTable(currentTable, out, err);
        } else {
            checkColumnsForPartition(out, err);
        }
    }

    private void checkColumnsForPartition(PrintStream out, PrintStream err) {
        if (cachedMetaState == null || cachedMetaState.getColumns().size() == 0) {
            err.println("no meta state available for current table");
            return;
        }
        if (cachedPartitionScan == null || currentPartitionIndex >= cachedPartitionScan.size()) {
            err.println("no partition scan available");
            return;
        }

        PartitionScanEntry entry = cachedPartitionScan.getQuick(currentPartitionIndex);
        TxnPartitionState txnPart = entry.getTxnPartition();
        if (txnPart == null) {
            err.println("cannot check: partition has no row count (ORPHAN)");
            return;
        }
        if (txnPart.isParquetFormat()) {
            renderer.printCheckSkipped(entry.getPartitionName(), "parquet", out);
            return;
        }

        long rowCount = resolvePartitionRowCount(txnPart, currentPartitionIndex, cachedPartitionScan.size(), cachedTxnState);

        try (Path path = new Path()) {
            path.of(dbRoot).concat(currentTable.getDirName()).$();
            ColumnVersionState cvState = hasCvIssues(cachedCvState) ? null : cachedCvState;
            ColumnCheckResult result = columnCheckService.checkPartition(
                    path.toString(),
                    entry.getDirName(),
                    txnPart.getTimestampLo(),
                    rowCount,
                    cachedMetaState,
                    cvState
            );
            renderer.printCheckResult(result, currentTable.getTableName(), rowCount, out);

            int errors = 0, warnings = 0;
            for (int j = 0, m = result.getEntries().size(); j < m; j++) {
                ColumnCheckEntry checkEntry = result.getEntries().getQuick(j);
                switch (checkEntry.getStatus()) {
                    case ERROR -> errors++;
                    case WARNING -> warnings++;
                }
            }
            renderer.printCheckSummary(currentTable.getTableName(), 1, errors, warnings, 0, out);
        }
    }

    private void checkColumnsForTable(DiscoveredTable table, PrintStream out, PrintStream err) {
        MetaState metaState = cachedMetaState != null ? cachedMetaState : metaStateService.readMetaState(dbRoot, table);
        if (metaState.getColumns().size() == 0) {
            err.println("no columns found for table: " + table.getTableName());
            return;
        }

        TxnState txnState = cachedTxnState != null ? cachedTxnState : txnStateService.readTxnState(dbRoot, table);
        ColumnVersionState cvState = cachedCvState != null ? cachedCvState : columnVersionStateService.readColumnVersionState(dbRoot, table);
        ObjList<PartitionScanEntry> partitionScan = cachedPartitionScan;
        if (partitionScan == null) {
            try (Path path = new Path()) {
                path.of(dbRoot).concat(table.getDirName()).$();
                partitionScan = partitionScanService.scan(path.toString(), txnState, metaState);
            }
        }

        try (Path path = new Path()) {
            path.of(dbRoot).concat(table.getDirName()).$();
            checkAllPartitions(
                    table.getTableName(),
                    path.toString(),
                    partitionScan,
                    metaState,
                    hasCvIssues(cvState) ? null : cvState,
                    txnState,
                    out
            );
        }
    }

    private void checkColumnsFromRoot(PrintStream out, PrintStream err) {
        if (lastDiscoveredTables.size() == 0) {
            lastDiscoveredTables = tableDiscoveryService.discoverTables(dbRoot);
        }

        for (int i = 0, n = lastDiscoveredTables.size(); i < n; i++) {
            DiscoveredTable table = lastDiscoveredTables.getQuick(i);
            out.println();
            out.println(color.bold("=== " + table.getTableName() + " ==="));
            checkColumnsForTable(table, out, err);
        }
    }

    private DiscoveredTable findTable(String target) {
        try {
            final int index = Numbers.parseInt(target);
            if (index >= 1 && index <= lastDiscoveredTables.size()) {
                return lastDiscoveredTables.getQuick(index - 1);
            }
            return null;
        } catch (NumericException ignore) {
            for (int i = 0, n = lastDiscoveredTables.size(); i < n; i++) {
                DiscoveredTable table = lastDiscoveredTables.getQuick(i);
                if (target.equals(table.getTableName()) || target.equals(table.getDirName())) {
                    return table;
                }
            }
            return null;
        }
    }

    private String formatPartitionName(int partitionIndex) {
        if (cachedPartitionScan == null || partitionIndex < 0 || partitionIndex >= cachedPartitionScan.size()) {
            return "?";
        }
        return cachedPartitionScan.getQuick(partitionIndex).getPartitionName();
    }

    private static boolean hasCvIssues(ColumnVersionState cvState) {
        if (cvState == null) {
            return true;
        }
        for (int i = 0, n = cvState.getIssues().size(); i < n; i++) {
            if (cvState.getIssues().getQuick(i).getSeverity() == RecoveryIssueSeverity.ERROR) {
                return true;
            }
        }
        return false;
    }

    private void ls(PrintStream out, PrintStream err) {
        if (currentTable == null) {
            lastDiscoveredTables = tableDiscoveryService.discoverTables(dbRoot);
            cachedRegistryState = registryStateService.readRegistryState(dbRoot);
            tableDiscoveryService.crossReferenceRegistry(lastDiscoveredTables, cachedRegistryState);
            renderer.printTables(lastDiscoveredTables, cachedRegistryState, out);
        } else if (currentPartitionIndex < 0) {
            renderer.printPartitionScan(cachedPartitionScan, cachedTxnState, cachedMetaState, out);
        } else {
            if (cachedMetaState == null) {
                err.println("no meta state cached for current table");
                return;
            }
            renderer.printColumns(cachedMetaState, out);
        }
    }

    private void pwd(PrintStream out) {
        StringBuilder sb = new StringBuilder("/");
        if (currentTable != null) {
            sb.append(currentTable.getTableName());
            if (currentPartitionIndex >= 0) {
                sb.append("/").append(formatPartitionName(currentPartitionIndex));
            }
        }
        out.println(sb);
    }

    private static long resolvePartitionRowCount(TxnPartitionState txnPart, int partitionIndex, int totalPartitions, TxnState txnState) {
        // The last partition's row count is stored in transientRowCount in the _txn header,
        // not in the partition entry itself (which stores 0 for the last partition).
        if (partitionIndex == totalPartitions - 1
                && txnState != null
                && txnState.getTransientRowCount() != TxnState.UNSET_LONG) {
            return txnState.getTransientRowCount();
        }
        return txnPart.getRowCount();
    }

    private void show(String target, PrintStream out, PrintStream err) {
        if (target.isEmpty()) {
            if (currentTable != null) {
                TxnState state = cachedTxnState != null ? cachedTxnState : txnStateService.readTxnState(dbRoot, currentTable);
                renderer.printShow(currentTable, state, out);
            } else {
                err.println("show requires a table name or index (or cd into a table first)");
            }
            return;
        }

        if (lastDiscoveredTables.size() == 0) {
            lastDiscoveredTables = tableDiscoveryService.discoverTables(dbRoot);
        }

        DiscoveredTable table = findTable(target);
        if (table == null) {
            err.println("table not found: " + target);
            return;
        }

        TxnState state = txnStateService.readTxnState(dbRoot, table);
        renderer.printShow(table, state, out);
    }
}
