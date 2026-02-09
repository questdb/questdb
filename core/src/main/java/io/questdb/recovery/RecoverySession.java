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
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;

public class RecoverySession {
    private boolean cachedColumnInPartition = true;
    private long cachedColumnNameTxn = -1;
    private long cachedColumnTop = -1;
    private ColumnVersionState cachedCvState;
    private long cachedEffectiveRows = -1;
    private MetaState cachedMetaState;
    private ObjList<PartitionScanEntry> cachedPartitionScan;
    private RegistryState cachedRegistryState;
    private TxnState cachedTxnState;
    private final ColumnCheckService columnCheckService;
    private final AnsiColor color;
    private final ColumnValueReader columnValueReader;
    private final ColumnVersionStateService columnVersionStateService;
    private int currentColumnIndex = -1;
    private int currentPartitionIndex = -1;
    private DiscoveredTable currentTable;
    private final CharSequence dbRoot;
    private final FilesFacade ff;
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
            ColumnValueReader columnValueReader,
            ColumnVersionStateService columnVersionStateService,
            FilesFacade ff,
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
        this.columnValueReader = columnValueReader;
        this.columnVersionStateService = columnVersionStateService;
        this.ff = ff;
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
                    if (currentColumnIndex >= 0) {
                        showColumn(out, err);
                    } else if (currentTable != null) {
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

                if (line.regionMatches(true, 0, "print ", 0, 6)) {
                    printValue(line.substring(6).trim(), out, err);
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
                if (currentColumnIndex >= 0) {
                    sb.append("/").append(formatColumnName(currentColumnIndex));
                }
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
        } else if (currentColumnIndex < 0) {
            cdIntoColumn(target, err);
        } else {
            err.println("already at leaf level (column); cd .. to go up");
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

    private void cdIntoColumn(String target, PrintStream err) {
        if (cachedMetaState == null || cachedMetaState.getColumns().size() == 0) {
            err.println("no columns available");
            return;
        }

        PartitionScanEntry partEntry = cachedPartitionScan.getQuick(currentPartitionIndex);
        if (partEntry.getTxnPartition() != null && partEntry.getTxnPartition().isParquetFormat()) {
            err.println("cannot enter columns of a parquet partition");
            return;
        }

        ObjList<MetaColumnState> columns = cachedMetaState.getColumns();
        int colIndex = -1;

        // try numeric index (0-based)
        try {
            int index = Numbers.parseInt(target);
            if (index >= 0 && index < columns.size()) {
                colIndex = index;
            }
        } catch (NumericException ignore) {
            // not a number, try by name
        }

        // try by column name
        if (colIndex < 0) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                if (target.equalsIgnoreCase(columns.getQuick(i).getName())) {
                    colIndex = i;
                    break;
                }
            }
        }

        if (colIndex < 0) {
            err.println("column not found: " + target);
            return;
        }

        MetaColumnState col = columns.getQuick(colIndex);
        if (col.getType() < 0) {
            err.println("cannot enter dropped column: " + col.getName());
            return;
        }

        currentColumnIndex = colIndex;
        computeColumnCache();
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
        currentColumnIndex = -1;
        currentPartitionIndex = -1;
        cachedColumnInPartition = true;
        cachedColumnNameTxn = -1;
        cachedColumnTop = -1;
        cachedCvState = null;
        cachedEffectiveRows = -1;
        cachedMetaState = null;
        cachedPartitionScan = null;
        cachedTxnState = null;
    }

    private void cdUp() {
        if (currentColumnIndex >= 0) {
            currentColumnIndex = -1;
            cachedColumnInPartition = true;
            cachedColumnNameTxn = -1;
            cachedColumnTop = -1;
            cachedEffectiveRows = -1;
        } else if (currentPartitionIndex >= 0) {
            currentPartitionIndex = -1;
        } else if (currentTable != null) {
            cdRoot();
        }
    }

    private void computeColumnCache() {
        PartitionScanEntry entry = cachedPartitionScan.getQuick(currentPartitionIndex);
        TxnPartitionState txnPart = entry.getTxnPartition();
        long partitionRowCount = txnPart != null
                ? entry.getRowCount()
                : 0;
        long partitionTimestamp = txnPart != null ? txnPart.getTimestampLo() : 0;

        ColumnVersionState cvState = cachedCvState != null && !hasCvIssues(cachedCvState) ? cachedCvState : null;
        cachedColumnTop = cvState != null ? cvState.getColumnTop(partitionTimestamp, currentColumnIndex) : 0;
        cachedColumnNameTxn = cvState != null ? cvState.getColumnNameTxn(partitionTimestamp, currentColumnIndex) : -1;

        if (cachedColumnTop == -1) {
            cachedColumnInPartition = false;
            cachedColumnTop = 0;
            cachedEffectiveRows = 0;
            return;
        }
        cachedColumnInPartition = true;
        cachedEffectiveRows = Math.max(0, partitionRowCount - cachedColumnTop);
    }

    private long[] computeAllColumnTops() {
        ObjList<MetaColumnState> columns = cachedMetaState.getColumns();
        long[] tops = new long[columns.size()];
        PartitionScanEntry entry = cachedPartitionScan.getQuick(currentPartitionIndex);
        TxnPartitionState txnPart = entry.getTxnPartition();
        long partitionTimestamp = txnPart != null ? txnPart.getTimestampLo() : 0;
        ColumnVersionState cvState = cachedCvState != null && !hasCvIssues(cachedCvState) ? cachedCvState : null;
        for (int i = 0, n = columns.size(); i < n; i++) {
            tops[i] = cvState != null ? cvState.getColumnTop(partitionTimestamp, i) : 0;
        }
        return tops;
    }

    private void checkAllPartitions(
            String tableName,
            String tableDir,
            ObjList<PartitionScanEntry> partitionScan,
            MetaState metaState,
            ColumnVersionState cvState,
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

            long rowCount = entry.getRowCount();
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

        renderer.printCheckSummary(checked, errors, warnings, skipped, out);
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

        long rowCount = entry.getRowCount();

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
            renderer.printCheckSummary(1, errors, warnings, 0, out);
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

    private String formatColumnName(int columnIndex) {
        if (cachedMetaState == null || columnIndex < 0 || columnIndex >= cachedMetaState.getColumns().size()) {
            return "?";
        }
        return cachedMetaState.getColumns().getQuick(columnIndex).getName();
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
        } else if (currentColumnIndex < 0) {
            if (cachedMetaState == null) {
                err.println("no meta state cached for current table");
                return;
            }
            long[] columnTops = computeAllColumnTops();
            renderer.printColumns(cachedMetaState, columnTops, out);
        } else {
            showColumn(out, err);
        }
    }

    private void printValue(String rowArg, PrintStream out, PrintStream err) {
        if (currentColumnIndex < 0) {
            err.println("print is only valid at column level");
            return;
        }

        if (!cachedColumnInPartition) {
            err.println("column not in this partition (added later)");
            return;
        }

        long rowNo;
        try {
            rowNo = Numbers.parseLong(rowArg);
        } catch (NumericException e) {
            err.println("invalid row number: " + rowArg);
            return;
        }

        long partitionRowCount = cachedEffectiveRows + cachedColumnTop;
        if (rowNo < 0 || rowNo >= partitionRowCount) {
            err.println("row out of range [0, " + partitionRowCount + ")");
            return;
        }

        if (rowNo < cachedColumnTop) {
            out.println("[" + rowNo + "] = null (column top)");
            return;
        }

        MetaColumnState col = cachedMetaState.getColumns().getQuick(currentColumnIndex);
        PartitionScanEntry entry = cachedPartitionScan.getQuick(currentPartitionIndex);
        long fileRowNo = rowNo - cachedColumnTop;

        try (Path path = new Path()) {
            path.of(dbRoot).concat(currentTable.getDirName()).$();
            String value = columnValueReader.readValue(
                    path.toString(),
                    entry.getDirName(),
                    col.getName(),
                    cachedColumnNameTxn,
                    col.getType(),
                    fileRowNo,
                    cachedEffectiveRows
            );
            out.println("[" + rowNo + "] = " + value);
        }
    }

    private void pwd(PrintStream out) {
        StringBuilder sb = new StringBuilder("/");
        if (currentTable != null) {
            sb.append(currentTable.getTableName());
            if (currentPartitionIndex >= 0) {
                sb.append("/").append(formatPartitionName(currentPartitionIndex));
                if (currentColumnIndex >= 0) {
                    sb.append("/").append(formatColumnName(currentColumnIndex));
                }
            }
        }
        out.println(sb);
    }


    private void showColumn(PrintStream out, PrintStream err) {
        if (cachedMetaState == null || currentColumnIndex < 0) {
            err.println("no column selected");
            return;
        }

        MetaColumnState col = cachedMetaState.getColumns().getQuick(currentColumnIndex);

        if (!cachedColumnInPartition) {
            renderer.printColumnDetail(col, currentColumnIndex,
                    0, cachedColumnNameTxn, 0,
                    -1, -1, -1, -1, false, out);
            return;
        }

        PartitionScanEntry entry = cachedPartitionScan.getQuick(currentPartitionIndex);
        int colType = col.getType();

        long expectedDataSize;
        long expectedAuxSize = -1;
        long actualDataSize;
        long actualAuxSize = -1;

        try (Path path = new Path()) {
            path.of(dbRoot).concat(currentTable.getDirName()).slash().concat(entry.getDirName()).slash();
            int pathLen = path.size();

            if (ColumnType.isVarSize(colType)) {
                expectedAuxSize = cachedEffectiveRows >= 0 ? ColumnType.getDriver(colType).getAuxVectorSize(cachedEffectiveRows) : -1;
                // .d file
                path.trimTo(pathLen);
                TableUtils.dFile(path, col.getName(), cachedColumnNameTxn);
                actualDataSize = ff.length(path.$());
                if (actualDataSize < 0) {
                    actualDataSize = -1;
                }
                // .i file
                path.trimTo(pathLen);
                TableUtils.iFile(path, col.getName(), cachedColumnNameTxn);
                actualAuxSize = ff.length(path.$());
                if (actualAuxSize < 0) {
                    actualAuxSize = -1;
                }
                expectedDataSize = -1;
                if (cachedEffectiveRows == 0) {
                    expectedDataSize = 0;
                } else if (actualAuxSize >= 0) {
                    long auxFd = ff.openRO(path.$());
                    if (auxFd >= 0) {
                        try {
                            expectedDataSize = ColumnType.getDriver(colType).getDataVectorSizeAtFromFd(ff, auxFd, cachedEffectiveRows - 1);
                        } catch (Exception ignore) {
                            // expectedDataSize is already -1 from assignment above
                        } finally {
                            ff.close(auxFd);
                        }
                    }
                }
            } else {
                int typeSize = ColumnType.sizeOf(colType);
                expectedDataSize = cachedEffectiveRows * typeSize;
                path.trimTo(pathLen);
                TableUtils.dFile(path, col.getName(), cachedColumnNameTxn);
                actualDataSize = ff.length(path.$());
                if (actualDataSize < 0) {
                    actualDataSize = -1;
                }
            }
        }

        renderer.printColumnDetail(
                col, currentColumnIndex,
                cachedColumnTop, cachedColumnNameTxn, cachedEffectiveRows,
                expectedDataSize, actualDataSize,
                expectedAuxSize, actualAuxSize, true, out
        );
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
