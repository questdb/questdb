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

import java.io.PrintStream;

public class NavigationContext {
    private final BoundedColumnVersionReader columnVersionReader;
    private final CharSequence dbRoot;
    private final BoundedMetaReader metaReader;
    private final PartitionScanService partitionScanService;
    private final BoundedRegistryReader registryReader;
    private final TableDiscoveryService tableDiscoveryService;
    private final BoundedTxnReader txnReader;
    private boolean cachedColumnInPartition = true;
    private long cachedColumnNameTxn = -1;
    private long cachedColumnTop = -1;
    private ColumnVersionState cachedCvState;
    private long cachedEffectiveRows = -1;
    private MetaState cachedMetaState;
    private ObjList<PartitionScanEntry> cachedPartitionScan;
    private RegistryState cachedRegistryState;
    private TxnState cachedTxnState;
    private int currentColumnIndex = -1;
    private int currentPartitionIndex = -1;
    private DiscoveredTable currentTable;
    private ObjList<DiscoveredTable> lastDiscoveredTables = new ObjList<>();

    public NavigationContext(
            CharSequence dbRoot,
            BoundedColumnVersionReader columnVersionReader,
            BoundedMetaReader metaReader,
            PartitionScanService partitionScanService,
            BoundedRegistryReader registryReader,
            TableDiscoveryService tableDiscoveryService,
            BoundedTxnReader txnReader
    ) {
        this.dbRoot = dbRoot;
        this.columnVersionReader = columnVersionReader;
        this.metaReader = metaReader;
        this.partitionScanService = partitionScanService;
        this.registryReader = registryReader;
        this.tableDiscoveryService = tableDiscoveryService;
        this.txnReader = txnReader;
    }

    public String buildPrompt() {
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

    public void cd(String target, PrintStream err) {
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

    public void cdRoot() {
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

    public void cdUp() {
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

    public long[] computeAllColumnTops() {
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

    public void discoverTables() {
        lastDiscoveredTables = tableDiscoveryService.discoverTables(dbRoot);
    }

    public DiscoveredTable findTable(String target) {
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

    public String formatColumnName(int columnIndex) {
        if (cachedMetaState == null || columnIndex < 0 || columnIndex >= cachedMetaState.getColumns().size()) {
            return "?";
        }
        return cachedMetaState.getColumns().getQuick(columnIndex).getName();
    }

    public String formatPartitionName(int partitionIndex) {
        if (cachedPartitionScan == null || partitionIndex < 0 || partitionIndex >= cachedPartitionScan.size()) {
            return "?";
        }
        return cachedPartitionScan.getQuick(partitionIndex).getPartitionName();
    }

    public boolean getCachedColumnInPartition() {
        return cachedColumnInPartition;
    }

    public long getCachedColumnNameTxn() {
        return cachedColumnNameTxn;
    }

    public long getCachedColumnTop() {
        return cachedColumnTop;
    }

    public ColumnVersionState getCachedCvState() {
        return cachedCvState;
    }

    public long getCachedEffectiveRows() {
        return cachedEffectiveRows;
    }

    public MetaState getCachedMetaState() {
        return cachedMetaState;
    }

    public ObjList<PartitionScanEntry> getCachedPartitionScan() {
        return cachedPartitionScan;
    }

    public RegistryState getCachedRegistryState() {
        return cachedRegistryState;
    }

    public TxnState getCachedTxnState() {
        return cachedTxnState;
    }

    public BoundedColumnVersionReader getColumnVersionReader() {
        return columnVersionReader;
    }

    public int getCurrentColumnIndex() {
        return currentColumnIndex;
    }

    public int getCurrentPartitionIndex() {
        return currentPartitionIndex;
    }

    public DiscoveredTable getCurrentTable() {
        return currentTable;
    }

    public CharSequence getDbRoot() {
        return dbRoot;
    }

    public ObjList<DiscoveredTable> getLastDiscoveredTables() {
        return lastDiscoveredTables;
    }

    public BoundedMetaReader getMetaReader() {
        return metaReader;
    }

    public TableDiscoveryService getTableDiscoveryService() {
        return tableDiscoveryService;
    }

    public BoundedTxnReader getTxnReader() {
        return txnReader;
    }

    public boolean isAtColumn() {
        return currentColumnIndex >= 0;
    }

    public boolean isAtPartition() {
        return currentTable != null && currentPartitionIndex >= 0 && currentColumnIndex < 0;
    }

    public boolean isAtRoot() {
        return currentTable == null;
    }

    public boolean isAtTable() {
        return currentTable != null && currentPartitionIndex < 0;
    }

    public void pwd(PrintStream out) {
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

    public RegistryState readRegistryState() {
        cachedRegistryState = registryReader.read(dbRoot);
        return cachedRegistryState;
    }

    static boolean hasCvIssues(ColumnVersionState cvState) {
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
        cachedTxnState = txnReader.readForTable(dbRoot, table);
        cachedMetaState = metaReader.readForTable(dbRoot, table);
        cachedCvState = columnVersionReader.readForTable(dbRoot, table);
        try (Path path = new Path()) {
            path.of(dbRoot).concat(table.getDirName()).$();
            cachedPartitionScan = partitionScanService.scan(
                    path.toString(),
                    cachedTxnState, cachedMetaState
            );
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
}
