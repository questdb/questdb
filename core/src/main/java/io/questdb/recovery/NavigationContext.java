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

/**
 * Owns all mutable navigation state for the recovery session. The user navigates
 * a four-level hierarchy: <em>root → table → partition → column</em>, with an
 * alternative WAL branch: <em>table → wal root → wal dir → segment</em>.
 *
 * <p>On {@code cd} into a table, the context eagerly reads and caches
 * {@link TxnState}, {@link MetaState}, {@link ColumnVersionState}, and the
 * partition scan. On {@code cd} into a column, per-column cache values
 * (column top, name txn, effective row count, in-partition flag) are computed
 * from the cached column-version state.
 *
 * <p>{@code cd wal} from table level enters the WAL navigation branch via
 * {@link WalNavigationContext}. {@code cd ..} from WAL root returns to table.
 *
 * <p>{@code cd ..} from column level clears column cache only; from partition
 * level clears the partition index; from table level resets all caches (same
 * as {@code cd /}).
 */
public class NavigationContext {
    private final BoundedColumnVersionReader columnVersionReader;
    private final CharSequence dbRoot;
    private final BoundedMetaReader metaReader;
    private final PartitionScanService partitionScanService;
    private final BoundedRegistryReader registryReader;
    private final BoundedSeqTxnLogReader seqTxnLogReader;
    private final TableDiscoveryService tableDiscoveryService;
    private final BoundedTxnReader txnReader;
    private final WalDiscoveryService walDiscoveryService;
    private boolean cachedColumnInPartition = true;
    private long cachedColumnNameTxn = -1;
    private long cachedColumnTop = -1;
    private ColumnVersionState cachedCvState;
    private long cachedEffectiveRows = -1;
    private MetaState cachedMetaState;
    private SeqTxnLogState cachedSeqTxnLogState;
    private ObjList<PartitionScanEntry> cachedPartitionScan;
    private String previousPath;
    private RegistryState cachedRegistryState;
    private TxnState cachedTxnState;
    private int currentColumnIndex = -1;
    private int currentPartitionIndex = -1;
    private DiscoveredTable currentTable;
    private ObjList<DiscoveredTable> lastDiscoveredTables = new ObjList<>();
    private WalNavigationContext walNav;

    public NavigationContext(
            CharSequence dbRoot,
            BoundedColumnVersionReader columnVersionReader,
            BoundedMetaReader metaReader,
            PartitionScanService partitionScanService,
            BoundedRegistryReader registryReader,
            BoundedSeqTxnLogReader seqTxnLogReader,
            TableDiscoveryService tableDiscoveryService,
            BoundedTxnReader txnReader,
            WalDiscoveryService walDiscoveryService
    ) {
        this.dbRoot = dbRoot;
        this.columnVersionReader = columnVersionReader;
        this.metaReader = metaReader;
        this.partitionScanService = partitionScanService;
        this.registryReader = registryReader;
        this.seqTxnLogReader = seqTxnLogReader;
        this.tableDiscoveryService = tableDiscoveryService;
        this.txnReader = txnReader;
        this.walDiscoveryService = walDiscoveryService;
    }

    public String buildPath() {
        if (currentTable == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(currentTable.getTableName());
        if (walNav != null) {
            sb.append("/wal");
            if (walNav.getCurrentWalId() >= 0) {
                sb.append("/wal").append(walNav.getCurrentWalId());
                if (walNav.getCurrentSegmentId() >= 0) {
                    sb.append("/").append(walNav.getCurrentSegmentId());
                }
            }
        } else if (currentPartitionIndex >= 0) {
            sb.append("/").append(formatPartitionName(currentPartitionIndex));
            if (currentColumnIndex >= 0) {
                sb.append("/").append(formatColumnName(currentColumnIndex));
            }
        }
        return sb.toString();
    }

    public String buildPrompt() {
        StringBuilder sb = new StringBuilder("recover:");
        if (currentTable == null) {
            sb.append("/");
        } else {
            sb.append("/").append(currentTable.getTableName());
            if (walNav != null) {
                sb.append(walNav.buildPromptSuffix());
            } else if (currentPartitionIndex >= 0) {
                sb.append("/").append(formatPartitionName(currentPartitionIndex));
                if (currentColumnIndex >= 0) {
                    sb.append("/").append(formatColumnName(currentColumnIndex));
                }
            }
        }
        sb.append("> ");
        return sb.toString();
    }

    public boolean cd(String target, PrintStream err) {
        if (target.isEmpty() || "/".equals(target)) {
            previousPath = buildPath();
            cdRoot();
            return true;
        }

        if ("..".equals(target)) {
            previousPath = buildPath();
            cdUp();
            return true;
        }

        if ("-".equals(target)) {
            if (previousPath == null) {
                err.println("no previous directory");
                return false;
            }
            String currentPath = buildPath();
            String targetPath = previousPath;
            // temporarily nullify previousPath so recursive cd() doesn't overwrite it
            previousPath = null;
            cdRoot();
            if (targetPath.isEmpty()) {
                previousPath = currentPath;
                return true;
            }
            // navigate each segment of targetPath
            String[] segments = targetPath.split("/");
            boolean ok = true;
            for (String segment : segments) {
                if (segment.isEmpty()) {
                    continue;
                }
                if (!cdSingle(segment, err)) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                previousPath = currentPath;
                return true;
            }
            // restore: cd back to where we were
            cdRoot();
            if (!currentPath.isEmpty()) {
                String[] restoreSegments = currentPath.split("/");
                for (String segment : restoreSegments) {
                    if (!segment.isEmpty()) {
                        cdSingle(segment, err);
                    }
                }
            }
            previousPath = targetPath;
            return false;
        }

        // multi-level cd: split on "/" and navigate each segment
        if (target.contains("/")) {
            String savedPath = buildPath();
            String[] segments = target.split("/");
            for (String segment : segments) {
                if (segment.isEmpty()) {
                    continue;
                }
                if ("..".equals(segment)) {
                    cdUp();
                } else if (!cdSingle(segment, err)) {
                    previousPath = savedPath;
                    return false;
                }
            }
            previousPath = savedPath;
            return true;
        }

        String savedPath = buildPath();
        if (cdSingle(target, err)) {
            previousPath = savedPath;
            return true;
        }
        return false;
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
        cachedRegistryState = null;
        cachedSeqTxnLogState = null;
        cachedTxnState = null;
        walNav = null;
    }

    public void cdUp() {
        if (walNav != null) {
            if (walNav.isAtWalRoot()) {
                // leaving WAL mode back to table level
                walNav = null;
            } else {
                walNav.cdUp();
            }
        } else if (currentColumnIndex >= 0) {
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
        TxnPartitionState txnPart = entry.txnPartition();
        long partitionTimestamp = txnPart != null ? txnPart.timestampLo() : 0;
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
            if (index >= 0 && index < lastDiscoveredTables.size()) {
                return lastDiscoveredTables.getQuick(index);
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
        return cachedMetaState.getColumns().getQuick(columnIndex).name();
    }

    public String formatPartitionName(int partitionIndex) {
        if (cachedPartitionScan == null || partitionIndex < 0 || partitionIndex >= cachedPartitionScan.size()) {
            return "?";
        }
        return cachedPartitionScan.getQuick(partitionIndex).partitionName();
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

    public SeqTxnLogState getCachedSeqTxnLogState() {
        return cachedSeqTxnLogState;
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

    public WalDiscoveryService getWalDiscoveryService() {
        return walDiscoveryService;
    }

    public WalNavigationContext getWalNav() {
        return walNav;
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
        return currentTable != null && currentPartitionIndex < 0 && walNav == null;
    }

    public boolean isInWalMode() {
        return walNav != null;
    }

    public void pwd(PrintStream out) {
        StringBuilder sb = new StringBuilder("/");
        if (currentTable != null) {
            sb.append(currentTable.getTableName());
            if (walNav != null) {
                sb.append(walNav.buildPromptSuffix());
            } else if (currentPartitionIndex >= 0) {
                sb.append("/").append(formatPartitionName(currentPartitionIndex));
                if (currentColumnIndex >= 0) {
                    sb.append("/").append(formatColumnName(currentColumnIndex));
                }
            }
        }
        out.println(sb);
    }

    public SeqTxnLogState readSeqTxnLogState() {
        if (currentTable == null) {
            return null;
        }
        cachedSeqTxnLogState = seqTxnLogReader.readForTable(dbRoot, currentTable);
        return cachedSeqTxnLogState;
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
            if (cvState.getIssues().getQuick(i).severity() == RecoveryIssueSeverity.ERROR) {
                return true;
            }
        }
        return false;
    }

    private boolean cdSingle(String target, PrintStream err) {
        // WAL navigation delegation
        if (walNav != null) {
            return walNav.cd(target, err);
        }

        // "cd wal" from table level enters WAL navigation
        if ("wal".equals(target) && currentTable != null && currentPartitionIndex < 0) {
            return cdIntoWal(err);
        }

        if (currentTable == null) {
            return cdIntoTable(target, err);
        } else if (currentPartitionIndex < 0) {
            return cdIntoPartition(target, err);
        } else if (currentColumnIndex < 0) {
            return cdIntoColumn(target, err);
        } else {
            err.println("already at leaf level (column); cd .. to go up");
            return false;
        }
    }

    private boolean cdIntoColumn(String target, PrintStream err) {
        if (cachedMetaState == null || cachedMetaState.getColumns().size() == 0) {
            err.println("no columns available");
            return false;
        }

        PartitionScanEntry partEntry = cachedPartitionScan.getQuick(currentPartitionIndex);
        if (partEntry.txnPartition() != null && partEntry.txnPartition().parquetFormat()) {
            err.println("cannot enter columns of a parquet partition");
            return false;
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

        // try by column name, skipping dropped columns (negative type)
        if (colIndex < 0) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                MetaColumnState candidate = columns.getQuick(i);
                if (candidate.type() >= 0 && target.equalsIgnoreCase(candidate.name())) {
                    colIndex = i;
                    break;
                }
            }
        }

        if (colIndex < 0) {
            err.println("column not found: " + target);
            return false;
        }

        MetaColumnState col = columns.getQuick(colIndex);
        if (col.type() < 0) {
            // numeric index explicitly targeting a dropped column
            err.println("cannot enter dropped column: " + col.name());
            return false;
        }

        currentColumnIndex = colIndex;
        computeColumnCache();
        return true;
    }

    private boolean cdIntoWal(PrintStream err) {
        DiscoveredTable table = currentTable;
        if (table.isWalEnabledKnown() && !table.isWalEnabled()) {
            err.println("this table does not use WAL");
            return false;
        }

        // ensure seqTxnLogState is loaded so scan can cross-reference
        if (cachedSeqTxnLogState == null) {
            cachedSeqTxnLogState = seqTxnLogReader.readForTable(dbRoot, table);
        }

        // scan WAL directories, cross-referencing with seq txnlog state
        try (Path path = new Path()) {
            path.of(dbRoot).concat(table.getDirName()).$();
            WalScanState walScanState = walDiscoveryService.scan(path.toString(), cachedSeqTxnLogState);
            walNav = new WalNavigationContext(walScanState);
        }
        return true;
    }

    private boolean cdIntoPartition(String target, PrintStream err) {
        if (cachedPartitionScan == null || cachedPartitionScan.size() == 0) {
            err.println("no partitions available");
            return false;
        }

        // try numeric index (0-based)
        try {
            int index = Numbers.parseInt(target);
            if (index >= 0 && index < cachedPartitionScan.size()) {
                currentPartitionIndex = index;
                return true;
            }
            // fall through to try by formatted partition name (e.g. YEAR partitions like "2024")
        } catch (NumericException ignore) {
            // not a number, try by name
        }

        // try by partition name, then by raw dir name
        for (int i = 0, n = cachedPartitionScan.size(); i < n; i++) {
            PartitionScanEntry entry = cachedPartitionScan.getQuick(i);
            if (target.equals(entry.partitionName())) {
                currentPartitionIndex = i;
                return true;
            }
        }
        for (int i = 0, n = cachedPartitionScan.size(); i < n; i++) {
            PartitionScanEntry entry = cachedPartitionScan.getQuick(i);
            if (target.equals(entry.dirName())) {
                currentPartitionIndex = i;
                return true;
            }
        }

        err.println("partition not found: " + target);
        return false;
    }

    private boolean cdIntoTable(String target, PrintStream err) {
        if (lastDiscoveredTables.size() == 0) {
            lastDiscoveredTables = tableDiscoveryService.discoverTables(dbRoot);
        }

        DiscoveredTable table = findTable(target);
        if (table == null) {
            err.println("table not found: " + target);
            return false;
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
        return true;
    }

    private void computeColumnCache() {
        PartitionScanEntry entry = cachedPartitionScan.getQuick(currentPartitionIndex);
        TxnPartitionState txnPart = entry.txnPartition();
        long partitionRowCount = txnPart != null
                ? entry.rowCount()
                : 0;
        long partitionTimestamp = txnPart != null ? txnPart.timestampLo() : 0;

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
