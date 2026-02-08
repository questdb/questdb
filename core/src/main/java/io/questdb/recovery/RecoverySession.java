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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;

public class RecoverySession {
    private MetaState cachedMetaState;
    private TxnState cachedTxnState;
    private int currentPartitionIndex = -1;
    private DiscoveredTable currentTable;
    private final CharSequence dbRoot;
    private ObjList<DiscoveredTable> lastDiscoveredTables = new ObjList<>();
    private final MetaStateService metaStateService;
    private final ConsoleRenderer renderer;
    private final TableDiscoveryService tableDiscoveryService;
    private final TxnStateService txnStateService;

    public RecoverySession(
            CharSequence dbRoot,
            MetaStateService metaStateService,
            TableDiscoveryService tableDiscoveryService,
            TxnStateService txnStateService,
            ConsoleRenderer renderer
    ) {
        this.dbRoot = dbRoot;
        this.metaStateService = metaStateService;
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
                    renderer.printTables(lastDiscoveredTables, out);
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
        if (cachedTxnState == null || cachedTxnState.getPartitions().size() == 0) {
            err.println("no partitions available");
            return;
        }

        // try numeric index (0-based)
        try {
            int index = Numbers.parseInt(target);
            if (index >= 0 && index < cachedTxnState.getPartitions().size()) {
                currentPartitionIndex = index;
                return;
            }
            // fall through to try by formatted partition name (e.g. YEAR partitions like "2024")
        } catch (NumericException ignore) {
            // not a number, try by name
        }

        // try by formatted partition name
        for (int i = 0, n = cachedTxnState.getPartitions().size(); i < n; i++) {
            String partName = formatPartitionName(i);
            if (target.equals(partName)) {
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
    }

    private void cdRoot() {
        currentTable = null;
        currentPartitionIndex = -1;
        cachedTxnState = null;
        cachedMetaState = null;
    }

    private void cdUp() {
        if (currentPartitionIndex >= 0) {
            currentPartitionIndex = -1;
        } else if (currentTable != null) {
            cdRoot();
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
        if (cachedTxnState == null || partitionIndex < 0 || partitionIndex >= cachedTxnState.getPartitions().size()) {
            return "?";
        }
        int partitionBy = cachedMetaState != null ? cachedMetaState.getPartitionBy() : TxnState.UNSET_INT;
        int timestampType = cachedMetaState != null ? cachedMetaState.getTimestampColumnType() : io.questdb.cairo.ColumnType.TIMESTAMP;
        long timestampLo = cachedTxnState.getPartitions().getQuick(partitionIndex).getTimestampLo();
        return ConsoleRenderer.formatPartitionName(partitionBy, timestampType, timestampLo);
    }

    private void ls(PrintStream out, PrintStream err) {
        if (currentTable == null) {
            lastDiscoveredTables = tableDiscoveryService.discoverTables(dbRoot);
            renderer.printTables(lastDiscoveredTables, out);
        } else if (currentPartitionIndex < 0) {
            if (cachedTxnState == null) {
                err.println("no txn state cached for current table");
                return;
            }
            renderer.printPartitions(cachedTxnState, cachedMetaState, out);
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
