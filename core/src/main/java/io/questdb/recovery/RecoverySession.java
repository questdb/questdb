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
    private final CharSequence dbRoot;
    private final TableDiscoveryService tableDiscoveryService;
    private final TxnStateService txnStateService;
    private final ConsoleRenderer renderer;
    private ObjList<DiscoveredTable> lastDiscoveredTables = new ObjList<>();

    public RecoverySession(
            CharSequence dbRoot,
            TableDiscoveryService tableDiscoveryService,
            TxnStateService txnStateService,
            ConsoleRenderer renderer
    ) {
        this.dbRoot = dbRoot;
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
            out.print("recover> ");
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

    private void show(String target, PrintStream out, PrintStream err) {
        if (target.isEmpty()) {
            err.println("show requires a table name or index");
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
}
