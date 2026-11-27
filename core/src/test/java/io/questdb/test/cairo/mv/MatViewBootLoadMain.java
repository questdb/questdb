/*+*****************************************************************************
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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.mv.MatViewState;

/**
 * Boots an engine on the data directory of the first argument, as a server starts, and prints the pending definition
 * seqTxn of each materialized view the other arguments name, see
 * {@link AlterMatViewAsTest#testBootLoadWithoutAssertionsRecordsPendingDefinition()}. The test runs it in a JVM without
 * {@code -ea}, as the Windows service starts QuestDB, and it exits with 3 when assertions are enabled.
 */
public final class MatViewBootLoadMain {

    private MatViewBootLoadMain() {
    }

    public static void main(String[] args) {
        if (CairoEngine.class.desiredAssertionStatus()) {
            System.out.println("assertions are enabled");
            System.exit(3);
        }
        // the test's engine keeps the table name registry open, which its own configuration allows as well
        final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(args[0]) {
            @Override
            public boolean getAllowTableRegistrySharedWrite() {
                return true;
            }
        };
        try (CairoEngine engine = new CairoEngine(configuration)) {
            engine.load();
            engine.buildViewGraphs();
            for (int i = 1; i < args.length; i++) {
                final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName(args[i]));
                System.out.println("pending definition " + args[i] + '=' + state.getPendingDefinitionSeqTxn());
            }
        } catch (Throwable th) {
            th.printStackTrace(System.out);
            System.exit(2);
        }
        System.exit(0);
    }
}
