/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cutlass.line.tcp.load.TableData;
import io.questdb.std.Os;
import org.junit.Test;

public class LineTcpCommitTimeoutTest extends AbstractLineTcpReceiverFuzzTest {

    @Test
    public void testCommitTimeout() throws Exception {
        AbstractCairoTest.configOverrideMaxUncommittedRows = 22;
        maintenanceInterval = 3000000;
        commitTimeout = 300;

        initLoadParameters(5, 5, 3, 3, 50, true);

        runTest();
    }

    void handleWriterReturnEvent(CharSequence name) {
        // with the settings above we are ingesting 25 rows into each table
        // we expect a commit after 22 rows and the last 3 rows should be committed 300 ms after ingestion stops
        // we do not expect commit by the maintenance job at all as maintenanceInterval is set much higher than commitTimeout
        setError("Table writer is not expected to be released, maintenanceInterval is set higher than timeoutInterval");
    }

    void assertTable(TableData table) {
        // timeout is 60 seconds, we try it every second
        for (int i = 0; i < 60; i++) {
            if (checkTable(table)) {
                return;
            }
            Os.sleep(1000);
        }
        setError("Timed out on waiting for the data to be committed");
    }
}
