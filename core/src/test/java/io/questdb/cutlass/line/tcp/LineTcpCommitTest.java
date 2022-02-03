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

import io.questdb.cairo.pool.PoolListener;
import io.questdb.cutlass.line.tcp.load.TableData;
import io.questdb.std.Os;
import org.junit.Test;

public class LineTcpCommitTest extends AbstractLineTcpReceiverFuzzTest {

    @Test
    public void testCommitIntervalBasedFraction() throws Exception {
        // rows based commit every 110 rows -> will never happen, we ingest only 100 rows per table -> test would timeout
        configOverrideMaxUncommittedRows = 110;

        // idle table commit after 30 seconds inactivity -> test would timeout
        maintenanceInterval = 30_000_000;
        minIdleMsBeforeWriterRelease = 30_000_000;

        // time based commit every 0.5 seconds (50% of 1 sec commit lag) -> should commit rows and make test pass
        configOverrideCommitLagMicros = 1_000_000;
        commitIntervalFraction = 0.5;
        commitIntervalDefault = 30_000_000;

        initLoadParameters(2000, 5, 5, 5, 50, true);

        runTest();
    }

    @Test
    public void testCommitIntervalBasedDefaultLagZero() throws Exception {
        // rows based commit every 22 rows -> will commit 88 rows per table only, test would timeout
        configOverrideMaxUncommittedRows = 22;

        // idle table commit after 30 seconds inactivity -> test would timeout
        maintenanceInterval = 30_000_000;
        minIdleMsBeforeWriterRelease = 30_000_000;

        // time based commit every 0.5 seconds (default interval) -> should commit last 12 rows per table and make test pass
        configOverrideCommitLagMicros = 0;
        commitIntervalFraction = 0.2;
        commitIntervalDefault = 500;

        initLoadParameters(20, 5, 2, 2, 50, true);

        runTest();
    }

    @Test
    public void testCommitIntervalBasedDefaultFractionZero() throws Exception {
        // rows based commit every 22 rows -> will commit 88 rows per table only, test would timeout
        configOverrideMaxUncommittedRows = 22;

        // idle table commit after 30 seconds inactivity -> test would timeout
        maintenanceInterval = 30_000_000;
        minIdleMsBeforeWriterRelease = 30_000_000;

        // time based commit every 0.5 seconds (default interval) -> should commit last 12 rows per table and make test pass
        configOverrideCommitLagMicros = 1_000_000;
        commitIntervalFraction = 0.0;
        commitIntervalDefault = 500;

        initLoadParameters(20, 5, 2, 2, 50, true);

        runTest();
    }

    @Test
    public void testCommitNumOfRowsBased() throws Exception {
        // rows based commit every 10 rows -> will commit 10 times 10 rows per table -> should make test pass
        configOverrideMaxUncommittedRows = 10;

        // idle table commit after 30 seconds inactivity -> test would timeout
        maintenanceInterval = 30_000_000;
        minIdleMsBeforeWriterRelease = 30_000_000;

        // time based commit every 30 seconds (default interval) -> test would timeout
        configOverrideCommitLagMicros = 0;
        commitIntervalFraction = 0.2;
        commitIntervalDefault = 30_000_000;

        initLoadParameters(20, 5, 2, 2, 50, true);

        runTest();
    }

    @Test
    public void testCommitTableReleased() throws Exception {
        // rows based commit every 22 rows -> will commit 88 rows per table only, test would timeout
        configOverrideMaxUncommittedRows = 22;

        // idle table commit after 0.5 seconds inactivity -> should commit last 12 rows per table and make test pass
        maintenanceInterval = 200;
        minIdleMsBeforeWriterRelease = 500;

        // time based commit every 30 seconds (default interval) -> test would timeout
        configOverrideCommitLagMicros = 0;
        commitIntervalFraction = 0.2;
        commitIntervalDefault = 30_000_000;

        initLoadParameters(20, 5, 2, 2, 50, true);

        runTest((factoryType, thread, name, event, segment, position) -> {
            if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_LOCK_SUCCESS) {
                handleWriterLockSuccessEvent(name);
            }
        }, minIdleMsBeforeWriterRelease);
    }

    void handleWriterReturnEvent(CharSequence name) {
        setError("Table writer is not expected to be released, maintenanceInterval and minIdleMsBeforeWriterRelease are set very high");
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
