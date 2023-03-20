/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cutlass.line.tcp.load.TableData;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;

public class LineTcpCommitFuzzTest extends AbstractLineTcpReceiverFuzzTest {

    private static final Log LOG = LogFactory.getLog(LineTcpCommitFuzzTest.class);

    public LineTcpCommitFuzzTest(WalMode walMode) {
        super(walMode);
    }

    @Test
    public void testCommitIntervalBasedDefaultFractionZero() throws Exception {

        // rows based commit every 22 rows -> will commit 88 rows per table only -> test would timeout
        configOverrideMaxUncommittedRows(22);

        // idle table commit after 5 mins inactivity -> test would timeout
        maintenanceInterval = 300_000_000;
        minIdleMsBeforeWriterRelease = 300_000_000;

        // time based commit every 0.5 seconds (default interval) -> should commit last 12 rows per table -> make test pass
        configOverrideO3MinLag(1_000_000);
        commitIntervalFraction = 0.0;
        commitIntervalDefault = 500;

        initLoadParameters(20, 5, 2, 2, 50, true);

        runTest();
    }

    @Test
    public void testCommitIntervalBasedDefaultLagZero() throws Exception {
        // rows based commit every 22 rows -> will commit 88 rows per table only -> test would timeout
        configOverrideMaxUncommittedRows(22);

        // idle table commit after 5 mins inactivity -> test would timeout
        maintenanceInterval = 300_000_000;
        minIdleMsBeforeWriterRelease = 300_000_000;

        // time based commit every 0.5 seconds (default interval) -> should commit last 12 rows per table -> make test pass
        configOverrideO3MinLag(0);
        commitIntervalFraction = 0.2;
        commitIntervalDefault = 500;

        initLoadParameters(20, 5, 2, 2, 50, true);

        runTest();
    }

    @Test
    public void testCommitIntervalBasedFraction() throws Exception {
        // rows based commit every 110 rows -> will never happen, we ingest only 100 rows per table -> test would timeout
        configOverrideMaxUncommittedRows(110);

        // idle table commit after 5 mins inactivity -> test would timeout
        maintenanceInterval = 300_000_000;
        minIdleMsBeforeWriterRelease = 300_000_000;

        // time based commit every 0.5 seconds (50% of 1 sec commit lag) -> should commit rows -> make test pass
        configOverrideO3MinLag(1_000_000);
        commitIntervalFraction = 0.5;
        commitIntervalDefault = 300_000;

        initLoadParameters(20, 5, 2, 2, 50, true);

        runTest();
    }

    @Test
    public void testCommitIntervalBasedFractionConstantLowRate() throws Exception {

        configOverrideMaxUncommittedRows(500_000);

        // idle table commit after 5 mins inactivity -> test would timeout
        maintenanceInterval = 300_000_000;
        minIdleMsBeforeWriterRelease = 300_000_000;

        // time based commit every 0.2 seconds (20% of 1 sec commit lag) -> should commit rows -> make test pass
        configOverrideO3MinLag(1_000_000);
        commitIntervalFraction = 0.2;

        initLoadParameters(10, 100, 1, 1, 10, false);
        runTest();

        for (CharSequence table : tableNames.keySet()) {
            try (TableWriter tw = getWriter(table)) {
                if (walEnabled) {
                    MatcherAssert.assertThat(tw.getSeqTxn(), greaterThan(4L));
                } else {
                    MatcherAssert.assertThat(tw.getTxn(), greaterThan(4L));
                }
            }
        }
    }

    @Test
    public void testCommitNumOfRowsBased() throws Exception {
        // rows based commit every 10 rows -> will commit 10 times 10 rows per table -> make test pass
        // WAL data is split against multiple WAL writers, so WAL table have to commit every row
        configOverrideMaxUncommittedRows(walEnabled ? 1 : 10);

        // idle table commit after 5 mins inactivity -> test would timeout
        maintenanceInterval = 300_000_000;
        minIdleMsBeforeWriterRelease = 300_000_000;

        // time based commit every 5 mins (default interval) -> test would timeout
        configOverrideO3MinLag(0);
        commitIntervalFraction = 0.2;
        commitIntervalDefault = 300_000;

        initLoadParameters(20, 5, 1, 1, 50, true);

        runTest();
    }

    @Test
    public void testCommitTableReleased() throws Exception {
        // rows based commit every 22 rows -> will commit 88 rows per table only -> test would timeout
        configOverrideMaxUncommittedRows(22);

        // idle table commit after 0.5 seconds inactivity -> should commit last 12 rows per table -> make test pass
        maintenanceInterval = 200;
        minIdleMsBeforeWriterRelease = 500;

        // time based commit every 5 mins (default interval) -> test would timeout
        configOverrideO3MinLag(0);
        commitIntervalFraction = 0.2;
        commitIntervalDefault = 300_000;

        initLoadParameters(20, 5, 2, 2, 50, true);

        runTest((factoryType, thread, token, event, segment, position) -> {
            if (walEnabled) {
                // There is no locking as such in WAL, so we treat writer return as an unlock event.
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                    handleWriterUnlockEvent(token.getTableName());
                }
            } else {
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_UNLOCKED) {
                    handleWriterUnlockEvent(token.getTableName());
                }
            }
        }, minIdleMsBeforeWriterRelease);
    }

    @Override
    protected Log getLog() {
        return LOG;
    }

    void handleWriterReturnEvent(CharSequence name) {
        // ApplyWal2TableJob releases TableWriter
        if (!walEnabled) {
            setError("Table writer is not expected to be released, maintenanceInterval and minIdleMsBeforeWriterRelease are set very high");
        }
    }

    @Override
    void handleWriterUnlockEvent(CharSequence name) {
        super.handleWriterUnlockEvent(name);

        // set the table ready right after created
        // instead of the 'ready' latch we will rely on the timeout in assertTable(table)
        final TableData table = tables.get(name);
        table.returnPermit();
    }
}
