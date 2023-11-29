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

package io.questdb.test;

import io.questdb.Bootstrap;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

public class TestServerMain extends ServerMain {
    private SqlExecutionContext sqlExecutionContext;

    public TestServerMain(String... args) {
        super(args);
    }

    public TestServerMain(final Bootstrap bootstrap) {
        super(bootstrap);
    }

    public static TestServerMain createWithManualWalRun(String... args) {
        return new TestServerMain(args) {
            @Override
            protected void setupWalApplyJob(
                    WorkerPool workerPool,
                    CairoEngine engine,
                    int sharedWorkerCount
            ) {
            }
        };
    }

    public void assertSql(String sql, String expected) {
        try {
            if (sqlExecutionContext == null) {
                sqlExecutionContext = new SqlExecutionContextImpl(getEngine(), 1).with(
                        getEngine().getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
            }
            TestUtils.assertSql(getEngine(), sqlExecutionContext, sql, Misc.getThreadLocalSink(), expected);
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    public void waitWalTxnApplied(String tableName, long expectedTxn) {
        int waitLim = 15;
        int sleep = 10;
        CairoEngine engine = getEngine();

        TableToken tableToken = null;
        for (int i = 0; i < waitLim; i++) {
            if (tableToken == null) {
                try {
                    tableToken = engine.verifyTableName(tableName);
                } catch (CairoException ex) {
                    // wait or error after timeout
                    if (i < waitLim - 1) {
                        Os.sleep(sleep *= 2);
                    } else {
                        throw ex;
                    }
                }
            }

            if (tableToken != null) {
                long seqTxn = expectedTxn > -1 ? expectedTxn : engine.getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn();
                long writerTxn = engine.getTableSequencerAPI().getTxnTracker(tableToken).getWriterTxn();
                if (seqTxn <= writerTxn) {
                    return;
                }

                boolean isSuspended = engine.getTableSequencerAPI().isSuspended(tableToken);
                if (isSuspended) {
                    Assert.fail("Table " + tableName + " is suspended");
                }

                if (i < waitLim - 1) {
                    Os.sleep(sleep *= 2);
                } else {
                    Assert.assertEquals("Writer Txn is not up to date with Seq Txn", seqTxn, writerTxn);
                }
            }
        }
        assert false;
    }
}
