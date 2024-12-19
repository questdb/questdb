/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.pgwire;

import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class PGCommitFailureTest extends BasePGTest {

    public PGCommitFailureTest(@NonNull LegacyMode legacyMode) {
        super(legacyMode);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return legacyModeParams();
    }

    @Test
    public void testExplicitCommitFailure() throws Exception {
        skipInLegacyMode();
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table x (a int, t timestamp) timestamp(t) partition by hour wal");
            FilesFacade ffTmp = ff;
            try {
                AtomicInteger counter = new AtomicInteger(2);
                ff = new FilesFacadeImpl() {
                    @Override
                    public long append(long fd, long buf, long len) {
                        if (counter.decrementAndGet() == 0) {
                            return -1;
                        }
                        return super.append(fd, buf, len);
                    }
                };
                connection.setAutoCommit(false);
                connection.prepareStatement("insert into x values (1, '2021-01-01T00:00:00.000Z')").execute();
                try {
                    connection.commit();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "could not append WAL invent index value");
                }
            } finally {
                ff = ffTmp;
            }
            TestUtils.drainWalQueue(engine);
            assertSql(
                    "count\n" +
                            "0\n",
                    "select count() from x"
            );
        });
    }

    @Test
    public void testImplicitCommitFailure() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table x (a int, t timestamp) timestamp(t) partition by hour wal");
            FilesFacade ffTmp = ff;
            try {
                AtomicInteger counter = new AtomicInteger(2);
                ff = new FilesFacadeImpl() {
                    @Override
                    public long append(long fd, long buf, long len) {
                        if (counter.decrementAndGet() == 0) {
                            return -1;
                        }
                        return super.append(fd, buf, len);
                    }
                };
                try {
                    connection.prepareStatement("insert into x values (1, '2021-01-01T00:00:00.000Z')").execute();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "could not append WAL invent index value");
                }
            } finally {
                ff = ffTmp;
            }
            TestUtils.drainWalQueue(engine);
            assertSql(
                    "count\n" +
                            "0\n",
                    "select count() from x"
            );
        });
    }

    @Test
    public void testImplicitPipelineCommitFailure() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            execute("create table x (a int, t timestamp) timestamp(t) partition by hour wal");
            FilesFacade ffTmp = ff;
            try {
                AtomicInteger counter = new AtomicInteger(2);
                ff = new FilesFacadeImpl() {
                    @Override
                    public long append(long fd, long buf, long len) {
                        if (counter.decrementAndGet() == 0) {
                            return -1;
                        }
                        return super.append(fd, buf, len);
                    }
                };

                Statement stmt = connection.createStatement();
                try {
                    stmt.execute(
                            "insert into x values (1, '2021-01-01T00:00:00.000Z');" +
                                    "insert into x values (2, '2021-01-01T00:00:00.000Z');" +
                                    "select * from x;"
                    );
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "could not append WAL invent index value");
                }
            } finally {
                ff = ffTmp;
            }
            TestUtils.drainWalQueue(engine);
            assertSql(
                    "count\n" +
                            "0\n",
                    "select count() from x"
            );
        });
    }
}
