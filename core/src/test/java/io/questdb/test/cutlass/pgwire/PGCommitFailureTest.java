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

package io.questdb.test.cutlass.pgwire;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.wal.WalUtils.EVENT_INDEX_FILE_NAME;

public class PGCommitFailureTest extends BasePGTest {

    @Test
    public void testExplicitCommitFailure() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
            execute("create table x (a int, t timestamp) timestamp(t) partition by hour wal");
            FilesFacade ffTmp = ff;
            try {
                AtomicInteger counter = new AtomicInteger(2);
                ff = new FilesFacadeImpl() {
                    long addr = 0;
                    long fd = 0;

                    @Override
                    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                        final long addr = super.mmap(fd, len, offset, flags, memoryTag);
                        if (fd == this.fd) {
                            this.addr = addr;
                        }
                        return addr;
                    }

                    @Override
                    public void msync(long addr, long len, boolean async) {
                        if ((addr == this.addr) && (counter.decrementAndGet() == 0)) {
                            throw CairoException.critical(errno()).put("could not append WAL event index value");
                        }
                        super.msync(addr, len, async);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.endsWithAscii(name, Files.SEPARATOR + EVENT_INDEX_FILE_NAME)
                                && Utf8s.containsAscii(name, Files.SEPARATOR + "x~")) {
                            this.fd = fd;
                        }
                        return fd;
                    }
                };
                connection.setAutoCommit(false);
                connection.prepareStatement("insert into x values (1, '2021-01-01T00:00:00.000Z')").execute();
                try {
                    connection.commit();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "could not append WAL event index value");
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
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
            execute("create table x (a int, t timestamp) timestamp(t) partition by hour wal");
            FilesFacade ffTmp = ff;
            try {
                AtomicInteger counter = new AtomicInteger(2);
                ff = new FilesFacadeImpl() {
                    long addr = 0;
                    long fd = 0;

                    @Override
                    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                        final long addr = super.mmap(fd, len, offset, flags, memoryTag);
                        if (fd == this.fd) {
                            this.addr = addr;
                        }
                        return addr;
                    }

                    @Override
                    public void msync(long addr, long len, boolean async) {
                        if ((addr == this.addr) && (counter.decrementAndGet() == 0)) {
                            throw CairoException.critical(errno()).put("could not append WAL event index value");
                        }
                        super.msync(addr, len, async);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.endsWithAscii(name, Files.SEPARATOR + EVENT_INDEX_FILE_NAME)
                                && Utf8s.containsAscii(name, Files.SEPARATOR + "x~")) {
                            this.fd = fd;
                        }
                        return fd;
                    }
                };
                try {
                    connection.prepareStatement("insert into x values (1, '2021-01-01T00:00:00.000Z')").execute();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "could not append WAL event index value");
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
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
            execute("create table x (a int, t timestamp) timestamp(t) partition by hour wal");
            FilesFacade ffTmp = ff;
            try {
                AtomicInteger counter = new AtomicInteger(2);
                ff = new FilesFacadeImpl() {
                    long addr = 0;
                    long fd = 0;

                    @Override
                    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                        final long addr = super.mmap(fd, len, offset, flags, memoryTag);
                        if (fd == this.fd) {
                            this.addr = addr;
                        }
                        return addr;
                    }

                    @Override
                    public void msync(long addr, long len, boolean async) {
                        if ((addr == this.addr) && (counter.decrementAndGet() == 0)) {
                            throw CairoException.critical(errno()).put("could not append WAL event index value");
                        }
                        super.msync(addr, len, async);
                    }

                    @Override
                    public long openRW(LPSZ name, int opts) {
                        long fd = super.openRW(name, opts);
                        if (Utf8s.endsWithAscii(name, Files.SEPARATOR + EVENT_INDEX_FILE_NAME)
                                && Utf8s.containsAscii(name, Files.SEPARATOR + "x~")) {
                            this.fd = fd;
                        }
                        return fd;
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
                    TestUtils.assertContains(e.getMessage(), "could not append WAL event index value");
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
