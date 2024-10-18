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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class MetadataCacheTest extends AbstractCairoTest {
    private static final String xMetaString = "MetadataCache [tableCount=1]\n" +
            "\tCairoTable [name=x, id=1, directoryName=x~, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=NONE, timestampIndex=3, timestampName=timestamp, walEnabled=false, columnCount=16]\n" +
            "\t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
            "\t\tCairoColumn [name=sym, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n" +
            "\t\tCairoColumn [name=amt, position=2, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=2]\n" +
            "\t\tCairoColumn [name=timestamp, position=3, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=3]\n" +
            "\t\tCairoColumn [name=b, position=4, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=4]\n" +
            "\t\tCairoColumn [name=c, position=5, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=5]\n" +
            "\t\tCairoColumn [name=d, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=6]\n" +
            "\t\tCairoColumn [name=e, position=7, type=FLOAT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=7]\n" +
            "\t\tCairoColumn [name=f, position=8, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=8]\n" +
            "\t\tCairoColumn [name=g, position=9, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=9]\n" +
            "\t\tCairoColumn [name=ik, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=0, writerIndex=10]\n" +
            "\t\tCairoColumn [name=j, position=11, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=11]\n" +
            "\t\tCairoColumn [name=k, position=12, type=TIMESTAMP, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=12]\n" +
            "\t\tCairoColumn [name=l, position=13, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=13]\n" +
            "\t\tCairoColumn [name=m, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=14]\n" +
            "\t\tCairoColumn [name=n, position=15, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=15]\n";
    private static final String yMetaString = "MetadataCache [tableCount=1]\n" +
            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=1]\n" +
            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n";
    private static final String zMetaString = "MetadataCache [tableCount=1]\n" +
            "\tCairoTable [name=z, id=1, directoryName=z~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=3, timestampName=timestamp, walEnabled=true, columnCount=16]\n" +
            "\t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
            "\t\tCairoColumn [name=sym, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n" +
            "\t\tCairoColumn [name=amt, position=2, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=2]\n" +
            "\t\tCairoColumn [name=timestamp, position=3, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=3]\n" +
            "\t\tCairoColumn [name=b, position=4, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=4]\n" +
            "\t\tCairoColumn [name=c, position=5, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=5]\n" +
            "\t\tCairoColumn [name=d, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=6]\n" +
            "\t\tCairoColumn [name=e, position=7, type=FLOAT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=7]\n" +
            "\t\tCairoColumn [name=f, position=8, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=8]\n" +
            "\t\tCairoColumn [name=g, position=9, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=9]\n" +
            "\t\tCairoColumn [name=ik, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=0, writerIndex=10]\n" +
            "\t\tCairoColumn [name=j, position=11, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=11]\n" +
            "\t\tCairoColumn [name=k, position=12, type=TIMESTAMP, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=12]\n" +
            "\t\tCairoColumn [name=l, position=13, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=13]\n" +
            "\t\tCairoColumn [name=m, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=14]\n" +
            "\t\tCairoColumn [name=n, position=15, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=15]\n";

    @Test
    public void fuzzConcurrentCreatesAndDrops() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger creatorInteger = new AtomicInteger();
            AtomicInteger dropperInteger = new AtomicInteger();

            Thread creatingThread = new Thread(() -> {
                try {
                    fuzzConcurrentCreatesAndDropsCreatorThread(creatorInteger);
                } catch (Throwable ignore) {
                }
            });

            Thread droppingThread = new Thread(() -> {
                try {
                    fuzzConcurrentCreatesAndDropsDropperThread(dropperInteger);
                } catch (Throwable ignore) {
                }
            });

            creatingThread.start();
            droppingThread.start();

            Os.sleep(2_000);

            creatingThread.interrupt();
            droppingThread.interrupt();

            creatingThread.join();
            droppingThread.join();

            int creatorCounter = creatorInteger.get();
            int dropperCounter = dropperInteger.get();

            LOG.infoW().$("[creator=").$(creatorCounter).$(", dropper=").$(dropperCounter).I$();

            drainWalQueue();

            LOG.infoW().$("unpublished_wal_txn_count:").$(engine.getUnpublishedWalTxnCount()).$();

            TableToken tableToken;
            CairoTable cairoTable;
            tableToken = engine.getTableTokenIfExists("foo");

            if (tableToken != null) {
                try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                    cairoTable = metadataRO.getTable(tableToken);
                }

                if (engine.isTableDropped(tableToken)) {
                    Assert.assertNull(cairoTable);
                } else {
                    Assert.assertNotNull(cairoTable);
                }
            }

        });
    }

    @SuppressWarnings("BusyWait")
    @Test
    public void fuzzRenamesOnlyOneTablePresentAtATime() throws Exception {
        assertMemoryLeak(() -> {

            ddl("create table foo ( ts timestamp, x int ) timestamp(ts) partition by day wal;");
            AtomicReference<Throwable> exception = new AtomicReference<>();

            Thread fooToBahThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.ddl("rename table foo to bah", sqlExecutionContext);
                        assertSql("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                                        "x\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n", "show columns from bah",
                                engine, sqlExecutionContext, new StringSink());
                    }
                } catch (SqlException | CairoException ignore) {
                } catch (Throwable e) {
                    e.printStackTrace();
                    exception.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            });

            Thread bahToFooThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.ddl("rename table bah to foo", sqlExecutionContext);
                        assertSql("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                                        "x\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n", "show columns from foo",
                                engine, sqlExecutionContext, new StringSink()
                        );
                    }
                } catch (SqlException | CairoException ignore) {
                } catch (Throwable e) {
                    e.printStackTrace();
                    exception.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            });

            fooToBahThread.start();
            bahToFooThread.start();

            Instant i = Instant.now();

            String s;
            StringSink ss = new StringSink();
            // should only ever contain one or the other
            // check that `tables()` gives consistent view
            while (Instant.now().getEpochSecond() - i.getEpochSecond() < 2) {
                s = dumpTables(ss);
                Assert.assertTrue(
                        TestUtils.dumpMetadataCache(engine),
                        s.contains("foo\t") ^ s.contains("bah\t"));
                Thread.sleep(50);
            }

            ss.clear();

            fooToBahThread.interrupt();
            bahToFooThread.interrupt();

            fooToBahThread.join();
            bahToFooThread.join();

            if (exception.get() != null) {
                throw new RuntimeException(exception.get());
            }

            try {
                drainWalQueue();
            } catch (TableReferenceOutOfDateException e) {
                LOG.infoW().$(e).$();
            }

            TableToken fooToken = engine.getTableTokenIfExists("foo");
            TableToken bahToken = engine.getTableTokenIfExists("bah");

            // one should be null
            Assert.assertNotSame(fooToken, bahToken);

            String cacheString = TestUtils.dumpMetadataCache(engine);

            if (fooToken == null) {
                Assert.assertFalse(cacheString.contains("name=foo"));
                Assert.assertTrue(cacheString.contains("name=bah"));
                assertQueryNoLeakCheck("id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\n" +
                        "1\tbah\tts\tDAY\t1000\t300000000\ttrue\tfoo~1\tfalse\n", "tables()", "");
            }
            if (bahToken == null) {
                Assert.assertFalse(cacheString.contains("name=bah"));
                Assert.assertTrue(cacheString.contains("name=foo"));
                assertQueryNoLeakCheck("id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\n" +
                        "1\tfoo\tts\tDAY\t1000\t300000000\ttrue\tfoo~1\tfalse\n", "tables()", "");
            }
        });
    }

    @Test
    public void fuzzRenamesWithConcurrentAlters() throws Exception {

        assertMemoryLeak(() -> {
            ddl("create table foo ( ts timestamp, x int ) timestamp(ts) partition by day wal;");
            AtomicReference<Throwable> exception = new AtomicReference<>();

            Thread fooToBahThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.ddl("rename table foo to bah", sqlExecutionContext);
                        assertExceptionNoLeakCheck("show columns from foo", 18, "table does not exist", false, sqlExecutionContext);
                        assertSql(
                                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                                        "x\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n",
                                "show columns from bah",
                                engine,
                                sqlExecutionContext,
                                new StringSink()
                        );
                    }
                } catch (InterruptedException | SqlException | CairoException ignore) {
                } catch (Throwable e) {
                    e.printStackTrace();
                    exception.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            });

            Thread bahToFooThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.ddl("rename table bah to foo", sqlExecutionContext);
                        assertException("show columns from bah", 18, "table does not exist", sqlExecutionContext);
                        assertSql("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                                        "x\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n", "show columns from foo",
                                engine,
                                sqlExecutionContext,
                                new StringSink()
                        );
                    }
                } catch (InterruptedException | SqlException | CairoException ignore) {
                } catch (Throwable e) {
                    e.printStackTrace();
                    exception.set(e);
                } finally {
                    Path.clearThreadLocals();
                }

            });

            Thread adderThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.ddl("alter table foo add column y symbol", sqlExecutionContext);
                    }
                } catch (SqlException | CairoException ignored) {
                } catch (Throwable e) {
                    e.printStackTrace();
                    exception.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            });

            fooToBahThread.start();
            bahToFooThread.start();
            adderThread.start();

            Thread.sleep(2000);

            fooToBahThread.interrupt();
            bahToFooThread.interrupt();
            adderThread.interrupt();

            fooToBahThread.join();
            bahToFooThread.join();
            adderThread.join();

            if (exception.get() != null) {
                throw new RuntimeException(exception.get());
            }

            try {
                drainWalQueue();
            } catch (TableReferenceOutOfDateException e) {
                LOG.infoW().$(e).$();
            }

            TableToken fooToken = engine.getTableTokenIfExists("foo");
            TableToken bahToken = engine.getTableTokenIfExists("bah");

            // one should be null
            Assert.assertNotSame(fooToken, bahToken);

            String cacheString = TestUtils.dumpMetadataCache(engine);

            if (fooToken == null) {
                Assert.assertFalse(cacheString.contains("name=foo"));
                Assert.assertTrue(cacheString.contains("name=bah"));
            }
            if (bahToken == null) {
                Assert.assertFalse(cacheString.contains("name=bah"));
                Assert.assertTrue(cacheString.contains("name=foo"));
            }
        });
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            createY();

            assertCairoMetadata(yMetaString);


            ddl("ALTER TABLE y ADD COLUMN foo VARCHAR");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=256, writerIndex=1]\n");

            ddl("ALTER TABLE y ADD COLUMN bah SYMBOL");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=3]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=256, writerIndex=1]\n" +
                    "\t\tCairoColumn [name=bah, position=2, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=256, writerIndex=2]\n");
        });
    }

    @Test
    public void testAlterTableColumnAddIndex() throws Exception {
        assertMemoryLeak(() -> {

            ddl("CREATE TABLE y (ts TIMESTAMP, foo SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=256, writerIndex=1]\n");

            ddl("ALTER TABLE y ALTER COLUMN foo ADD INDEX");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=true, indexBlockCapacity=256, writerIndex=1]\n");

        });
    }

    @Test
    public void testAlterTableColumnCacheNocache() throws Exception {
        assertMemoryLeak(() -> {


            ddl("CREATE TABLE y ( ts TIMESTAMP, x SYMBOL ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=256, writerIndex=1]\n");

            ddl("ALTER TABLE y ALTER COLUMN x NOCACHE");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=128, isIndexed=false, indexBlockCapacity=256, writerIndex=1]\n");

            ddl("ALTER TABLE y ALTER COLUMN x CACHE");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=256, writerIndex=1]\n");

        });
    }

    @Test
    public void testAlterTableColumnDropIndex() throws Exception {
        assertMemoryLeak(() -> {


            ddl("CREATE TABLE y (ts TIMESTAMP, foo SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=true, indexBlockCapacity=256, writerIndex=1]\n");

            ddl("ALTER TABLE y ALTER COLUMN foo DROP INDEX");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=256, writerIndex=1]\n");

        });
    }

    @Test
    public void testAlterTableColumnType() throws Exception {
        assertMemoryLeak(() -> {

            ddl("CREATE TABLE y (ts TIMESTAMP, foo VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");

            ddl("ALTER TABLE y ALTER COLUMN foo TYPE SYMBOL");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=256, writerIndex=1]\n");
        });
    }

    @Test
    public void testAlterTableDedupDisable() throws Exception {
        assertMemoryLeak(() -> {


            ddl("CREATE TABLE y ( ts TIMESTAMP, foo INT ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, foo)");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=true, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=INT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");

            ddl("ALTER TABLE y DEDUP DISABLE");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");
        });
    }

    @Test
    public void testAlterTableDedupEnable() throws Exception {
        assertMemoryLeak(() -> {


            createZ();
            drainWalQueue();

            assertCairoMetadata(zMetaString);

            ddl("ALTER TABLE z DEDUP ENABLE UPSERT KEYS(timestamp, i, e, k)");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=z, id=1, directoryName=z~1, isDedup=true, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=3, timestampName=timestamp, walEnabled=true, columnCount=16]\n" +
                    "\t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=sym, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n" +
                    "\t\tCairoColumn [name=amt, position=2, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=2]\n" +
                    "\t\tCairoColumn [name=timestamp, position=3, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=3]\n" +
                    "\t\tCairoColumn [name=b, position=4, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=4]\n" +
                    "\t\tCairoColumn [name=c, position=5, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=5]\n" +
                    "\t\tCairoColumn [name=d, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=6]\n" +
                    "\t\tCairoColumn [name=e, position=7, type=FLOAT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=7]\n" +
                    "\t\tCairoColumn [name=f, position=8, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=8]\n" +
                    "\t\tCairoColumn [name=g, position=9, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=9]\n" +
                    "\t\tCairoColumn [name=ik, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, isIndexed=false, indexBlockCapacity=0, writerIndex=10]\n" +
                    "\t\tCairoColumn [name=j, position=11, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=11]\n" +
                    "\t\tCairoColumn [name=k, position=12, type=TIMESTAMP, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=12]\n" +
                    "\t\tCairoColumn [name=l, position=13, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=13]\n" +
                    "\t\tCairoColumn [name=m, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=14]\n" +
                    "\t\tCairoColumn [name=n, position=15, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=15]\n");
        });
    }

    @Test
    public void testAlterTableDropColumn() throws Exception {
        assertMemoryLeak(() -> {


            ddl("CREATE TABLE y ( ts TIMESTAMP, foo INT ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, foo)");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=true, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=1, type=INT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");

            ddl("ALTER TABLE y DROP COLUMN foo");
            drainWalQueue();


            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=true, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=1]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n");
        });
    }

    @Test
    public void testAlterTableRenameColumn() throws Exception {
        assertMemoryLeak(() -> {


            ddl("CREATE TABLE y ( ts TIMESTAMP, x INT ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");

            ddl("ALTER TABLE y RENAME COLUMN x TO x2");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x2, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");
        });
    }

    @Test
    public void testAlterTableSetParam() throws Exception {
        assertMemoryLeak(() -> {


            ddl("CREATE TABLE y ( ts TIMESTAMP, x INT ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");

            ddl("ALTER TABLE y SET PARAM maxUncommittedRows = 42");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=42, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");

            ddl("ALTER TABLE y SET PARAM o3MaxLag = 42s");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=42, o3MaxLag=42000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");

        });
    }

    @Test
    public void testBasicMetadata() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            assertCairoMetadata(xMetaString);
        });
    }

    @Test
    public void testDropTable() throws Exception {
        assertMemoryLeak(() -> {


            ddl("CREATE TABLE y ( ts TIMESTAMP, x INT ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");

            drop("DROP TABLE y");
            drainWalQueue();

            assertException("table_columns('y')", -1, "table does not exist");
        });
    }

    @Test
    public void testMetadataUpdatedCorrectlyWhenRenamingTables() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table foo ( ts timestamp, x int) timestamp(ts) partition by day wal;");
            assertSql("id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\n" +
                    "1\tfoo\tts\tDAY\t1000\t300000000\ttrue\tfoo~1\tfalse\n", "tables()");

            ddl("rename table foo to bah");
            drainWalQueue();
            assertSql("id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\n" +
                    "1\tbah\tts\tDAY\t1000\t300000000\ttrue\tfoo~1\tfalse\n", "tables()");
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        assertMemoryLeak(() -> {

            createY();

            assertCairoMetadata(yMetaString);

            ddl("RENAME TABLE y TO y2");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y2, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=1]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n");

        });
    }

    @Test
    public void testTruncateTable() throws Exception {
        assertMemoryLeak(() -> {


            ddl("CREATE TABLE y ( ts TIMESTAMP, x INT ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");

            ddl("TRUNCATE TABLE y");
            drainWalQueue();

            assertCairoMetadata("MetadataCache [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, isIndexed=false, indexBlockCapacity=0, writerIndex=1]\n");
        });
    }

    private void assertSql(
            String expected,
            String sql,
            CairoEngine engine,
            SqlExecutionContextImpl sqlExecutionContext,
            StringSink stringSink
    ) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSql(engine, sqlExecutionContext, sql, stringSink, expected);
        }
    }

    private void createX() throws SqlException {
        ddl(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(10)" +
                        ") timestamp (timestamp);"
        );
    }

    private void createY() throws SqlException {
        ddl("create table y ( ts timestamp ) timestamp(ts) partition by day wal;");
    }

    private void createZ() throws SqlException {
        ddl(
                "create table z as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(10)" +
                        ") timestamp (timestamp) partition by day wal;"
        );
    }

    @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
    private void fuzzConcurrentCreatesAndDropsCreatorThread(AtomicInteger counter) throws SqlException, InterruptedException {
        String createDdl = "CREATE TABLE IF NOT EXISTS foo ( ts TIMESTAMP, x INT, y DOUBLE, z SYMBOL );";

        try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
            while (true) {
                engine.ddl(createDdl, sqlExecutionContext);
                counter.incrementAndGet();
                Thread.sleep(50);
            }
        }
    }

    @SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
    private void fuzzConcurrentCreatesAndDropsDropperThread(AtomicInteger counter) throws SqlException, InterruptedException {
        String dropDdl = "DROP TABLE IF EXISTS foo;";

        while (true) {
            drop(dropDdl);
            counter.incrementAndGet();
            Thread.sleep(50);
        }
    }

    protected String dumpTables(StringSink stringSink) throws SqlException {
        return TestUtils.printSqlToString(engine, sqlExecutionContext, "tables()", stringSink);
    }

}
