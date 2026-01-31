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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.MetadataCache;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.CharSequenceObjHashMap;
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
    private static final String xMetaStringSansHeader = """
            CairoTable [name=x, id=1, directoryName=x~, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=NONE, timestampIndex=3, timestampName=timestamp, ttlHours=0, walEnabled=false, columnCount=16]
            \t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
            \t\tCairoColumn [name=sym, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
            \t\tCairoColumn [name=amt, position=2, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=2]
            \t\tCairoColumn [name=timestamp, position=3, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=3]
            \t\tCairoColumn [name=b, position=4, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=4]
            \t\tCairoColumn [name=c, position=5, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=5]
            \t\tCairoColumn [name=d, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=6]
            \t\tCairoColumn [name=e, position=7, type=FLOAT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=7]
            \t\tCairoColumn [name=f, position=8, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=8]
            \t\tCairoColumn [name=g, position=9, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=9]
            \t\tCairoColumn [name=ik, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, writerIndex=10]
            \t\tCairoColumn [name=j, position=11, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=11]
            \t\tCairoColumn [name=k, position=12, type=TIMESTAMP, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=12]
            \t\tCairoColumn [name=l, position=13, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=13]
            \t\tCairoColumn [name=m, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=14]
            \t\tCairoColumn [name=n, position=15, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=15]""";

    private static final String xMetaString = "MetadataCache [tableCount=1]\n" +
            "\t" + xMetaStringSansHeader + "\n";

    private static final String xMetaStringId2SansHeader = xMetaStringSansHeader.replace("id=1", "id=2");

    private static final String yMetaString = """
            MetadataCache [tableCount=1]
            \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=1]
            \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
            """;
    private static final String zMetaString = """
            MetadataCache [tableCount=1]
            \tCairoTable [name=z, id=1, directoryName=z~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=3, timestampName=timestamp, ttlHours=0, walEnabled=true, columnCount=16]
            \t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
            \t\tCairoColumn [name=sym, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
            \t\tCairoColumn [name=amt, position=2, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=2]
            \t\tCairoColumn [name=timestamp, position=3, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=3]
            \t\tCairoColumn [name=b, position=4, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=4]
            \t\tCairoColumn [name=c, position=5, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=5]
            \t\tCairoColumn [name=d, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=6]
            \t\tCairoColumn [name=e, position=7, type=FLOAT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=7]
            \t\tCairoColumn [name=f, position=8, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=8]
            \t\tCairoColumn [name=g, position=9, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=9]
            \t\tCairoColumn [name=ik, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, writerIndex=10]
            \t\tCairoColumn [name=j, position=11, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=11]
            \t\tCairoColumn [name=k, position=12, type=TIMESTAMP, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=12]
            \t\tCairoColumn [name=l, position=13, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=13]
            \t\tCairoColumn [name=m, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=14]
            \t\tCairoColumn [name=n, position=15, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=15]
            """;

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
            execute("create table foo ( ts timestamp, x int ) timestamp(ts) partition by day wal;");
            AtomicReference<Throwable> exception = new AtomicReference<>();

            Thread fooToBahThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.execute("rename table foo to bah", sqlExecutionContext);
                        TestUtils.assertSql(engine, sqlExecutionContext, "show columns from bah", new StringSink(), "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\nts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\nx\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
                    }
                } catch (SqlException | CairoException ignore) {
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    exception.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            });

            Thread bahToFooThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.execute("rename table bah to foo", sqlExecutionContext);
                        TestUtils.assertSql(engine, sqlExecutionContext, "show columns from foo", new StringSink(), "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\nts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\nx\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
                    }
                } catch (SqlException | CairoException ignore) {
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
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
                        s.contains("foo\t") ^ s.contains("bah\t")
                );
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
                assertQueryNoLeakCheck(
                        """
                                id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\tttlValue\tttlUnit\ttable_type
                                1\tbah\tts\tDAY\t1000\t300000000\ttrue\tfoo~1\tfalse\t0\tHOUR\tT
                                """,
                        "select id, table_name, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup, ttlValue, ttlUnit, matView, table_type from tables()",
                        ""
                );
            }
            if (bahToken == null) {
                Assert.assertFalse(cacheString.contains("name=bah"));
                Assert.assertTrue(cacheString.contains("name=foo"));
                assertQueryNoLeakCheck(
                        """
                                id	table_name	designatedTimestamp	partitionBy	maxUncommittedRows	o3MaxLag	walEnabled	directoryName	dedup	ttlValue	ttlUnit	matView	table_type
                                1	foo	ts	DAY	1000	300000000	true	foo~1	false	0	HOUR	false	T
                                """,
                        "select id, table_name, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup, ttlValue, ttlUnit, matView, table_type from tables()",
                        "",
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void fuzzRenamesWithConcurrentAlters() throws Exception {

        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, x int ) timestamp(ts) partition by day wal;");
            AtomicReference<Throwable> exception = new AtomicReference<>();

            Thread fooToBahThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.execute("rename table foo to bah", sqlExecutionContext);
                        assertExceptionNoLeakCheck("show columns from foo", 18, "table does not exist", false, sqlExecutionContext);
                        TestUtils.assertSql(engine, sqlExecutionContext, "show columns from bah", new StringSink(), "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\nts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\nx\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
                    }
                } catch (InterruptedException | SqlException | CairoException ignore) {
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    exception.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            });

            Thread bahToFooThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.execute("rename table bah to foo", sqlExecutionContext);
                        assertException("show columns from bah", 18, "table does not exist", sqlExecutionContext);
                        TestUtils.assertSql(engine, sqlExecutionContext, "show columns from foo", new StringSink(), "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\nts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\nx\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
                    }
                } catch (InterruptedException | SqlException | CairoException ignore) {
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    exception.set(e);
                } finally {
                    Path.clearThreadLocals();
                }

            });

            Thread adderThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                    while (true) {
                        engine.execute("alter table foo add column y symbol", sqlExecutionContext);
                    }
                } catch (SqlException | CairoException ignored) {
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
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


            execute("ALTER TABLE y ADD COLUMN foo VARCHAR");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=256, writerIndex=1]
                    """);

            execute("ALTER TABLE y ADD COLUMN bah SYMBOL");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=3]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=256, writerIndex=1]
                    \t\tCairoColumn [name=bah, position=2, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=2]
                    """);

            execute("ALTER TABLE y ALTER COLUMN foo TYPE STRING");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=3, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=3]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=256, writerIndex=3]
                    \t\tCairoColumn [name=bah, position=2, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=2]
                    """);

        });
    }

    @Test
    public void testAlterTableColumnAddIndex() throws Exception {
        assertMemoryLeak(() -> {

            execute("CREATE TABLE y (ts TIMESTAMP, foo SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=1]
                    """);

            execute("ALTER TABLE y ALTER COLUMN foo ADD INDEX");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=SYMBOL, indexBlockCapacity=256, writerIndex=1]
                    """);

        });
    }

    @Test
    public void testAlterTableColumnCacheNocache() throws Exception {
        assertMemoryLeak(() -> {


            execute("CREATE TABLE y ( ts TIMESTAMP, x SYMBOL ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=1]
                    """);

            execute("ALTER TABLE y ALTER COLUMN x NOCACHE");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=1]
                    """);

            execute("ALTER TABLE y ALTER COLUMN x CACHE");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=1]
                    """);

        });
    }

    @Test
    public void testAlterTableColumnDropIndex() throws Exception {
        assertMemoryLeak(() -> {


            execute("CREATE TABLE y (ts TIMESTAMP, foo SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=SYMBOL, indexBlockCapacity=256, writerIndex=1]
                    """);

            execute("ALTER TABLE y ALTER COLUMN foo DROP INDEX");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=1]
                    """);

        });
    }

    @Test
    public void testAlterTableColumnType() throws Exception {
        assertMemoryLeak(() -> {

            execute("CREATE TABLE y (ts TIMESTAMP, foo VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);

            execute("ALTER TABLE y ALTER COLUMN foo TYPE SYMBOL");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=2]
                    """);
        });
    }

    @Test
    public void testAlterTableDedupDisable() throws Exception {
        assertMemoryLeak(() -> {


            execute("CREATE TABLE y ( ts TIMESTAMP, foo INT ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, foo)");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=true, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=INT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);

            execute("ALTER TABLE y DEDUP DISABLE");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);
        });
    }

    @Test
    public void testAlterTableDedupEnable() throws Exception {
        assertMemoryLeak(() -> {

            createZ();
            drainWalQueue();

            assertCairoMetadata(zMetaString);

            execute("ALTER TABLE z DEDUP ENABLE UPSERT KEYS(timestamp, i, e, k)");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=z, id=1, directoryName=z~1, hasDedup=true, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=3, timestampName=timestamp, ttlHours=0, walEnabled=true, columnCount=16]
                    \t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=sym, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    \t\tCairoColumn [name=amt, position=2, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=2]
                    \t\tCairoColumn [name=timestamp, position=3, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=3]
                    \t\tCairoColumn [name=b, position=4, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=4]
                    \t\tCairoColumn [name=c, position=5, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=5]
                    \t\tCairoColumn [name=d, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=6]
                    \t\tCairoColumn [name=e, position=7, type=FLOAT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=7]
                    \t\tCairoColumn [name=f, position=8, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=8]
                    \t\tCairoColumn [name=g, position=9, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=9]
                    \t\tCairoColumn [name=ik, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, writerIndex=10]
                    \t\tCairoColumn [name=j, position=11, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=11]
                    \t\tCairoColumn [name=k, position=12, type=TIMESTAMP, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=12]
                    \t\tCairoColumn [name=l, position=13, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=13]
                    \t\tCairoColumn [name=m, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=14]
                    \t\tCairoColumn [name=n, position=15, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=15]
                    """);
        });
    }

    @Test
    public void testAlterTableDropColumn() throws Exception {
        assertMemoryLeak(() -> {


            execute("CREATE TABLE y ( bar SYMBOL, ts TIMESTAMP, foo INT ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, foo)");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=true, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=1, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=3]
                    \t\tCairoColumn [name=bar, position=0, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=0]
                    \t\tCairoColumn [name=ts, position=1, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    \t\tCairoColumn [name=foo, position=2, type=INT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=2]
                    """);

            // drop column behind designated timestamp
            execute("ALTER TABLE y DROP COLUMN foo");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=true, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=1, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=bar, position=0, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, writerIndex=0]
                    \t\tCairoColumn [name=ts, position=1, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);

            // drop column in front of designated timestamp
            execute("ALTER TABLE y DROP COLUMN bar");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=true, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=1]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);
        });
    }

    @Test
    public void testAlterTableRenameColumn() throws Exception {
        assertMemoryLeak(() -> {


            execute("CREATE TABLE y ( ts TIMESTAMP, x INT ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);

            execute("ALTER TABLE y RENAME COLUMN x TO x2");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x2, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);
        });
    }

    @Test
    public void testAlterTableSetParam() throws Exception {
        assertMemoryLeak(() -> {


            execute("CREATE TABLE y ( ts TIMESTAMP, x INT ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);

            execute("ALTER TABLE y SET PARAM maxUncommittedRows = 42");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=42, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);

            execute("ALTER TABLE y SET PARAM o3MaxLag = 42s");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=42, o3MaxLag=42000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);

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
    public void testCreateBeforeCacheHydrated() throws Exception {
        assertMemoryLeak(() -> {
            try (MetadataCache cache = new MetadataCache(engine)) {
                TableToken yToken = createY();

                var writer = cache.writeLock();
                writer.hydrateTable(yToken);
                writer.close();
            }
        });
    }

    @Test
    public void testDropAndRecreateTableRefreshSnapshots() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            CharSequenceObjHashMap<CairoTable> cache = new CharSequenceObjHashMap<>();
            long tableCacheVersion = -1;

            try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                tableCacheVersion = metadataRO.snapshot(cache, tableCacheVersion);
            }

            CairoTable x = cache.get("x");
            sink.clear();
            x.toSink(sink);
            TestUtils.assertEquals(xMetaStringSansHeader, sink);

            execute("DROP TABLE x");
            createX();
            drainWalQueue();

            try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                metadataRO.snapshot(cache, tableCacheVersion);
            }
            x = cache.get("x");
            sink.clear();
            x.toSink(sink);
            TestUtils.assertEquals(xMetaStringId2SansHeader, sink);
        });
    }

    @Test
    public void testDropBeforeCacheHydrated() throws Exception {
        assertMemoryLeak(() -> {
            TableToken yToken = createY();

            try (MetadataCache cache = new MetadataCache(engine)) {
                execute("DROP TABLE y");

                var writer = cache.writeLock();
                writer.dropTable(yToken);
                writer.close();
            }
        });
    }

    @Test
    public void testDropTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE y ( ts TIMESTAMP, x INT ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);

            execute("DROP TABLE y");
            drainWalQueue();

            assertException("table_columns('y')", 14, "table does not exist");
        });
    }

    @Test
    public void testMetadataAfterColumnTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            assertCairoMetadata(xMetaString);

            final TableToken tt = engine.getTableTokenIfExists("x");
            assertTimestamp(tt, 3);
            execute("alter table x alter column i type long");
            assertTimestamp(tt, 3);
        });
    }

    @Test
    public void testMetadataAfterDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            assertCairoMetadata(xMetaString);

            final TableToken tt = engine.getTableTokenIfExists("x");
            assertTimestamp(tt, 3);
            execute("alter table x drop column i");
            assertTimestamp(tt, 2);
        });
    }

    @Test
    public void testMetadataUpdatedCorrectlyWhenRenamingTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table foo ( ts timestamp, x int) timestamp(ts) partition by day wal;");
            assertSql(
                    """
                            id	table_name	designatedTimestamp	partitionBy	maxUncommittedRows	o3MaxLag	walEnabled	directoryName	dedup	ttlValue	ttlUnit	matView	table_type
                            1	foo	ts	DAY	1000	300000000	true	foo~1	false	0	HOUR	false	T
                            """,
                    "select id, table_name, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup, ttlValue, ttlUnit, matView, table_type from tables()"
            );

            execute("rename table foo to bah");
            drainWalQueue();
            assertSql(
                    """
                            id	table_name	designatedTimestamp	partitionBy	maxUncommittedRows	o3MaxLag	walEnabled	directoryName	dedup	ttlValue	ttlUnit	matView	table_type
                            1	bah	ts	DAY	1000	300000000	true	foo~1	false	0	HOUR	false	T
                            """,
                    "select id, table_name, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup, ttlValue, ttlUnit, matView, table_type from tables()"
            );
        });
    }

    @Test
    public void testRenameBeforeCacheHydrated() throws Exception {
        assertMemoryLeak(() -> {
            TableToken yToken = createY();

            try (MetadataCache cache = new MetadataCache(engine)) {
                execute("RENAME TABLE y TO y2");
                TableToken y2Token = engine.verifyTableName("y2");

                var writer = cache.writeLock();
                writer.renameTable(yToken, y2Token);
                writer.close();
            }
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        assertMemoryLeak(() -> {

            createY();

            assertCairoMetadata(yMetaString);

            execute("RENAME TABLE y TO y2");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y2, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=1]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    """);

        });
    }

    @Test
    public void testTruncateTable() throws Exception {
        assertMemoryLeak(() -> {


            execute("CREATE TABLE y ( ts TIMESTAMP, x INT ) timestamp(ts) partition by day wal;");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);

            execute("TRUNCATE TABLE y");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, writerIndex=1]
                    """);
        });
    }

    private static void assertCairoMetadata(String expected) {
        try (MetadataCacheReader ro = engine.getMetadataCache().readLock()) {
            sink.clear();
            ro.toSink(sink);
            TestUtils.assertEquals(expected, sink);
        }

        // Check that startup load reads the same metadata
        engine.getMetadataCache().onStartupAsyncHydrator();
        try (MetadataCacheReader ro = engine.getMetadataCache().readLock()) {
            sink.clear();
            ro.toSink(sink);
            TestUtils.assertEquals(expected, sink);
        }
    }

    private static void assertTimestamp(TableToken tt, int timestampIndex) {
        try (MetadataCacheReader reader = engine.getMetadataCache().readLock()) {
            final CairoTable table = reader.getTable(tt);
            Assert.assertNotNull(table);
            final int tsIndex = table.getTimestampIndex();
            Assert.assertEquals(timestampIndex, tsIndex);
            Assert.assertEquals("timestamp", table.getTimestampName());
        }
    }

    private void createX() throws SqlException {
        execute(
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

    private TableToken createY() throws SqlException {
        execute("create table y ( ts timestamp ) timestamp(ts) partition by day wal;");
        return engine.verifyTableName("y");
    }

    private void createZ() throws SqlException {
        execute(
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

    private String dumpTables(StringSink stringSink) throws SqlException {
        return TestUtils.printSqlToString(engine, sqlExecutionContext, "tables()", stringSink);
    }

    @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
    private void fuzzConcurrentCreatesAndDropsCreatorThread(AtomicInteger counter) throws SqlException, InterruptedException {
        String createDdl = "CREATE TABLE IF NOT EXISTS foo ( ts TIMESTAMP, x INT, y DOUBLE, z SYMBOL );";

        try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
            while (true) {
                engine.execute(createDdl, sqlExecutionContext);
                counter.incrementAndGet();
                Thread.sleep(50);
            }
        }
    }

    @SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
    private void fuzzConcurrentCreatesAndDropsDropperThread(AtomicInteger counter) throws SqlException, InterruptedException {
        String dropDdl = "DROP TABLE IF EXISTS foo;";

        while (true) {
            execute(dropDdl);
            counter.incrementAndGet();
            Thread.sleep(50);
        }
    }
}
