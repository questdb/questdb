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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoColumn;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.MetadataCache;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.CharSequenceObjSortedHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class MetadataCacheTest extends AbstractCairoTest {
    private static final String xMetaStringSansHeader = """
            CairoTable [name=x, id=1, directoryName=x~, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=NONE, timestampIndex=3, timestampName=timestamp, ttlHours=0, walEnabled=false, columnCount=16]
            \t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
            \t\tCairoColumn [name=sym, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
            \t\tCairoColumn [name=amt, position=2, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=2]
            \t\tCairoColumn [name=timestamp, position=3, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=3]
            \t\tCairoColumn [name=b, position=4, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=4]
            \t\tCairoColumn [name=c, position=5, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=5]
            \t\tCairoColumn [name=d, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=6]
            \t\tCairoColumn [name=e, position=7, type=FLOAT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=7]
            \t\tCairoColumn [name=f, position=8, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=8]
            \t\tCairoColumn [name=g, position=9, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=9]
            \t\tCairoColumn [name=ik, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=10]
            \t\tCairoColumn [name=j, position=11, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=11]
            \t\tCairoColumn [name=k, position=12, type=TIMESTAMP, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=12]
            \t\tCairoColumn [name=l, position=13, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=13]
            \t\tCairoColumn [name=m, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=14]
            \t\tCairoColumn [name=n, position=15, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=15]""";

    private static final String xMetaString = "MetadataCache [tableCount=1]\n" +
            "\t" + xMetaStringSansHeader + "\n";

    private static final String xMetaStringId2SansHeader = xMetaStringSansHeader.replace("id=1", "id=2");

    private static final String yMetaString = """
            MetadataCache [tableCount=1]
            \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=1]
            \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
            """;
    private static final String zMetaString = """
            MetadataCache [tableCount=1]
            \tCairoTable [name=z, id=1, directoryName=z~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=3, timestampName=timestamp, ttlHours=0, walEnabled=true, columnCount=16]
            \t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
            \t\tCairoColumn [name=sym, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
            \t\tCairoColumn [name=amt, position=2, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=2]
            \t\tCairoColumn [name=timestamp, position=3, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=3]
            \t\tCairoColumn [name=b, position=4, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=4]
            \t\tCairoColumn [name=c, position=5, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=5]
            \t\tCairoColumn [name=d, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=6]
            \t\tCairoColumn [name=e, position=7, type=FLOAT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=7]
            \t\tCairoColumn [name=f, position=8, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=8]
            \t\tCairoColumn [name=g, position=9, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=9]
            \t\tCairoColumn [name=ik, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=10]
            \t\tCairoColumn [name=j, position=11, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=11]
            \t\tCairoColumn [name=k, position=12, type=TIMESTAMP, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=12]
            \t\tCairoColumn [name=l, position=13, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=13]
            \t\tCairoColumn [name=m, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=14]
            \t\tCairoColumn [name=n, position=15, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=15]
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
                        assertQuery("show columns from bah")
                                .withContext(sqlExecutionContext)
                                .noLeakCheck()
                                .returnsOnce("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\tindexType\tindexInclude\nts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\nx\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
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
                        assertQuery("show columns from foo")
                                .withContext(sqlExecutionContext)
                                .noLeakCheck()
                                .returnsOnce("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\tindexType\tindexInclude\nts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\nx\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
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
                assertQuery("select id, table_name, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup, ttlValue, ttlUnit, matView, table_type from tables()")
                        .noLeakCheck()
                        .timestamp("")
                        .noRandomAccess()
                        .returns("""
                                id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\tttlValue\tttlUnit\ttable_type
                                1\tbah\tts\tDAY\t1000\t300000000\ttrue\tfoo~1\tfalse\t0\tHOUR\tT
                                """);
            }
            if (bahToken == null) {
                Assert.assertFalse(cacheString.contains("name=bah"));
                Assert.assertTrue(cacheString.contains("name=foo"));
                assertQuery("select id, table_name, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup, ttlValue, ttlUnit, matView, table_type from tables()")
                        .noLeakCheck()
                        .timestamp("")
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                id	table_name	designatedTimestamp	partitionBy	maxUncommittedRows	o3MaxLag	walEnabled	directoryName	dedup	ttlValue	ttlUnit	matView	table_type
                                1	foo	ts	DAY	1000	300000000	true	foo~1	false	0	HOUR	false	T
                                """);
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
                        assertQuery("show columns from foo")
                                .withContext(sqlExecutionContext)
                                .noLeakCheck()
                                .fails(18, "table does not exist");
                        assertQuery("show columns from bah")
                                .withContext(sqlExecutionContext)
                                .noLeakCheck()
                                .returnsOnce("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\tindexType\tindexInclude\nts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\nx\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
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
                        assertQuery("show columns from bah")
                                .withContext(sqlExecutionContext)
                                .noLeakCheck()
                                .fails(18, "table does not exist");
                        assertQuery("show columns from foo")
                                .withContext(sqlExecutionContext)
                                .noLeakCheck()
                                .returnsOnce("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\tindexType\tindexInclude\nts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\nx\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
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

    @Override
    public void setUp() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, TestUtils.randomSymbolIndexTypeName(rnd));
        super.setUp();
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("ALTER TABLE y ADD COLUMN bah SYMBOL");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=3]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    \t\tCairoColumn [name=bah, position=2, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=2]
                    """);

            execute("ALTER TABLE y ALTER COLUMN foo TYPE STRING");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=3, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=3]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=3]
                    \t\tCairoColumn [name=bah, position=2, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=2]
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("ALTER TABLE y ALTER COLUMN foo ADD INDEX TYPE POSTING");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=POSTING, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("ALTER TABLE y ALTER COLUMN x NOCACHE");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("ALTER TABLE y ALTER COLUMN x CACHE");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

        });
    }

    @Test
    public void testAlterTableColumnDropIndex() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            String indexType = TestUtils.randomSymbolIndexTypeName(rnd);
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, indexType);

            execute("CREATE TABLE y (ts TIMESTAMP, foo SYMBOL INDEX) TIMESTAMP(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=###INDEXTYPE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """.replace("###INDEXTYPE", indexType));

            execute("ALTER TABLE y ALTER COLUMN foo DROP INDEX");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("ALTER TABLE y ALTER COLUMN foo TYPE SYMBOL");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=2]
                    """);
        });
    }

    @Test
    public void testAlterTableColumnTypeChangedTwiceKeepsColumnOrder() throws Exception {
        // After two ALTER COLUMN TYPE on the same column, the new column's replacingIndex
        // points at its immediate predecessor (the intermediate column), not the chain
        // root. hydrateTable() must derive the catalogue position from the chain root
        // (getOriginalWriterIndex()), otherwise the twice-converted column jumps to the
        // end of table_columns()/information_schema.columns while SELECT * (reader
        // metadata) still keeps it in place.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE y (ts TIMESTAMP, foo VARCHAR, bah SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL;");
            drainWalQueue();

            execute("ALTER TABLE y ALTER COLUMN foo TYPE STRING");
            drainWalQueue();

            execute("ALTER TABLE y ALTER COLUMN foo TYPE INT");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=3]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=4]
                    \t\tCairoColumn [name=bah, position=2, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=2]
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=INT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("ALTER TABLE y DEDUP DISABLE");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=foo, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
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
                    \t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=sym, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    \t\tCairoColumn [name=amt, position=2, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=2]
                    \t\tCairoColumn [name=timestamp, position=3, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=3]
                    \t\tCairoColumn [name=b, position=4, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=4]
                    \t\tCairoColumn [name=c, position=5, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=5]
                    \t\tCairoColumn [name=d, position=6, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=6]
                    \t\tCairoColumn [name=e, position=7, type=FLOAT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=7]
                    \t\tCairoColumn [name=f, position=8, type=SHORT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=8]
                    \t\tCairoColumn [name=g, position=9, type=DATE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=9]
                    \t\tCairoColumn [name=ik, position=10, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=10]
                    \t\tCairoColumn [name=j, position=11, type=LONG, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=11]
                    \t\tCairoColumn [name=k, position=12, type=TIMESTAMP, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=12]
                    \t\tCairoColumn [name=l, position=13, type=BYTE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=13]
                    \t\tCairoColumn [name=m, position=14, type=BINARY, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=14]
                    \t\tCairoColumn [name=n, position=15, type=STRING, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=15]
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
                    \t\tCairoColumn [name=bar, position=0, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=ts, position=1, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    \t\tCairoColumn [name=foo, position=2, type=INT, isDedupKey=true, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=2]
                    """);

            // drop column behind designated timestamp
            execute("ALTER TABLE y DROP COLUMN foo");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=true, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=1, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=bar, position=0, type=SYMBOL, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, indexType=NONE, indexBlockCapacity=256, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=ts, position=1, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            // drop column in front of designated timestamp
            execute("ALTER TABLE y DROP COLUMN bar");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=true, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=1]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("ALTER TABLE y RENAME COLUMN x TO x2");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x2, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("ALTER TABLE y SET PARAM maxUncommittedRows = 42");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=42, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("ALTER TABLE y SET PARAM o3MaxLag = 42s");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=42, o3MaxLag=42000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
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
    public void testCoveringIndexSurvivesStartupHydration() throws Exception {
        // Regression: onStartupAsyncHydrator() reads _meta directly via
        // MetadataCache.hydrateTableStartup(). That path reads indexType and
        // block capacity, but never reads the trailing covering-column
        // section, so any INCLUDE list previously cached gets wiped when the
        // hydrator replaces the CairoTable entry. SHOW CREATE TABLE,
        // SHOW COLUMNS, and information_schema.columns all read from
        // MetadataCache, so users see the INCLUDE list silently disappear
        // after a server restart even though _meta on disk still has it.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_inc (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Reach the cache via the good path: TableWriter.addIndex() calls
            // MetadataCacheWriter.hydrateTable(TableMetadata) which preserves
            // covering indices. The startup hydrator path is the buggy one.
            execute("ALTER TABLE t_inc ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");

            TableToken token = engine.getTableTokenIfExists("t_inc");
            Assert.assertNotNull(token);

            // Sanity: ALTER populated the cache with the covering list.
            assertCoveringIncludesPriceAndQty(token, "before startup hydration");

            // Simulate a fresh-process restart: reload metadata from _meta.
            engine.getMetadataCache().onStartupAsyncHydrator();

            // The covering list must survive a meta-file re-read, otherwise
            // SHOW CREATE TABLE / SHOW COLUMNS will silently drop INCLUDE.
            assertCoveringIncludesPriceAndQty(token, "after startup hydration");
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
            CharSequenceObjSortedHashMap<CairoTable> cache = new CharSequenceObjSortedHashMap<>();
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("DROP TABLE y");
            drainWalQueue();

            assertQuery("table_columns('y')")
                    .noLeakCheck()
                    .fails(14, "table does not exist");
        });
    }

    @Test
    public void testClearCacheReenablesReconcile() throws Exception {
        // clearCache() wipes the cache and must reset the fast-path flag so the next
        // catalogue reconcile rebuilds the full set. Guards the checkpoint/restore
        // style window where the registry is ahead of a freshly-cleared cache.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Warm the cache and flip it into the fast path.
            engine.getMetadataCache().onStartupAsyncHydrator();

            // Wipe the cache. clearCache() must clear the completeness flag too.
            try (MetadataCacheWriter w = engine.getMetadataCache().writeLock()) {
                w.clearCache();
            }

            // The reconcile must run again and rebuild the complete set.
            engine.getMetadataCache().hydrateAllTables();
            try (MetadataCacheReader ro = engine.getMetadataCache().readLock()) {
                Assert.assertEquals(2, ro.getTableCount());
            }
        });
    }

    @Test
    public void testReconcileLatchesCompleteAfterHydratingMissingTables() throws Exception {
        // M3 optimization: when one hydrateAllTables() reconcile successfully hydrates
        // every missing table, it must latch cacheComplete in that same pass (reusing the
        // token snapshot it already collected), not leave the flag off and force the next
        // catalogue query to run a second, redundant full reconcile (getTableTokens +
        // allocation + scan) just to observe "nothing missing".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final MetadataCache cache = engine.getMetadataCache();

            // Empty + unlatch: tables stay registered, cache is empty, cacheComplete=false.
            try (MetadataCacheWriter w = cache.writeLock()) {
                w.clearCache();
            }
            Assert.assertFalse(cache.isCacheComplete());

            // A single reconcile hydrates both missing tables AND latches in the same
            // pass. Pre-fix this latched only on a second reconcile, so the flag stayed
            // off here.
            cache.hydrateAllTables();
            Assert.assertTrue(cache.isCacheComplete());
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertEquals(2, ro.getTableCount());
            }
        });
    }

    @Test
    public void testHydrateAllTablesShortCircuitsWhenCacheComplete() throws Exception {
        // hydrateAllTables() backs the tables()/all_tables() startup-race fix, but it
        // must be a no-op once the cache is known complete: from then on writers keep
        // the cache current, so a per-query reconcile would be pure overhead. This pins
        // that fast-path contract.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Complete the one-shot startup hydration: fills the cache and flips
            // MetadataCache into its steady-state fast path.
            engine.getMetadataCache().onStartupAsyncHydrator();

            // Evict a single still-registered table directly from the cache (not via
            // clearCache(), which would reset the fast-path flag). Because the cache was
            // marked complete, hydrateAllTables() must short-circuit and leave the gap.
            TableToken a = engine.getTableTokenIfExists("a");
            try (MetadataCacheWriter w = engine.getMetadataCache().writeLock()) {
                w.dropTable(a);
            }
            engine.getMetadataCache().hydrateAllTables();
            try (MetadataCacheReader ro = engine.getMetadataCache().readLock()) {
                Assert.assertEquals(1, ro.getTableCount());
                Assert.assertNull(ro.getTable(a));
            }
        });
    }

    @Test
    public void testStartupHydratorDoesNotLatchCompleteWhenATableFailsToHydrate() throws Exception {
        // A table whose _meta cannot be read at startup (fd exhaustion, torn read during
        // concurrent WAL apply, slow/NFS storage, mid-conversion) makes
        // hydrateTableStartup() swallow the failure (throwError=false) and evict the
        // table from the cache. onStartupAsyncHydrator() must NOT then latch
        // cacheComplete: doing so would short-circuit every future catalogue reconcile
        // and hide the table forever (until a writer touches it or the process
        // restarts). Leaving the flag unset lets the reconcile self-heal once the
        // _meta becomes readable again.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE alpha (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE bravo (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE charlie (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final MetadataCache cache = engine.getMetadataCache();
            final File meta = metaFile("charlie");
            final File hidden = new File(meta.getParentFile(), "_meta.hidden");

            // Make charlie's _meta unreadable, then run the startup hydrator from an
            // empty cache (mimics a fresh process whose hydrator races a transient fault).
            java.nio.file.Files.move(meta.toPath(), hidden.toPath());
            try {
                try (MetadataCacheWriter w = cache.writeLock()) {
                    w.clearCache();
                }
                cache.onStartupAsyncHydrator();

                // alpha + bravo hydrated; charlie could not be read and is absent.
                try (MetadataCacheReader ro = cache.readLock()) {
                    Assert.assertEquals(2, ro.getTableCount());
                    Assert.assertNull(ro.getTable(engine.getTableTokenIfExists("charlie")));
                }
            } finally {
                // _meta is readable again (the transient fault cleared).
                java.nio.file.Files.move(hidden.toPath(), meta.toPath());
            }

            // Self-heal: because the startup hydrator did NOT latch cacheComplete, the
            // reconcile still runs and now picks charlie up. With the pre-fix
            // unconditional latch this short-circuited and charlie stayed hidden.
            cache.hydrateAllTables();
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertEquals(3, ro.getTableCount());
                Assert.assertNotNull(ro.getTable(engine.getTableTokenIfExists("charlie")));
            }
        });
    }

    @Test
    public void testReconcileBudgetCountsOnlyZeroProgressRounds() throws Exception {
        // M2: the give-up budget must count consecutive reconcile rounds that make NO
        // progress, not every reconcile that still finds something missing - otherwise a
        // post-restart storm (>=MAX concurrent catalogue queries racing the startup
        // hydrator) exhausts the 8-budget in one burst and latches cacheComplete with a
        // transiently-unreadable table still absent. A reconcile that hydrates >=1
        // previously-missing table is self-healing, so it resets the budget; only rounds
        // that hydrate nothing spend it. Here we drive several progress rounds (tables
        // becoming readable one round at a time, as a storm hydrates incrementally) and
        // assert the budget is not consumed while progress continues - it is spent only by
        // the genuinely-stuck table once progress stops.
        final int maxIncompleteReconcilePasses = 8;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE alpha (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE bravo (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE gamma (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE stuck (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final MetadataCache cache = engine.getMetadataCache();

            final File bravoMeta = metaFile("bravo");
            final File bravoHidden = new File(bravoMeta.getParentFile(), "_meta.hidden");
            final File gammaMeta = metaFile("gamma");
            final File gammaHidden = new File(gammaMeta.getParentFile(), "_meta.hidden");
            final File stuckMeta = metaFile("stuck");
            final File stuckHidden = new File(stuckMeta.getParentFile(), "_meta.hidden");

            // Hide bravo, gamma and stuck so only alpha can hydrate initially; reveal
            // bravo/gamma one round at a time to mimic a storm making incremental progress.
            java.nio.file.Files.move(bravoMeta.toPath(), bravoHidden.toPath());
            java.nio.file.Files.move(gammaMeta.toPath(), gammaHidden.toPath());
            java.nio.file.Files.move(stuckMeta.toPath(), stuckHidden.toPath());
            try {
                try (MetadataCacheWriter w = cache.writeLock()) {
                    w.clearCache();
                }

                // Round 1: only alpha hydrates (progress) - bravo/gamma/stuck still hidden.
                cache.hydrateAllTables();
                Assert.assertFalse(cache.isCacheComplete());

                // Round 2: reveal bravo, it hydrates (progress).
                java.nio.file.Files.move(bravoHidden.toPath(), bravoMeta.toPath());
                cache.hydrateAllTables();
                Assert.assertFalse(cache.isCacheComplete());

                // Round 3: reveal gamma, it hydrates (progress).
                java.nio.file.Files.move(gammaHidden.toPath(), gammaMeta.toPath());
                cache.hydrateAllTables();
                Assert.assertFalse(cache.isCacheComplete());

                // Three progress rounds happened, yet the budget is untouched: now only the
                // permanently-stuck table is missing, so the next rounds are zero-progress.
                // MAX_INCOMPLETE_RECONCILE_PASSES - 1 of them must NOT yet latch (proving the
                // earlier progress rounds were not counted; pre-fix the budget would already
                // be spent here).
                for (int i = 0; i < maxIncompleteReconcilePasses - 1; i++) {
                    cache.hydrateAllTables();
                    Assert.assertFalse(
                            "give-up budget exhausted too early - progress rounds were counted",
                            cache.isCacheComplete());
                }

                // One more zero-progress round tips the budget over MAX and gives up.
                cache.hydrateAllTables();
                Assert.assertTrue(cache.isCacheComplete());
                try (MetadataCacheReader ro = cache.readLock()) {
                    Assert.assertEquals(3, ro.getTableCount());
                    Assert.assertNull(ro.getTable(engine.getTableTokenIfExists("stuck")));
                }
            } finally {
                if (bravoHidden.exists()) {
                    java.nio.file.Files.move(bravoHidden.toPath(), bravoMeta.toPath());
                }
                if (gammaHidden.exists()) {
                    java.nio.file.Files.move(gammaHidden.toPath(), gammaMeta.toPath());
                }
                java.nio.file.Files.move(stuckHidden.toPath(), stuckMeta.toPath());
            }
        });
    }

    @Test
    public void testReconcileGivesUpAfterRepeatedHydrationFailures() throws Exception {
        // Mirror of MetadataCache.MAX_INCOMPLETE_RECONCILE_PASSES. A genuinely
        // unhydratable table would otherwise be re-read (and logged CRITICAL) on every
        // catalogue query forever, since a failed hydration never lands in the cache
        // and so always looks "missing". After this many incomplete passes the
        // reconcile must give up and latch cacheComplete, leaving the table for a
        // writer or the next clearCache() epoch to pick up.
        final int maxIncompleteReconcilePasses = 8;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE alpha (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE bravo (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE charlie (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final MetadataCache cache = engine.getMetadataCache();
            final File meta = metaFile("charlie");
            final File hidden = new File(meta.getParentFile(), "_meta.hidden");

            java.nio.file.Files.move(meta.toPath(), hidden.toPath());
            try {
                try (MetadataCacheWriter w = cache.writeLock()) {
                    w.clearCache();
                }

                // First reconcile hydrates alpha+bravo (progress) and leaves charlie
                // missing. A progress round does not spend the give-up budget, so do it
                // up front; only the zero-progress rounds below count toward giving up.
                cache.hydrateAllTables();
                try (MetadataCacheReader ro = cache.readLock()) {
                    Assert.assertEquals(2, ro.getTableCount());
                }
                Assert.assertFalse(cache.isCacheComplete());

                // Now only charlie is missing and unhydratable: each reconcile re-reads
                // its (still missing) _meta and fails, making zero progress. After
                // maxIncompleteReconcilePasses such rounds the flag latches.
                for (int i = 0; i < maxIncompleteReconcilePasses; i++) {
                    cache.hydrateAllTables();
                    try (MetadataCacheReader ro = cache.readLock()) {
                        Assert.assertEquals(2, ro.getTableCount());
                    }
                }
            } finally {
                java.nio.file.Files.move(hidden.toPath(), meta.toPath());
            }

            // The retry budget is spent and cacheComplete latched, so the reconcile now
            // short-circuits: charlie stays hidden even though its _meta is readable.
            // This caps the CRITICAL-spam / per-query disk I/O path for corrupt tables.
            cache.hydrateAllTables();
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertEquals(2, ro.getTableCount());
                Assert.assertNull(ro.getTable(engine.getTableTokenIfExists("charlie")));
            }

            // clearCache() resets the budget, so a fresh epoch reconciles charlie back in.
            try (MetadataCacheWriter w = cache.writeLock()) {
                w.clearCache();
            }
            cache.hydrateAllTables();
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertEquals(3, ro.getTableCount());
                Assert.assertNotNull(ro.getTable(engine.getTableTokenIfExists("charlie")));
            }
        });
    }

    @Test
    public void testCompletenessLatchIsMutuallyExclusiveWithClearCache() throws Exception {
        // M2 regression (deterministic): the completeness latch (cacheComplete=true) in
        // hydrateAllTables() must be published under the read lock that observed the
        // cache as complete, so it is mutually exclusive with clearCache() (write lock).
        // We force the exact interleaving via a test hook fired right before the
        // publish: while the hook runs, another thread tries to clearCache(). With the
        // publish correctly inside the read lock, that clearCache() is blocked until we
        // release, so the latch can never end up set on an emptied cache. Were the
        // publish moved back outside the lock (the bug), the clearCache() would land
        // between the scan and the publish and leave cacheComplete=true over an empty
        // cache - which the post-condition below catches.
        assertMemoryLeak(() -> {
            final int tableCount = 3;
            for (int i = 0; i < tableCount; i++) {
                execute("CREATE TABLE t" + i + " (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            drainWalQueue();

            final MetadataCache cache = engine.getMetadataCache();

            // Start from a warm-but-unlatched state: full cache, cacheComplete=false.
            // clearCache() empties + unlatches; we then warm the cache table-by-table via
            // the point-lookup path (hydrateTableOnDemand never latches cacheComplete), so
            // the cache holds every table while the flag stays off. A full
            // hydrateAllTables() reconcile cannot produce this state: once it hydrates
            // every missing table it latches immediately (under the read lock).
            try (MetadataCacheWriter w = cache.writeLock()) {
                w.clearCache();
            }
            for (int i = 0; i < tableCount; i++) {
                cache.hydrateTableOnDemand(engine.getTableTokenIfExists("t" + i));
            }
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertEquals(tableCount, ro.getTableCount());
            }
            Assert.assertFalse(cache.isCacheComplete());

            // The hook fires once, on the next reconcile's missing==null publish. It
            // kicks off a concurrent clearCache() and gives it ample time to run. In
            // correct code that clearCache() is blocked on the write lock (we hold the
            // read lock), so it cannot empty the cache before we publish.
            final AtomicReference<Thread> clearer = new AtomicReference<>();
            final AtomicReference<Throwable> clearError = new AtomicReference<>();
            final AtomicBoolean fired = new AtomicBoolean(false);
            cache.setLatchCompleteTestHook(() -> {
                if (!fired.compareAndSet(false, true)) {
                    return;
                }
                final Thread b = new Thread(() -> {
                    try (MetadataCacheWriter w = cache.writeLock()) {
                        w.clearCache();
                    } catch (Throwable t) {
                        clearError.compareAndSet(null, t);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "clearer");
                clearer.set(b);
                b.start();
                // Long enough that, were we NOT holding the read lock, the clearer would
                // certainly have emptied the cache before we publish the latch.
                Os.sleep(200);
            });

            try {
                cache.hydrateAllTables();
            } finally {
                cache.setLatchCompleteTestHook(null);
            }

            final Thread b = clearer.get();
            Assert.assertNotNull("hook did not fire - publish path changed?", b);
            b.join();
            if (clearError.get() != null) {
                throw new RuntimeException(clearError.get());
            }

            // Invariant: cacheComplete must never be latched over an incomplete cache.
            // Correct code ends here with the cache emptied by the clearer and the flag
            // off (publish happened under the read lock while the cache was still full,
            // then the clearer emptied + unlatched). The buggy code would end with
            // cacheComplete=true over an empty cache.
            try (MetadataCacheReader ro = cache.readLock()) {
                if (cache.isCacheComplete()) {
                    Assert.assertEquals(
                            "cacheComplete latched on an incomplete cache",
                            tableCount, ro.getTableCount());
                }
            }

            // And the cache still self-heals to the full set.
            cache.hydrateAllTables();
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertEquals(tableCount, ro.getTableCount());
            }
        });
    }

    @Test
    public void testConcurrentReconcileAndClearCacheNeverMarksEmptyCacheComplete() throws Exception {
        // M2 regression: the completeness latch (cacheComplete=true) in
        // hydrateAllTables() / onStartupAsyncHydrator() must be published under a lock
        // that excludes clearCache(). Set outside the lock, a clearCache() can
        // interleave between observing the cache as complete and publishing the flag,
        // leaving an emptied cache marked complete - catalogue queries then
        // short-circuit forever with no self-healing. We hammer the reconcile path
        // against concurrent clearCache() and assert the invariant "cacheComplete =>
        // the cache holds every table" is never violated. The invariant is checked
        // under the read lock, which excludes clearCache(), so the table set is stable
        // for the duration of the check.
        assertMemoryLeak(() -> {
            final int tableCount = 4;
            for (int i = 0; i < tableCount; i++) {
                execute("CREATE TABLE t" + i + " (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            drainWalQueue();

            final MetadataCache cache = engine.getMetadataCache();
            final AtomicBoolean stop = new AtomicBoolean(false);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            // Heavily oversubscribe so the scheduler frequently preempts a reconciler in
            // the narrow window between releasing the pass-1 read lock and publishing the
            // latch - that preemption is what widens the race the buggy (unlocked) latch
            // exposes to clearCache(). Bounded by wall-clock so the test stays fast.
            final int reconcilerCount = Math.max(8, 2 * Runtime.getRuntime().availableProcessors());
            final long deadline = System.nanoTime() + 1_000_000_000L;

            // Reconcilers: drive the completeness-latch path from multiple threads.
            final Runnable reconciler = () -> {
                try {
                    while (!stop.get() && error.get() == null && System.nanoTime() < deadline) {
                        cache.hydrateAllTables();
                    }
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    // hydrateTableStartup() allocates a thread-local Path; release it so
                    // the surrounding assertMemoryLeak() does not flag native memory.
                    Path.clearThreadLocals();
                }
            };
            // Checker: a latched cacheComplete must imply a full cache. Read the count
            // and the flag under the read lock, which excludes clearCache(), so the
            // table set cannot change mid-check; only a buggy unlocked latch can flip
            // cacheComplete while we hold the lock over an emptied cache.
            final Runnable checker = () -> {
                try {
                    while (!stop.get() && error.get() == null && System.nanoTime() < deadline) {
                        try (MetadataCacheReader ro = cache.readLock()) {
                            final int count = ro.getTableCount();
                            if (cache.isCacheComplete() && count != tableCount) {
                                throw new AssertionError(
                                        "cacheComplete latched on an incomplete cache: tableCount="
                                                + count + " expected=" + tableCount);
                            }
                        }
                    }
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    Path.clearThreadLocals();
                }
            };

            final Thread[] reconcilers = new Thread[reconcilerCount];
            for (int i = 0; i < reconcilerCount; i++) {
                reconcilers[i] = new Thread(reconciler, "reconciler-" + i);
                reconcilers[i].start();
            }
            final Thread chk = new Thread(checker, "checker");
            chk.start();
            try {
                while (error.get() == null && System.nanoTime() < deadline) {
                    try (MetadataCacheWriter w = cache.writeLock()) {
                        w.clearCache();
                    }
                    // Brief yield so reconcilers can refill and reach the vulnerable
                    // missing==null publish, and so the checker can observe a latched
                    // flag before the next clear resets it.
                    Os.pause();
                }
            } finally {
                stop.set(true);
            }
            for (Thread t : reconcilers) {
                t.join();
            }
            chk.join();

            if (error.get() != null) {
                throw new RuntimeException(error.get());
            }

            // With clears stopped, the cache must converge back to the full set: had a
            // racing clear latched an empty-complete cache, this reconcile would
            // short-circuit and leave it short.
            cache.hydrateAllTables();
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertEquals(tableCount, ro.getTableCount());
            }
        });
    }

    @Test
    public void testHydrateTableOnDemandPopulatesMissingTable() throws Exception {
        // M3: point-lookup catalogue paths (SHOW COLUMNS / SHOW CREATE TABLE / parquet
        // partition probes) resolve the token from the registry but read the lazily
        // hydrated cache. hydrateTableOnDemand() is their single-table reconcile: it must
        // populate a registered-but-uncached table, and no-op once the cache is marked
        // complete (a still-missing table is then genuinely gone / given up on).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final MetadataCache cache = engine.getMetadataCache();
            final TableToken a = engine.getTableTokenIfExists("a");

            // Empty cache (post-restart window): the table is registered but not cached.
            try (MetadataCacheWriter w = cache.writeLock()) {
                w.clearCache();
            }
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertNull(ro.getTable(a));
            }

            // A null token is a no-op (no exception).
            cache.hydrateTableOnDemand(null);

            // On-demand hydrate brings just this table into the cache.
            cache.hydrateTableOnDemand(a);
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertNotNull(ro.getTable(a));
                Assert.assertEquals(1, ro.getTableCount());
            }

            // Once the cache is marked complete, a still-missing table is treated as gone:
            // hydrateTableOnDemand must short-circuit (no re-read) rather than re-read and
            // log on every point lookup. Evict via dropTable(), which does not reset the
            // completeness flag.
            cache.hydrateAllTables();
            Assert.assertTrue(cache.isCacheComplete());
            try (MetadataCacheWriter w = cache.writeLock()) {
                w.dropTable(a);
            }
            cache.hydrateTableOnDemand(a);
            try (MetadataCacheReader ro = cache.readLock()) {
                Assert.assertNull(ro.getTable(a));
            }
        });
    }

    @Test
    public void testShowColumnsBeforeStartupHydration() throws Exception {
        // M3 regression: SHOW COLUMNS resolves the token from the registry but reads the
        // lazily hydrated cache; in the startup window it used to throw "table does not
        // exist" for a registered table. It must hydrate the table on demand.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo (ts TIMESTAMP, a INT, b LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            // Simulate the window: registry knows the table, metadata cache is empty.
            try (MetadataCacheWriter w = engine.getMetadataCache().writeLock()) {
                w.clearCache();
            }

            printSql("show columns from foo");
            final String out = sink.toString();
            TestUtils.assertContains(out, "ts\tTIMESTAMP");
            TestUtils.assertContains(out, "a\tINT");
            TestUtils.assertContains(out, "b\tLONG");
        });
    }

    @Test
    public void testSnapshotHydratesEachTableExactlyOncePerClearEpoch() throws Exception {
        // The catalogue read path (MetadataCache.snapshot()) reconciles via
        // hydrateAllTables(), which reads each table's _meta exactly once per
        // clearCache() epoch: a table already present in the cache is never re-read.
        // We prove this through CairoTable object identity - a re-hydration would
        // replace the instance.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final MetadataCache cache = engine.getMetadataCache();
            final TableToken a = engine.getTableTokenIfExists("a");
            final TableToken b = engine.getTableTokenIfExists("b");
            final CharSequenceObjSortedHashMap<CairoTable> snap = new CharSequenceObjSortedHashMap<>();
            long version = -1;

            // Start from an empty cache to mimic the post-restart reconcile window.
            try (MetadataCacheWriter w = cache.writeLock()) {
                w.clearCache();
            }

            // First snapshot hydrates both tables from disk.
            version = cache.snapshot(snap, version);
            CairoTable a1, b1;
            try (MetadataCacheReader ro = cache.readLock()) {
                a1 = ro.getTable(a);
                b1 = ro.getTable(b);
            }
            Assert.assertNotNull(a1);
            Assert.assertNotNull(b1);

            // Repeated snapshots within the same epoch must NOT re-read: the cached
            // CairoTable instances stay identical.
            for (int i = 0; i < 3; i++) {
                version = cache.snapshot(snap, version);
                try (MetadataCacheReader ro = cache.readLock()) {
                    Assert.assertSame(a1, ro.getTable(a));
                    Assert.assertSame(b1, ro.getTable(b));
                }
            }

            // A clearCache() opens a new epoch: the next snapshot re-reads from disk,
            // producing fresh CairoTable instances.
            try (MetadataCacheWriter w = cache.writeLock()) {
                w.clearCache();
            }
            cache.snapshot(snap, version);
            try (MetadataCacheReader ro = cache.readLock()) {
                CairoTable a2 = ro.getTable(a);
                CairoTable b2 = ro.getTable(b);
                Assert.assertNotNull(a2);
                Assert.assertNotNull(b2);
                Assert.assertNotSame(a1, a2);
                Assert.assertNotSame(b1, b2);
            }
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
            assertQuery("select id, table_name, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup, ttlValue, ttlUnit, matView, table_type from tables()")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            id	table_name	designatedTimestamp	partitionBy	maxUncommittedRows	o3MaxLag	walEnabled	directoryName	dedup	ttlValue	ttlUnit	matView	table_type
                            1	foo	ts	DAY	1000	300000000	true	foo~1	false	0	HOUR	false	T
                            """);

            execute("rename table foo to bah");
            drainWalQueue();
            assertQuery("select id, table_name, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup, ttlValue, ttlUnit, matView, table_type from tables()")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            id	table_name	designatedTimestamp	partitionBy	maxUncommittedRows	o3MaxLag	walEnabled	directoryName	dedup	ttlValue	ttlUnit	matView	table_type
                            1	bah	ts	DAY	1000	300000000	true	foo~1	false	0	HOUR	false	T
                            """);
        });
    }

    @Test
    public void testParquetEncodingConfig() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, temp DOUBLE PARQUET(plain, zstd(3)), status INT PARQUET(delta_binary_packed)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=t, id=1, directoryName=t~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=3]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=temp, position=1, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=plain, parquetCompression=zstd 3, writerIndex=1]
                    \t\tCairoColumn [name=status, position=2, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=delta_binary_packed, parquetCompression=Default, writerIndex=2]
                    """);

            execute("ALTER TABLE t ALTER COLUMN status SET PARQUET(delta_binary_packed, lz4_raw)");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=t, id=1, directoryName=t~1, hasDedup=false, isSoftLink=false, metadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=3]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=temp, position=1, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=plain, parquetCompression=zstd 3, writerIndex=1]
                    \t\tCairoColumn [name=status, position=2, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=delta_binary_packed, parquetCompression=lz4_raw, writerIndex=2]
                    """);

            execute("ALTER TABLE t ALTER COLUMN temp SET PARQUET(default)");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=t, id=1, directoryName=t~1, hasDedup=false, isSoftLink=false, metadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=3]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=temp, position=1, type=DOUBLE, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    \t\tCairoColumn [name=status, position=2, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=delta_binary_packed, parquetCompression=lz4_raw, writerIndex=2]
                    """);
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    """);

        });
    }

    @Test
    public void testSnapshotSortedAlwaysAsNumberOfTablesGrow() throws Exception {
        assertMemoryLeak(() -> {
            CharSequenceObjSortedHashMap<CairoTable> sortedMap = new CharSequenceObjSortedHashMap<>();

            for (int cu = 'Z'; cu > 'A' - 1; cu--) {
                execute("CREATE TABLE " + (char) cu + " ( ts TIMESTAMP, x INT, y DOUBLE, z SYMBOL );");
            }

            long version = assertTableNamesOrderedWith(sortedMap, "[A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z]", -1);

            for (int cu = 'Z'; cu > 'A' - 1; cu = cu - 2) {
                execute("drop table " + (char) cu);
            }

            for (int cu = 'z'; cu > 'a' - 1; cu = cu - 2) {
                for (int cd = 0; cd < 2; cd++) {
                    execute("CREATE TABLE " + new String(new char[]{(char) cu, (char) (cd + 48)}) + " ( ts TIMESTAMP, x INT, y DOUBLE, z SYMBOL );");
                }
            }

            assertTableNamesOrderedWith(sortedMap, "[A,C,E,G,I,K,M,O,Q,S,U,W,Y,b0,b1,d0,d1,f0,f1,h0,h1,j0,j1,l0,l1,n0,n1,p0,p1,r0,r1,t0,t1,v0,v1,x0,x1,z0,z1]", version);
        });
    }

    @Test
    public void testSnapshotSortedWithInitialListOfTables() throws Exception {
        assertMemoryLeak(() -> {
            for (int cu = 'Z'; cu > 'A' - 1; cu--) {
                execute("CREATE TABLE " + (char) cu + " ( ts TIMESTAMP, x INT, y DOUBLE, z SYMBOL );");
            }

            assertTableNamesOrderedWith(new CharSequenceObjSortedHashMap<>(), "[A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z]", -1);
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
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);

            execute("TRUNCATE TABLE y");
            drainWalQueue();

            assertCairoMetadata("""
                    MetadataCache [tableCount=1]
                    \tCairoTable [name=y, id=1, directoryName=y~1, hasDedup=false, isSoftLink=false, metadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, ttlHours=0, walEnabled=true, columnCount=2]
                    \t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=0]
                    \t\tCairoColumn [name=x, position=1, type=INT, isDedupKey=false, isDesignated=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, indexType=NONE, indexBlockCapacity=0, parquetEncoding=Default, parquetCompression=Default, writerIndex=1]
                    """);
        });
    }

    private static void assertCoveringIncludesPriceAndQty(TableToken token, String stage) {
        try (MetadataCacheReader ro = engine.getMetadataCache().readLock()) {
            CairoTable table = ro.getTable(token);
            Assert.assertNotNull("table missing from MetadataCache " + stage, table);
            CairoColumn sym = table.getColumnQuiet("sym");
            Assert.assertNotNull("sym column missing " + stage, sym);
            IntList covering = sym.getCoveringColumnIndices();
            Assert.assertNotNull("covering INCLUDE list dropped from MetadataCache " + stage, covering);

            CairoColumn price = table.getColumnQuiet("price");
            CairoColumn qty = table.getColumnQuiet("qty");
            Assert.assertNotNull(price);
            Assert.assertNotNull(qty);
            Assert.assertTrue("INCLUDE list missing price writer index " + stage,
                    covering.contains(price.getWriterIndex()));
            Assert.assertTrue("INCLUDE list missing qty writer index " + stage,
                    covering.contains(qty.getWriterIndex()));
        }
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

    private static long assertTableNamesOrderedWith(CharSequenceObjSortedHashMap<CairoTable> sortedMap, String expected, long version) {
        try (MetadataCacheReader metadataCacheReader = engine.getMetadataCache().readLock()) {
            version = metadataCacheReader.snapshot(sortedMap, version);

            sink.clear();
            sortedMap.keys().toSink(sink);

            TestUtils.assertEquals(expected, sink);

            return version;
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

    private File metaFile(String tableName) {
        final TableToken token = engine.getTableTokenIfExists(tableName);
        Assert.assertNotNull("table not found: " + tableName, token);
        return new File(
                new File(engine.getConfiguration().getDbRoot(), token.getDirName()),
                TableUtils.META_FILE_NAME
        );
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
