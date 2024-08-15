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

import io.questdb.cairo.CairoMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

// todo: fuzzers!
public class CairoMetadataTest extends AbstractCairoTest {


    private static String xMetaString = "CairoMetadata [tableCount=1]\n" +
            "\tCairoTable [name=x, id=1, directoryName=x~, isDedup=false, isSoftLink=false, lastMetadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=NONE, timestampIndex=3, timestampName=timestamp, walEnabled=false, columnCount=16]\n" +
            "\t\tCairoColumn [name=i, position=0, type=INT, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
            "\t\tCairoColumn [name=sym, position=0, type=SYMBOL, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, denseSymbolIndex=0, isIndexed=false, indexBlockCapacity=0, stableIndex=1, writerIndex=1]\n" +
            "\t\tCairoColumn [name=amt, position=1, type=DOUBLE, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=2, writerIndex=2]\n" +
            "\t\tCairoColumn [name=timestamp, position=2, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=3, writerIndex=3]\n" +
            "\t\tCairoColumn [name=b, position=3, type=BOOLEAN, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=4, writerIndex=4]\n" +
            "\t\tCairoColumn [name=c, position=4, type=STRING, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=5, writerIndex=5]\n" +
            "\t\tCairoColumn [name=d, position=5, type=DOUBLE, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=6, writerIndex=6]\n" +
            "\t\tCairoColumn [name=e, position=6, type=FLOAT, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=7, writerIndex=7]\n" +
            "\t\tCairoColumn [name=f, position=7, type=SHORT, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=8, writerIndex=8]\n" +
            "\t\tCairoColumn [name=g, position=8, type=DATE, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=9, writerIndex=9]\n" +
            "\t\tCairoColumn [name=ik, position=9, type=SYMBOL, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=true, symbolCapacity=128, denseSymbolIndex=1, isIndexed=false, indexBlockCapacity=0, stableIndex=10, writerIndex=10]\n" +
            "\t\tCairoColumn [name=j, position=10, type=LONG, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=11, writerIndex=11]\n" +
            "\t\tCairoColumn [name=k, position=11, type=TIMESTAMP, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=12, writerIndex=12]\n" +
            "\t\tCairoColumn [name=l, position=12, type=BYTE, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=13, writerIndex=13]\n" +
            "\t\tCairoColumn [name=m, position=13, type=BINARY, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=14, writerIndex=14]\n" +
            "\t\tCairoColumn [name=n, position=14, type=STRING, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=15, writerIndex=15]\n";
    private static String yMetaString = "CairoMetadata [tableCount=1]\n" +
            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=1]\n" +
            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n";

    @Test
    public void testAlterTableAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            CairoMetadata.INSTANCE.clear();
            createY();

            TestUtils.assertEquals(yMetaString,
                    CairoMetadata.INSTANCE.toString0());

            ddl("ALTER TABLE y ADD COLUMN foo VARCHAR");
            drainWalQueue();

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=foo, position=1, type=VARCHAR, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=false, symbolCached=true, symbolCapacity=128, denseSymbolIndex=0, isIndexed=false, indexBlockCapacity=256, stableIndex=-1, writerIndex=1]\n",
                    CairoMetadata.INSTANCE.toString0());
        });
    }

    @Test
    public void testAlterTableColumnAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            CairoMetadata.INSTANCE.clear();
            createY();

            TestUtils.assertEquals(yMetaString,
                    CairoMetadata.INSTANCE.toString0());

            ddl("ALTER TABLE y ADD COLUMN foo SYMBOL");

            drainWalQueue();

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=false, symbolCached=true, symbolCapacity=128, denseSymbolIndex=0, isIndexed=false, indexBlockCapacity=256, stableIndex=-1, writerIndex=1]\n",
                    CairoMetadata.INSTANCE.toString0());

            ddl("ALTER TABLE y ALTER COLUMN foo ADD INDEX");
            drainWalQueue();

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=false, symbolCached=false, symbolCapacity=0, denseSymbolIndex=0, isIndexed=true, indexBlockCapacity=256, stableIndex=-1, writerIndex=1]\n",
                    CairoMetadata.INSTANCE.toString0());
        });
    }

    @Test
    public void testAlterTableColumnDropIndex() throws Exception {
        assertMemoryLeak(() -> {
            CairoMetadata.INSTANCE.clear();
            createY();

            TestUtils.assertEquals(yMetaString,
                    CairoMetadata.INSTANCE.toString0());

            ddl("ALTER TABLE y ADD COLUMN foo SYMBOL");

            drainWalQueue();

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=1, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=false, symbolCached=true, symbolCapacity=128, denseSymbolIndex=0, isIndexed=false, indexBlockCapacity=256, stableIndex=-1, writerIndex=1]\n",
                    CairoMetadata.INSTANCE.toString0());

            ddl("ALTER TABLE y ALTER COLUMN foo ADD INDEX");
            drainWalQueue();

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=false, symbolCached=false, symbolCapacity=0, denseSymbolIndex=0, isIndexed=true, indexBlockCapacity=256, stableIndex=-1, writerIndex=1]\n",
                    CairoMetadata.INSTANCE.toString0());

            ddl("ALTER TABLE y ALTER COLUMN foo DROP INDEX");
            drainWalQueue();

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=3, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=false, symbolCached=false, symbolCapacity=0, denseSymbolIndex=0, isIndexed=false, indexBlockCapacity=256, stableIndex=-1, writerIndex=1]\n",
                    CairoMetadata.INSTANCE.toString0());

        });
    }

    @Test
    public void testAlterTableColumnType() throws Exception {
        assertMemoryLeak(() -> {
            CairoMetadata.INSTANCE.clear();
            createY();

            TestUtils.assertEquals(yMetaString,
                    CairoMetadata.INSTANCE.toString0());

            ddl("ALTER TABLE y ADD COLUMN foo VARCHAR");

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=1]\n" +
                            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n",
                    CairoMetadata.INSTANCE.toString0());

            ddl("ALTER TABLE y ALTER COLUMN foo TYPE SYMBOL");
            drainWalQueue();

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=2, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=foo, position=1, type=SYMBOL, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=false, symbolCached=true, symbolCapacity=128, denseSymbolIndex=0, isIndexed=false, indexBlockCapacity=256, stableIndex=-1, writerIndex=1]\n",
                    CairoMetadata.INSTANCE.toString0());
        });
    }

    @Test
    public void testAlterTableDedupDisable() throws Exception {
        assertMemoryLeak(() -> {
            CairoMetadata.INSTANCE.clear();

            ddl("CREATE TABLE y ( ts TIMESTAMP, foo INT ) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, foo)");
            drainWalQueue();

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                            "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=true, isSoftLink=false, lastMetadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                            "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=true, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
                            "\t\tCairoColumn [name=foo, position=0, type=INT, isDedupKey=true, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=1, writerIndex=1]\n",
                    CairoMetadata.INSTANCE.toString0());

            ddl("ALTER TABLE y DEDUP DISABLE");
            drainWalQueue();

            TestUtils.assertEquals("CairoMetadata [tableCount=1]\n" +
                    "\tCairoTable [name=y, id=1, directoryName=y~1, isDedup=false, isSoftLink=false, lastMetadataVersion=0, maxUncommittedRows=1000, o3MaxLag=300000000, partitionBy=DAY, timestampIndex=0, timestampName=ts, walEnabled=true, columnCount=2]\n" +
                    "\t\tCairoColumn [name=ts, position=0, type=TIMESTAMP, isDedupKey=false, isDesignated=true, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=0, writerIndex=0]\n" +
                    "\t\tCairoColumn [name=foo, position=0, type=INT, isDedupKey=false, isDesignated=false, isSequential=false, isSymbolTableStatic=true, symbolCached=false, symbolCapacity=0, denseSymbolIndex=-1, isIndexed=false, indexBlockCapacity=0, stableIndex=1, writerIndex=1]\n", CairoMetadata.INSTANCE.toString0());
        });
    }

    @Test
    public void testBasicMetadata() throws Exception {
        assertMemoryLeak(() -> {
            CairoMetadata.INSTANCE.clear();
            createX();

            TestUtils.assertEquals(xMetaString,
                    CairoMetadata.INSTANCE.toString0());


        });
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

}
