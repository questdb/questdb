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

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
abstract class BaseLineTcpInsertGeoHashTest extends BaseLineTcpContextTest {
    static final String tableName = "tracking";
    static final String targetColumnName = "geohash";

    private final boolean walEnabled;

    public BaseLineTcpInsertGeoHashTest(WalMode walMode) {
        this.walEnabled = (walMode == WalMode.WITH_WAL);
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { WalMode.WITH_WAL }, { WalMode.NO_WAL }
        });
    }

    @Before
    public void setUp() {
        defaultTableWriteMode = walEnabled ? 1 : 0;
        super.setUp();
    }

    @Test
    public abstract void testGeoHashes() throws Exception;

    @Test
    public abstract void testGeoHashesTruncating() throws Exception;

    @Test
    public abstract void testTableHasGeoHashMessageDoesNot() throws Exception;

    @Test
    public abstract void testExcessivelyLongGeoHashesAreTruncated() throws Exception;

    @Test
    public abstract void testGeoHashesNotEnoughPrecision() throws Exception;

    @Test
    public abstract void testWrongCharGeoHashes() throws Exception;

    @Test
    public abstract void testNullGeoHash() throws Exception;

    protected void assertGeoHash(int columnBits, String inboundLines, String expected) throws Exception {
        assertGeoHash(columnBits, inboundLines, expected, (String[]) null);
    }

    protected void assertGeoHash(int columnBits,
                                 String inboundLines,
                                 String expected,
                                 String... expectedExtraStringColumns) throws Exception {
        runInContext(() -> {
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                model.col(targetColumnName, ColumnType.getGeoHashTypeWithBits(columnBits)).timestamp();
                engine.createTableUnsafe(AllowAllCairoSecurityContext.INSTANCE, model.getMem(), model.getPath(), model);
            }
            if (walEnabled) {
                Assert.assertTrue(isWalTable(tableName));
            }
            recvBuffer = inboundLines;
            handleContextIO();
            waitForIOCompletion();
            closeContext();
            mayDrainWalQueue();
            assertTable(expected, tableName);
            if (expectedExtraStringColumns != null) {
                try (TableReader reader = new TableReader(configuration, tableName)) {
                    TableReaderMetadata meta = reader.getMetadata();
                    Assert.assertEquals(2 + expectedExtraStringColumns.length, meta.getColumnCount());
                    for (String colName : expectedExtraStringColumns) {
                        Assert.assertEquals(ColumnType.STRING, meta.getColumnType(colName));
                    }
                }
            }
        });
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }
}
