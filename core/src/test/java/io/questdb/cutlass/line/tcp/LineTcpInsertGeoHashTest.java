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
import org.junit.Assert;
import org.junit.Test;

abstract class LineTcpInsertGeoHashTest extends BaseLineTcpContextTest {
    static final String tableName = "tracking";
    static final String targetColumnName = "geohash";

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
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
                CairoTestUtils.create(model.col(targetColumnName, ColumnType.getGeoHashTypeWithBits(columnBits)).timestamp());
            }
            recvBuffer = inboundLines;
            handleContextIO();
            waitForIOCompletion();
            closeContext();
            assertTable(expected, tableName);
            if (expectedExtraStringColumns != null) {
                try (TableReader reader = newTableReader(configuration, tableName)) {
                    TableReaderMetadata meta = reader.getMetadata();
                    Assert.assertEquals(2 + expectedExtraStringColumns.length, meta.getColumnCount());
                    for (String colName : expectedExtraStringColumns) {
                        Assert.assertEquals(ColumnType.STRING, meta.getColumnType(colName));
                    }
                }
            }
        });
    }
}
