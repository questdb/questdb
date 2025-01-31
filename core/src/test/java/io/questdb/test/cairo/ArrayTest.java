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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ArrayTest extends AbstractCairoTest {

    @Test
    public void testCreateTableAllSupportedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (" +
                    "d double[][][]" +
                    ", b boolean[][][][]" +
                    ", bt byte[][][][][][][][]" +
                    ", f float[]" +
                    ", i int[][]" +
                    ", l long[][]" +
                    ", s short[][][][][]" +
                    ", dt date[][][][]" +
                    ", ts timestamp[][]" +
                    ", l2 long256[][][]" +
                    ", u uuid[][][][]" +
                    ", ip ipv4[][]" +
                    ", c double)");

            String[] expectedColumnNames = {
                    "d",
                    "b",
                    "bt",
                    "f",
                    "i",
                    "l",
                    "s",
                    "dt",
                    "ts",
                    "l2",
                    "u",
                    "ip",
                    "c"
            };

            String[] expectedColumnTypes = {
                    "DOUBLE[][][]",
                    "BOOLEAN[][][][]",
                    "BYTE[][][][][][][][]",
                    "FLOAT[]",
                    "INT[][]",
                    "LONG[][]",
                    "SHORT[][][][][]",
                    "DATE[][][][]",
                    "TIMESTAMP[][]",
                    "LONG256[][][]",
                    "UUID[][][][]",
                    "IPv4[][]",
                    "DOUBLE"
            };

            Assert.assertEquals(expectedColumnNames.length, expectedColumnTypes.length);
            // check the metadata
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("x"))) {
                Assert.assertEquals(expectedColumnNames.length, m.getColumnCount());

                for (int i = 0, n = expectedColumnNames.length; i < n; i++) {
                    Assert.assertEquals(expectedColumnNames[i], m.getColumnName(i));
                    Assert.assertEquals(expectedColumnTypes[i], ColumnType.nameOf(m.getColumnType(i)));
                }
            }
        });
    }

    @Test
    public void testUnsupportedArrayType() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "create table x (a symbol[][][])",
                    18,
                    "SYMBOL array type is not supported"
            );
            assertExceptionNoLeakCheck(
                    "create table x (abc varchar[][][])",
                    20,
                    "VARCHAR array type is not supported"
            );
        });
    }
}
