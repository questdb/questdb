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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Inserts into decimal columns from CHAR and VARCHAR sources, against every record-to-row copier
 * implementation. The narrow table stays under the bytecode method size limit, the wide one exceeds it
 * and falls back to the chunked or the looping copier depending on configuration.
 */
@RunWith(Parameterized.class)
public class InsertDecimalFromTextTest extends AbstractCairoTest {
    // varchar -> decimal is estimated at 38 bytecode bytes per column, so 250 columns exceed the 8000 byte limit
    private static final int WIDE_COLUMN_COUNT = 250;
    private final CopierMode copierMode;

    public InsertDecimalFromTextTest(CopierMode copierMode) {
        this.copierMode = copierMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {CopierMode.BYTECODE},
                {CopierMode.CHUNKED},
                {CopierMode.LOOPING}
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPIER_CHUNKED, copierMode == CopierMode.CHUNKED);
    }

    @Test
    public void testCharColumnIntoDecimal() throws Exception {
        // rejected at compile time, so the copier flavour is irrelevant here
        assertMemoryLeak(() -> {
            execute("create table src (c char)");
            execute("create table dst (d decimal(10,2))");
            execute("insert into src values ('5')");

            assertExceptionNoLeakCheck("insert into dst select c from src", 23, "inconvertible types: CHAR ->");
        });
    }

    @Test
    public void testCharLiteralIntoDecimal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dst (d decimal(10,2))");

            assertExceptionNoLeakCheck("insert into dst values ('a')", 24, "inconvertible types: CHAR ->");
            assertExceptionNoLeakCheck("insert into dst values ('5')", 24, "inconvertible types: CHAR ->");
        });
    }

    @Test
    public void testVarcharIntoDecimal() throws Exception {
        assertMemoryLeak(() -> {
            createTables("varchar", "decimal(10,2)");
            insertRow("'123.45'");
            insertRow("null");

            execute("insert into dst select * from src");

            assertQuery("select c0 from dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns("c0\n123.45\n\n");

            String lastColumn = "c" + (columnCount() - 1);
            assertQuery("select " + lastColumn + " from dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns(lastColumn + "\n123.45\n\n");
        });
    }

    @Test
    public void testVarcharIntoDecimalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            createTables("varchar", "decimal(10,2)");
            insertRow("'abc'");

            assertExceptionNoLeakCheck("insert into dst select * from src", -1, "inconvertible value: `abc`");
        });
    }

    @Test
    public void testVarcharIntoDecimalOverflow() throws Exception {
        assertMemoryLeak(() -> {
            createTables("varchar", "decimal(4,2)");
            insertRow("'12345.67'");

            assertExceptionNoLeakCheck("insert into dst select * from src", -1, "inconvertible value: `12345.67`");
        });
    }

    private int columnCount() {
        return copierMode == CopierMode.BYTECODE ? 1 : WIDE_COLUMN_COUNT;
    }

    private void createTables(String srcType, String dstType) throws Exception {
        execute(createTableSql("src", srcType));
        execute(createTableSql("dst", dstType));
    }

    private String createTableSql(String tableName, String type) {
        StringBuilder b = new StringBuilder("create table ").append(tableName).append(" (");
        for (int i = 0, n = columnCount(); i < n; i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append('c').append(i).append(' ').append(type);
        }
        return b.append(')').toString();
    }

    private void insertRow(String value) throws Exception {
        StringBuilder b = new StringBuilder("insert into src values (");
        for (int i = 0, n = columnCount(); i < n; i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(value);
        }
        execute(b.append(')').toString());
    }

    /**
     * Copier implementation exercised by the table shape and the chunking flag.
     */
    public enum CopierMode {
        BYTECODE,
        CHUNKED,
        LOOPING
    }
}
