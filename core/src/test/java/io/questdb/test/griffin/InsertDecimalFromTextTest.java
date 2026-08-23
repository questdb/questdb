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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.LoopingRecordToRowCopier;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.std.BytecodeAssembler;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

/**
 * Inserts into decimal columns from CHAR, STRING and VARCHAR sources, against every record-to-row copier
 * implementation and against both row writers. {@code debug.cairo.copier.type} forces one copier per
 * parameter, and {@link #assertForcedCopier()} asserts that the forced one is what
 * {@code RecordToRowCopierUtils.generateCopier} actually builds for the fixture, so a mode that degrades
 * to another implementation turns the test red. The plain tables drive {@code TableWriter.RowImpl}, the
 * WAL ones {@code WalWriter.RowImpl}.
 */
@RunWith(Parameterized.class)
public class InsertDecimalFromTextTest extends AbstractCairoTest {
    // the chunked copier splits only when a chunk would exceed CHUNK_TARGET_SIZE (6000 bytes) and falls
    // back to the single-method copier otherwise; text -> decimal is estimated at 38 bytes per column
    private static final int CHUNKED_COLUMN_COUNT = 160;
    // one column per decimal storage width
    private static final String WIDTH_TABLE_DDL =
            "CREATE TABLE dst (d0 DECIMAL(2,1), d1 DECIMAL(4,2), d2 DECIMAL(9,2), d3 DECIMAL(18,2), d4 DECIMAL(38,2), d5 DECIMAL(40,2))";
    private final CopierMode copierMode;

    public InsertDecimalFromTextTest(CopierMode copierMode) {
        this.copierMode = copierMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {CopierMode.CHUNKED},
                {CopierMode.LOOPING},
                {CopierMode.SINGLE_METHOD}
        });
    }

    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, copierMode.copierType);
    }

    @Test
    public void testCharBindVariableIntoDecimal() throws Exception {
        // a runtime constant is not folded at compile time, so the value crosses the copier
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dst (d DECIMAL(10,2))");
            bindVariableService.clear();
            bindVariableService.define(0, ColumnType.CHAR, 0);

            bindVariableService.setChar(0, '7');
            execute("INSERT INTO dst VALUES ($1)");

            bindVariableService.setChar(0, 'z');
            assertExceptionNoLeakCheck("INSERT INTO dst VALUES ($1)", -1, "inconvertible value: z [CHAR -> DECIMAL(10,2)]");

            assertQuery("SELECT d FROM dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns("d\n7.00\n");
        });
    }

    @Test
    public void testCharColumnIntoDecimal() throws Exception {
        assertMemoryLeak(() -> {
            createTables("CHAR", "DECIMAL(10,2)");
            insertRows("'5'", "'0'", "NULL");

            execute("INSERT INTO dst SELECT * FROM src");

            assertFirstAndLastColumn("5.00\n0.00\n\n");
        });
    }

    @Test
    public void testCharColumnIntoDecimalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            createTables("CHAR", "DECIMAL(10,2)");
            insertRows("'a'");

            assertExceptionNoLeakCheck(
                    "INSERT INTO dst SELECT * FROM src",
                    -1,
                    "inconvertible value: a [CHAR -> DECIMAL(10,2)]"
            );
        });
    }

    @Test
    public void testCharColumnIntoDecimalWal() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("CHAR", "DECIMAL(10,2)");
            insertWalRows("'5'", "NULL");
            drainWalQueue();

            execute("INSERT INTO dst SELECT * FROM src");
            drainWalQueue();

            assertFirstAndLastColumn("5.00\n\n");
        });
    }

    @Test
    public void testCharColumnIntoDecimalWalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("CHAR", "DECIMAL(10,2)");
            insertWalRows("'a'");
            drainWalQueue();

            assertExceptionNoLeakCheck(
                    "INSERT INTO dst SELECT * FROM src",
                    -1,
                    "inconvertible value: a [CHAR -> DECIMAL(10,2)]"
            );

            drainWalQueue();
            assertFirstAndLastColumn("");
        });
    }

    @Test
    public void testCharLiteralIntoDecimal() throws Exception {
        // a one-character quoted literal types as CHAR, a longer one as VARCHAR
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dst (small DECIMAL(2,1), mid DECIMAL(18,4), big DECIMAL(38,10))");

            execute("""
                    INSERT INTO dst VALUES
                    ('5', '7', '9'),
                    ('0', '0', '0'),
                    (NULL, NULL, NULL)""");

            assertQuery("SELECT * FROM dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            small\tmid\tbig
                            5.0\t7.0000\t9.0000000000
                            0.0\t0.0000\t0.0000000000
                            \t\t
                            """);
        });
    }

    @Test
    public void testCharLiteralIntoDecimalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dst (d DECIMAL(10,2))");

            assertExceptionNoLeakCheck("INSERT INTO dst VALUES ('a')", -1, "inconvertible value: a [CHAR -> DECIMAL(10,2)]");
            assertExceptionNoLeakCheck("INSERT INTO dst VALUES ('-')", -1, "inconvertible value: - [CHAR -> DECIMAL(10,2)]");
            assertExceptionNoLeakCheck("INSERT INTO dst VALUES ('.')", -1, "inconvertible value: . [CHAR -> DECIMAL(10,2)]");
            assertExceptionNoLeakCheck("INSERT INTO dst VALUES (' ')", -1, "inconvertible value:   [CHAR -> DECIMAL(10,2)]");

            assertQuery("SELECT d FROM dst").noLeakCheck().expectSize().returns("d\n");
        });
    }

    @Test
    public void testCharLiteralIntoDecimalOverflow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dst (d DECIMAL(1,1))");

            assertExceptionNoLeakCheck("INSERT INTO dst VALUES ('5')", -1, "inconvertible value: 5 [CHAR -> DECIMAL(1,1)]");
            execute("INSERT INTO dst VALUES ('0')");

            assertQuery("SELECT d FROM dst").noLeakCheck().expectSize().returns("d\n0.0\n");
        });
    }

    @Test
    public void testStringIntoDecimalNanAndInfinity() throws Exception {
        assertMemoryLeak(() -> {
            createTables("STRING", "DECIMAL(10,2)");
            insertRows("'NaN'", "'Infinity'", "'-Infinity'", "'+Infinity'");

            execute("INSERT INTO dst SELECT * FROM src");

            assertFirstAndLastColumn("\n\n\n\n");
        });
    }

    @Test
    public void testStringIntoDecimalNanAndInfinityAtEveryWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (v STRING)");
            execute("INSERT INTO src VALUES ('NaN'), ('Infinity'), ('-Infinity'), ('+Infinity')");
            execute(WIDTH_TABLE_DDL);

            execute("INSERT INTO dst SELECT v, v, v, v, v, v FROM src");

            assertQuery("SELECT * FROM dst").noLeakCheck().expectSize().returns(everyWidthNulls(4));
        });
    }

    @Test
    public void testStringIntoDecimalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            createTables("STRING", "DECIMAL(10,2)");
            insertRows("'abc'");

            assertExceptionNoLeakCheck(
                    "INSERT INTO dst SELECT * FROM src",
                    -1,
                    "inconvertible value: `abc` [STRING -> DECIMAL(10,2)]"
            );
        });
    }

    @Test
    public void testVarcharIntoDecimal() throws Exception {
        assertMemoryLeak(() -> {
            createTables("VARCHAR", "DECIMAL(10,2)");
            insertRows("'123.45'", "NULL");

            execute("INSERT INTO dst SELECT * FROM src");

            assertFirstAndLastColumn("123.45\n\n");
        });
    }

    @Test
    public void testVarcharIntoDecimalNanAndInfinity() throws Exception {
        assertMemoryLeak(() -> {
            createTables("VARCHAR", "DECIMAL(10,2)");
            insertRows("'NaN'", "'Infinity'", "'-Infinity'", "'+Infinity'");

            execute("INSERT INTO dst SELECT * FROM src");

            assertFirstAndLastColumn("\n\n\n\n");
        });
    }

    @Test
    public void testVarcharIntoDecimalNanAndInfinityAtEveryWidth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (v VARCHAR)");
            execute("INSERT INTO src VALUES ('NaN'), ('Infinity'), ('-Infinity'), ('+Infinity')");
            execute(WIDTH_TABLE_DDL);

            // a quoted literal of more than one character types as VARCHAR, so VALUES goes through the same copier
            execute("INSERT INTO dst VALUES ('NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN')");
            assertQuery("SELECT * FROM dst").noLeakCheck().expectSize().returns(everyWidthNulls(1));
            execute("TRUNCATE TABLE dst");

            execute("INSERT INTO dst SELECT v, v, v, v, v, v FROM src");

            assertQuery("SELECT * FROM dst").noLeakCheck().expectSize().returns(everyWidthNulls(4));
        });
    }

    @Test
    public void testVarcharIntoDecimalNanAndInfinityWal() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("VARCHAR", "DECIMAL(10,2)");
            insertWalRows("'NaN'", "'-Infinity'");
            drainWalQueue();

            execute("INSERT INTO dst SELECT * FROM src");
            drainWalQueue();

            assertFirstAndLastColumn("\n\n");
        });
    }

    @Test
    public void testVarcharIntoDecimalNonAscii() throws Exception {
        assertMemoryLeak(() -> {
            createTables("VARCHAR", "DECIMAL(10,2)");

            // fullwidth digits are not decimal digits, and the message must show them undamaged
            insertRows("'１２３'");
            assertExceptionNoLeakCheck(
                    "INSERT INTO dst SELECT * FROM src",
                    -1,
                    "inconvertible value: `１２３` [VARCHAR -> DECIMAL(10,2)]"
            );

            execute("TRUNCATE TABLE src");
            insertRows("'12.3€'");
            assertExceptionNoLeakCheck(
                    "INSERT INTO dst SELECT * FROM src",
                    -1,
                    "inconvertible value: `12.3€` [VARCHAR -> DECIMAL(10,2)]"
            );
        });
    }

    @Test
    public void testVarcharIntoDecimalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            createTables("VARCHAR", "DECIMAL(10,2)");
            insertRows("'abc'");

            assertExceptionNoLeakCheck(
                    "INSERT INTO dst SELECT * FROM src",
                    -1,
                    "inconvertible value: `abc` [VARCHAR -> DECIMAL(10,2)]"
            );
        });
    }

    @Test
    public void testVarcharIntoDecimalOverflow() throws Exception {
        assertMemoryLeak(() -> {
            createTables("VARCHAR", "DECIMAL(4,2)");
            insertRows("'12345.67'");

            assertExceptionNoLeakCheck(
                    "INSERT INTO dst SELECT * FROM src",
                    -1,
                    "inconvertible value: `12345.67` [VARCHAR -> DECIMAL(4,2)]"
            );
        });
    }

    @Test
    public void testVarcharIntoDecimalWal() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("VARCHAR", "DECIMAL(10,2)");
            insertWalRows("'123.45'", "NULL");
            drainWalQueue();

            execute("INSERT INTO dst SELECT * FROM src");
            drainWalQueue();

            assertFirstAndLastColumn("123.45\n\n");
        });
    }

    @Test
    public void testVarcharIntoDecimalWalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("VARCHAR", "DECIMAL(10,2)");
            insertWalRows("'abc'");
            drainWalQueue();

            assertExceptionNoLeakCheck(
                    "INSERT INTO dst SELECT * FROM src",
                    -1,
                    "inconvertible value: `abc` [VARCHAR -> DECIMAL(10,2)]"
            );

            drainWalQueue();
            assertFirstAndLastColumn("");
        });
    }

    @Test
    public void testVarcharIntoDecimalWalOverflow() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("VARCHAR", "DECIMAL(4,2)");
            insertWalRows("'12345.67'");
            drainWalQueue();

            assertExceptionNoLeakCheck(
                    "INSERT INTO dst SELECT * FROM src",
                    -1,
                    "inconvertible value: `12345.67` [VARCHAR -> DECIMAL(4,2)]"
            );
        });
    }

    /**
     * Reports which implementation {@code generateCopier} returned. The single-method and the chunked
     * copiers are both generated under the same class name, but only the chunked one splits the column
     * copies into private {@code copy0..copyN} sub-methods.
     */
    private static CopierMode copierModeOf(RecordToRowCopier copier) {
        if (copier instanceof LoopingRecordToRowCopier) {
            return CopierMode.LOOPING;
        }
        for (Method method : copier.getClass().getDeclaredMethods()) {
            if (method.getName().startsWith("copy") && !"copy".equals(method.getName())) {
                return CopierMode.CHUNKED;
            }
        }
        return CopierMode.SINGLE_METHOD;
    }

    private static String everyWidthNulls(int rowCount) {
        return "d0\td1\td2\td3\td4\td5\n" + "\t\t\t\t\t\n".repeat(rowCount);
    }

    private void assertFirstAndLastColumn(String values) throws Exception {
        assertQuery("SELECT c0 FROM dst").noLeakCheck().expectSize().returns("c0\n" + values);
        int last = columnCount() - 1;
        if (last > 0) {
            // c0 and the last column land in different sub-methods of the chunked copier
            assertQuery("SELECT c" + last + " FROM dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns("c" + last + "\n" + values);
        }
    }

    /**
     * Asserts that the copier the compiler builds for the src -> dst pair is the one this parameter forces,
     * using the same metadata, column filter and configuration the INSERT compilation uses.
     */
    private void assertForcedCopier() {
        try (
                TableReader src = getReader("src");
                TableReader dst = getReader("dst")
        ) {
            EntityColumnFilter columnFilter = new EntityColumnFilter();
            columnFilter.of(dst.getMetadata().getColumnCount());
            RecordToRowCopier copier = RecordToRowCopierUtils.generateCopier(
                    new BytecodeAssembler(),
                    src.getMetadata(),
                    dst.getMetadata(),
                    columnFilter,
                    configuration
            );
            Assert.assertEquals(copierMode, copierModeOf(copier));
        }
    }

    private int columnCount() {
        return copierMode == CopierMode.CHUNKED ? CHUNKED_COLUMN_COUNT : 1;
    }

    private void createTables(String srcType, String dstType) throws Exception {
        execute(createTableSql("src", srcType, false));
        execute(createTableSql("dst", dstType, false));
        assertForcedCopier();
    }

    private String createTableSql(String tableName, String type, boolean isWal) {
        StringBuilder b = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
        for (int i = 0, n = columnCount(); i < n; i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append('c').append(i).append(' ').append(type);
        }
        if (isWal) {
            return b.append(", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL").toString();
        }
        return b.append(')').toString();
    }

    private void createWalTables(String srcType, String dstType) throws Exception {
        execute(createTableSql("src", srcType, true));
        execute(createTableSql("dst", dstType, true));
        assertForcedCopier();
    }

    private void insertRows(String... values) throws Exception {
        execute(insertSql(false, values));
    }

    private String insertSql(boolean isWal, String... values) {
        StringBuilder b = new StringBuilder("INSERT INTO src VALUES ");
        for (int row = 0; row < values.length; row++) {
            if (row > 0) {
                b.append(", ");
            }
            b.append('(');
            for (int i = 0, n = columnCount(); i < n; i++) {
                if (i > 0) {
                    b.append(", ");
                }
                b.append(values[row]);
            }
            if (isWal) {
                // one row per second, so the rows read back in the order they were passed
                b.append(", '2024-01-01T00:00:").append(String.format("%02d", row)).append(".000000Z'");
            }
            b.append(')');
        }
        return b.toString();
    }

    private void insertWalRows(String... values) throws Exception {
        execute(insertSql(true, values));
    }

    /**
     * The copier implementation this parameter forces through {@code debug.cairo.copier.type}.
     */
    public enum CopierMode {
        CHUNKED(RecordToRowCopierUtils.COPIER_TYPE_CHUNKED),
        LOOPING(RecordToRowCopierUtils.COPIER_TYPE_LOOPING),
        SINGLE_METHOD(RecordToRowCopierUtils.COPIER_TYPE_SINGLE_METHOD);

        private final int copierType;

        CopierMode(int copierType) {
            this.copierType = copierType;
        }
    }
}
