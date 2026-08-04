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
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Inserts into decimal columns from CHAR, STRING and VARCHAR sources, against every record-to-row copier
 * implementation and against both row writers. Only the wide tables select a copier: they exceed the
 * bytecode method size limit and fall back to the chunked or the looping copier depending on configuration,
 * while the narrow ones stay under it and compile to the single-method copier in every mode.
 * The plain tables drive {@code TableWriter.RowImpl}, the WAL ones {@code WalWriter.RowImpl}.
 */
@RunWith(Parameterized.class)
public class InsertDecimalFromTextTest extends AbstractCairoTest {
    // char/varchar -> decimal is estimated at 38 bytecode bytes per column, so 250 columns exceed the 8000 byte limit
    private static final int WIDE_COLUMN_COUNT = 250;
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
            createTables("char", "decimal(10,2)");
            insertRow("'5'");
            insertRow("'0'");
            insertRow("null");

            execute("insert into dst select * from src");

            assertQuery("select c0 from dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns("c0\n5.00\n0.00\n\n");

            String lastColumn = "c" + (columnCount() - 1);
            assertQuery("select " + lastColumn + " from dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns(lastColumn + "\n5.00\n0.00\n\n");
        });
    }

    @Test
    public void testCharColumnIntoDecimalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            createTables("char", "decimal(10,2)");
            insertRow("'a'");

            assertExceptionNoLeakCheck(
                    "insert into dst select * from src",
                    -1,
                    "inconvertible value: a [CHAR -> DECIMAL(10,2)]"
            );
        });
    }

    @Test
    public void testCharColumnIntoDecimalWal() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("char", "decimal(10,2)");
            insertRow("'5'", "'2024-01-01T00:00:00.000000Z'");
            insertRow("null", "'2024-01-01T00:00:01.000000Z'");
            drainWalQueue();

            execute("insert into dst select * from src");
            drainWalQueue();

            assertQuery("select c0 from dst").noLeakCheck().expectSize().returns("c0\n5.00\n\n");

            String lastColumn = "c" + (columnCount() - 1);
            assertQuery("select " + lastColumn + " from dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns(lastColumn + "\n5.00\n\n");
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
            createTables("string", "decimal(10,2)");
            insertRow("'NaN'");
            insertRow("'Infinity'");
            insertRow("'-Infinity'");
            insertRow("'+Infinity'");

            execute("insert into dst select * from src");

            assertQuery("select c0 from dst").noLeakCheck().expectSize().returns("c0\n\n\n\n\n");

            String lastColumn = "c" + (columnCount() - 1);
            assertQuery("select " + lastColumn + " from dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns(lastColumn + "\n\n\n\n\n");
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
            createTables("string", "decimal(10,2)");
            insertRow("'abc'");

            assertExceptionNoLeakCheck(
                    "insert into dst select * from src",
                    -1,
                    "inconvertible value: `abc` [STRING -> DECIMAL(10,2)]"
            );
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
    public void testVarcharIntoDecimalNanAndInfinity() throws Exception {
        assertMemoryLeak(() -> {
            createTables("varchar", "decimal(10,2)");
            insertRow("'NaN'");
            insertRow("'Infinity'");
            insertRow("'-Infinity'");
            insertRow("'+Infinity'");

            execute("insert into dst select * from src");

            assertQuery("select c0 from dst").noLeakCheck().expectSize().returns("c0\n\n\n\n\n");

            String lastColumn = "c" + (columnCount() - 1);
            assertQuery("select " + lastColumn + " from dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns(lastColumn + "\n\n\n\n\n");
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
            createWalTables("varchar", "decimal(10,2)");
            insertRow("'NaN'", "'2024-01-01T00:00:00.000000Z'");
            insertRow("'-Infinity'", "'2024-01-01T00:00:01.000000Z'");
            drainWalQueue();

            execute("insert into dst select * from src");
            drainWalQueue();

            assertQuery("select c0 from dst").noLeakCheck().expectSize().returns("c0\n\n\n");

            String lastColumn = "c" + (columnCount() - 1);
            assertQuery("select " + lastColumn + " from dst")
                    .noLeakCheck()
                    .expectSize()
                    .returns(lastColumn + "\n\n\n");
        });
    }

    @Test
    public void testVarcharIntoDecimalNonAscii() throws Exception {
        assertMemoryLeak(() -> {
            createTables("varchar", "decimal(10,2)");

            // fullwidth digits are not decimal digits, and the message must show them undamaged
            insertRow("'１２３'");
            assertExceptionNoLeakCheck(
                    "insert into dst select * from src",
                    -1,
                    "inconvertible value: `１２３` [VARCHAR -> DECIMAL(10,2)]"
            );

            execute("truncate table src");
            insertRow("'12.3€'");
            assertExceptionNoLeakCheck(
                    "insert into dst select * from src",
                    -1,
                    "inconvertible value: `12.3€` [VARCHAR -> DECIMAL(10,2)]"
            );
        });
    }

    @Test
    public void testVarcharIntoDecimalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            createTables("varchar", "decimal(10,2)");
            insertRow("'abc'");

            assertExceptionNoLeakCheck(
                    "insert into dst select * from src",
                    -1,
                    "inconvertible value: `abc` [VARCHAR -> DECIMAL(10,2)]"
            );
        });
    }

    @Test
    public void testVarcharIntoDecimalOverflow() throws Exception {
        assertMemoryLeak(() -> {
            createTables("varchar", "decimal(4,2)");
            insertRow("'12345.67'");

            assertExceptionNoLeakCheck(
                    "insert into dst select * from src",
                    -1,
                    "inconvertible value: `12345.67` [VARCHAR -> DECIMAL(4,2)]"
            );
        });
    }

    @Test
    public void testVarcharIntoDecimalWal() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("varchar", "decimal(10,2)");
            insertRow("'123.45'", "'2024-01-01T00:00:00.000000Z'");
            insertRow("null", "'2024-01-01T00:00:01.000000Z'");
            drainWalQueue();

            execute("insert into dst select * from src");
            drainWalQueue();

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
    public void testVarcharIntoDecimalWalNonNumeric() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("varchar", "decimal(10,2)");
            insertRow("'abc'", "'2024-01-01T00:00:00.000000Z'");
            drainWalQueue();

            assertExceptionNoLeakCheck(
                    "insert into dst select * from src",
                    -1,
                    "inconvertible value: `abc` [VARCHAR -> DECIMAL(10,2)]"
            );

            drainWalQueue();
            assertQuery("select c0 from dst").noLeakCheck().expectSize().returns("c0\n");
        });
    }

    @Test
    public void testVarcharIntoDecimalWalOverflow() throws Exception {
        assertMemoryLeak(() -> {
            createWalTables("varchar", "decimal(4,2)");
            insertRow("'12345.67'", "'2024-01-01T00:00:00.000000Z'");
            drainWalQueue();

            assertExceptionNoLeakCheck(
                    "insert into dst select * from src",
                    -1,
                    "inconvertible value: `12345.67` [VARCHAR -> DECIMAL(4,2)]"
            );
        });
    }

    private static String everyWidthNulls(int rowCount) {
        return "d0\td1\td2\td3\td4\td5\n" + "\t\t\t\t\t\n".repeat(rowCount);
    }

    private int columnCount() {
        return copierMode == CopierMode.BYTECODE ? 1 : WIDE_COLUMN_COUNT;
    }

    private void createTables(String srcType, String dstType) throws Exception {
        execute(createTableSql("src", srcType, false));
        execute(createTableSql("dst", dstType, false));
    }

    private String createTableSql(String tableName, String type, boolean isWal) {
        StringBuilder b = new StringBuilder("create table ").append(tableName).append(" (");
        for (int i = 0, n = columnCount(); i < n; i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append('c').append(i).append(' ').append(type);
        }
        if (isWal) {
            return b.append(", ts timestamp) timestamp(ts) partition by day wal").toString();
        }
        return b.append(')').toString();
    }

    private void createWalTables(String srcType, String dstType) throws Exception {
        execute(createTableSql("src", srcType, true));
        execute(createTableSql("dst", dstType, true));
    }

    private void insertRow(String value) throws Exception {
        insertRow(value, null);
    }

    private void insertRow(String value, @Nullable String timestamp) throws Exception {
        StringBuilder b = new StringBuilder("insert into src values (");
        for (int i = 0, n = columnCount(); i < n; i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(value);
        }
        if (timestamp != null) {
            b.append(", ").append(timestamp);
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
