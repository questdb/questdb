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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.CreateTableFuzzTest.ColumnChaos.*;
import static io.questdb.test.griffin.CreateTableFuzzTest.WrongNameChoice.*;
import static io.questdb.test.tools.TestUtils.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class CreateTableFuzzTest extends AbstractCairoTest {
    private static final int INDEX_CAPACITY = 512;
    private static final int SYMBOL_CAPACITY = 64;
    private static final String WRONG_NAME = "bork";
    private final Rnd rnd = TestUtils.generateRandom(LOG);
    private final boolean messWithTableAfterCompile = rnd.nextBoolean();
    private final ColumnChaos columnChaos = ColumnChaos.values()[rnd.nextInt(ColumnChaos.values().length)];
    private final boolean addColumn = rnd.nextBoolean();
    private final boolean useCast = rnd.nextBoolean();
    private final boolean useCastSymbolCapacity = useCast && rnd.nextBoolean();
    private final boolean useIfNotExists = rnd.nextBoolean();
    private final boolean useIndex = rnd.nextBoolean();
    private final boolean useIndexCapacity = useIndex && rnd.nextBoolean();
    private final boolean usePartitionBy = rnd.nextBoolean();
    private final boolean useDedup = usePartitionBy && rnd.nextBoolean();
    private final boolean useSelectStar = rnd.nextBoolean();
    private final WrongNameChoice wrongName = WrongNameChoice.values()[rnd.nextInt(WrongNameChoice.values().length)];
    private String castCol = "str";
    private int castPos = -1;
    private int dedupPos = -1;
    private int defaultIndexCapacity;
    private int defaultSymbolCapacity;
    private String indexCol = "sym";
    private int indexPos = -1;
    private String strCol = "str";
    private int strPos = -1;
    private String symCol = "sym";
    private int symPos = -1;
    private String tsCol = "ts";
    private int tsPos = -1;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        defaultSymbolCapacity = engine.getConfiguration().getDefaultSymbolCapacity();
        defaultIndexCapacity = engine.getConfiguration().getIndexValueBlockSize();
    }

    @Test
    public void testCreateTableAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            StringBuilder ddl = generateCreateTable(true);
            System.out.printf(
                    "\n%s\nmessWithTableAfterCompile %s SelectStar %s WrongName %s Cast %s CastSymbolCapacity %s " +
                            "Dedup %s Index %s IndexCapacity %s PartitionBy %s\n",
                    ddl, messWithTableAfterCompile, useSelectStar, wrongName, useCast, useCastSymbolCapacity,
                    useDedup, useIndex, useIndexCapacity, usePartitionBy);
            System.out.printf("addColumn %s ColumnChaos %s\n\n", addColumn, columnChaos);
            createSourceTable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery query;
                try {
                    query = compiler.compile(ddl, sqlExecutionContext);
                } catch (SqlException e) {
                    String message = e.getMessage();
                    if (wrongName == NONE) {
                        throw new AssertionError("Compilation failed, but there were no wrong names in SQL", e);
                    }
                    int errPos = wrongName == CAST ? castPos
                            : wrongName == INDEX ? indexPos
                            : wrongName == TIMESTAMP ? tsPos
                            : wrongName == DEDUP ? dedupPos
                            : -1;
                    assertEquals(withErrPos(errPos, "Invalid column: " + WRONG_NAME), message);
                    return;
                }
                if (messWithTableAfterCompile) {
                    messWithSourceTable();
                }
                try (Operation op = query.getOperation()) {
                    assertNotNull(op);
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                        if (ddl.indexOf(WRONG_NAME) != -1) {
                            fail("SQL statement uses a wrong column name, yet execution didn't fail");
                        }
                    } catch (SqlException e) {
                        if (ddl.indexOf(WRONG_NAME) == -1) {
                            if (!messWithTableAfterCompile) {
                                throw new Exception("Didn't mess with table after compile, yet CREATE operation failed", e);
                            }
                            validateCreateAsSelectException(e);
                        }
                        return;
                    }
                    if (messWithTableAfterCompile) {
                        validateCreateAsSelectNoException();
                    }
                    validateCreatedTableAsSelect(messWithTableAfterCompile);
                    if (useIfNotExists) {
                        try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                            fut.await();
                        }
                        validateCreatedTableAsSelect(messWithTableAfterCompile);
                    }
                    if (!messWithTableAfterCompile) {
                        messWithSourceTable();
                    }
                    execute("DROP TABLE tango");
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    } catch (SqlException e) {
                        validateCreateAsSelectException(e);
                        return;
                    }
                    validateCreateAsSelectNoException();
                    validateCreatedTableAsSelectAfterMessing();
                }
            }
        });
    }

    @Test
    public void testCreateTableDirect() throws Exception {
        assertMemoryLeak(() -> {
            StringBuilder ddl = generateCreateTable(false);
            System.out.printf("\n%s\nWrongName %s Dedup %s PartitionBy %s\n\n", ddl, wrongName, useDedup, usePartitionBy);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery query;
                try {
                    query = compiler.compile(ddl, sqlExecutionContext);
                } catch (SqlException e) {
                    String message = e.getMessage();
                    if (wrongName == TIMESTAMP) {
                        assertEquals(withErrPos(tsPos, "invalid designated timestamp column [name=bork]"), message);
                    } else if (wrongName == DEDUP) {
                        assertEquals(withErrPos(dedupPos, "deduplicate key column not found [column=bork]"), message);
                    } else {
                        throw new AssertionError("Compiling failed with unexpected reason", e);
                    }
                    return;
                }
                if (ddl.indexOf(WRONG_NAME) != -1) {
                    fail("SQL statement uses a wrong column name, yet it compiled successfully");
                }
                try (Operation op = query.getOperation()) {
                    assertNotNull(op);
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                    validateCreatedTableDirect();
                    if (!useIfNotExists) {
                        execute("DROP TABLE tango");
                    }
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                    validateCreatedTableDirect();
                }
            }
        });
    }

    @Test
    public void testCreateTableLike() throws Exception {
        assertMemoryLeak(() -> {
            createSourceTable();
            StringBuilder ddl = generateCreateTableLike();
            System.out.printf("\naddColumn %s ColumnChaos %s\n\n", addColumn, columnChaos);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery query = compiler.compile(ddl, sqlExecutionContext);
                try (Operation op = query.getOperation()) {
                    assertNotNull(op);
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                    validateCreatedTableLike(false);
                    if (useIfNotExists) {
                        try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                            fut.await();
                        }
                        validateCreatedTableLike(false);
                    }
                    messWithSourceTable();
                    execute("DROP TABLE tango");
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                    validateCreatedTableLike(true);
                }
            }
        });
    }

    private static void createSourceTable() throws SqlException {
        engine.execute("CREATE TABLE samba (ts TIMESTAMP, sym SYMBOL, str STRING)", sqlExecutionContext);
    }

    private static String withErrPos(int pos, String message) {
        return String.format("[%d] %s", pos, message);
    }

    private void appendAsSelect(StringBuilder ddl) {
        ddl.append(" AS (");
        ddl.append("SELECT ");
        if (useSelectStar) {
            ddl.append('*');
        } else {
            ddl.append(tsCol = rndCase("ts")).append(", ");
            symPos = ddl.length();
            ddl.append(symCol = rndCase("sym")).append(", ");
            strPos = ddl.length();
            ddl.append(strCol = rndCase("str"));
        }
        ddl.append(" FROM ").append(rndCase("samba")).append(')');
        if (useCast) {
            ddl.append(", CAST(");
            castPos = ddl.length();
            ddl.append(wrongName == CAST ? WRONG_NAME : (castCol = rndCase("str"))).append(" AS SYMBOL");
            if (useCastSymbolCapacity) {
                ddl.append(" CAPACITY ").append(SYMBOL_CAPACITY);
            }
            ddl.append(')');
        }
        if (useIndex) {
            ddl.append(", INDEX(");
            indexPos = ddl.length();
            ddl.append(wrongName == INDEX ? WRONG_NAME : (indexCol = rndCase("sym")));
            if (useIndexCapacity) {
                ddl.append(" CAPACITY ").append(INDEX_CAPACITY);
            }
            ddl.append(')');
        }
    }

    private StringBuilder generateCreateTable(boolean asSelect) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ");
        if (useIfNotExists) {
            ddl.append("IF NOT EXISTS ");
        }
        ddl.append("tango");
        if (asSelect) {
            appendAsSelect(ddl);
        } else {
            ddl.append(" (ts TIMESTAMP, str STRING)");
        }
        if (!usePartitionBy) {
            return ddl;
        }
        ddl.append(" TIMESTAMP(");
        tsPos = ddl.length();
        ddl.append(wrongName == TIMESTAMP ? WRONG_NAME : rndCase("ts"))
                .append(") PARTITION BY HOUR WAL");
        if (useDedup) {
            ddl.append(" DEDUP UPSERT KEYS(");
            dedupPos = ddl.length();
            ddl.append(wrongName == DEDUP ? WRONG_NAME : rndCase("ts")).append(')');
        }
        return ddl;
    }

    private StringBuilder generateCreateTableLike() {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ");
        if (useIfNotExists) {
            ddl.append("IF NOT EXISTS ");
        }
        return ddl.append("tango (LIKE ").append(rndCase("samba")).append(')');
    }

    private void messWithSourceTable() throws SqlException {
        switch (columnChaos) {
            case DROP_STR:
                engine.execute("ALTER TABLE samba DROP COLUMN str");
                break;
            case DROP_SYM:
                engine.execute("ALTER TABLE samba DROP COLUMN sym");
                break;
            case CHANGE_TYPE:
                engine.execute("ALTER TABLE samba ALTER COLUMN str TYPE DOUBLE");
                break;
            case RENAME_AND_ADD:
                engine.execute("ALTER TABLE samba RENAME COLUMN str TO str_old");
                engine.execute("ALTER TABLE samba ADD COLUMN str DOUBLE");
                break;
        }
        if (addColumn) {
            engine.execute("ALTER TABLE samba ADD COLUMN str_new STRING");
        }
    }

    private String rndCase(String word) {
        StringBuilder result = new StringBuilder(word.length());
        for (int i = 0; i < word.length(); i++) {
            char c = word.charAt(i);
            if (c <= 'z' && Character.isLetter(c) && rnd.nextBoolean()) {
                c = (char) (c ^ 32); // flips the case
            }
            result.append(c);
        }
        return result.toString();
    }

    private void validateCreateAsSelectException(SqlException e) {
        String message = e.getMessage();
        String expected;
        switch (columnChaos) {
            case CHANGE_TYPE:
            case RENAME_AND_ADD:
                if (useCast) {
                    expected = withErrPos(castPos,
                            String.format("unsupported cast [column=%s, from=DOUBLE, to=SYMBOL]", strCol));
                } else {
                    throw new AssertionError("Error was expected only with a CAST clause present", e);
                }
                assertEquals(expected, message);
                return;
            case DROP_STR:
                if (!useSelectStar) {
                    expected = withErrPos(strPos, "Invalid column: " + strCol);
                } else if (useCast) {
                    expected = withErrPos(castPos,
                            String.format("CAST column doesn't exist [column=%s]", castCol));
                } else {
                    throw new AssertionError("Error isn't one of the expected DROP_STR failure cases", e);
                }
                assertEquals(expected, message);
                return;
            case DROP_SYM:
                if (!useSelectStar) {
                    expected = withErrPos(symPos, "Invalid column: " + symCol);
                } else if (useIndex) {
                    expected = withErrPos(indexPos,
                            String.format("INDEX column doesn't exist [column=%s]", indexCol));
                } else {
                    throw new AssertionError("Error isn't one of the expected DROP_SYM failure cases", e);
                }
                assertEquals(expected, message);
                return;
        }
        assert false : "This code was supposed to be unreachable";
    }

    private void validateCreateAsSelectNoException() {
        switch (columnChaos) {
            case CHANGE_TYPE:
            case RENAME_AND_ADD:
                if (useCast) {
                    fail("CAST clause present and cast column changed, yet CREATE operation succeeded");
                }
                break;
            case DROP_STR:
                if (!useSelectStar || useCast) {
                    fail("str column dropped, yet CREATE operation succeeded");
                }
                break;
            case DROP_SYM:
                if (!useSelectStar || useIndex) {
                    fail("sym column dropped, yet CREATE operation succeeded");
                }
                break;
        }
    }

    private void validateCreatedTableAsSelect(boolean afterMessing) throws SqlException {
        if (afterMessing) {
            validateCreatedTableAsSelectAfterMessing();
        } else {
            validateCreatedTableAsSelectBeforeMessing();
        }
    }

    private void validateCreatedTableAsSelectAfterMessing() throws SqlException {
        StringBuilder b = new StringBuilder(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n");
        b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\t")
                .append(usePartitionBy).append('\t').append(useDedup).append('\n');
        if (columnChaos != DROP_SYM) {
            b.append(symCol).append("\tSYMBOL\t").append(useIndex).append('\t')
                    .append(useIndexCapacity ? INDEX_CAPACITY : useIndex ? defaultIndexCapacity : 0).append('\t')
                    .append(!useIndex).append("\t128\t0\tfalse\tfalse\n");
        }
        if (columnChaos == RENAME_AND_ADD && useSelectStar) {
            b.append("str_old\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n");
        }
        if (columnChaos != DROP_STR) {
            String strColType = useCast ? "SYMBOL"
                    : columnChaos == CHANGE_TYPE || columnChaos == RENAME_AND_ADD ? "DOUBLE"
                    : "STRING";
            b.append(strCol).append("\t").append(strColType).append("\tfalse\t0\t")
                    .append(useCast).append('\t')
                    .append(useCastSymbolCapacity ? SYMBOL_CAPACITY : useCast ? defaultSymbolCapacity : 0)
                    .append("\t0\tfalse\tfalse\n");
        }
        if (addColumn && useSelectStar) {
            b.append("str_new\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n");
        }
        assertSql(b, "show columns from tango");
    }

    private void validateCreatedTableAsSelectBeforeMessing() throws SqlException {
        StringBuilder b = new StringBuilder(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n");
        b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\t")
                .append(usePartitionBy).append('\t').append(useDedup).append('\n');
        b.append(symCol).append("\tSYMBOL\t").append(useIndex).append('\t')
                .append(useIndexCapacity ? INDEX_CAPACITY : useIndex ? defaultIndexCapacity : 0).append('\t')
                .append(!useIndex).append("\t128\t0\tfalse\tfalse\n");
        b.append(strCol).append("\t").append(useCast ? "SYMBOL" : "STRING").append("\tfalse\t0\t")
                .append(useCast).append("\t")
                .append(useCastSymbolCapacity ? SYMBOL_CAPACITY : useCast ? defaultSymbolCapacity : 0)
                .append("\t0\tfalse\tfalse\n");
        assertSql(b, "show columns from tango");
    }

    private void validateCreatedTableDirect() throws SqlException {
        StringBuilder b = new StringBuilder(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n");
        b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\t")
                .append(usePartitionBy).append('\t').append(useDedup).append('\n');
        b.append(strCol).append("\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n");
        assertSql(b, "show columns from tango");
    }

    private void validateCreatedTableLike(boolean afterMessing) throws SqlException {
        boolean beforeMessing = !afterMessing;
        StringBuilder b = new StringBuilder(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n");
        b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n");
        if (beforeMessing || columnChaos != DROP_SYM) {
            b.append(symCol).append("\tSYMBOL\tfalse\t").append(defaultIndexCapacity).append("\ttrue\t")
                    .append(defaultSymbolCapacity).append("\t0\tfalse\tfalse\n");
        }
        if (beforeMessing) {
            b.append(strCol).append("\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n");
        } else {
            if (columnChaos == RENAME_AND_ADD) {
                b.append("str_old\tSTRING\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n");
            }
            if (columnChaos != DROP_STR) {
                boolean alteredStr = columnChaos == CHANGE_TYPE || columnChaos == RENAME_AND_ADD;
                b.append(strCol).append('\t').append(alteredStr ? "DOUBLE" : "STRING")
                        .append("\tfalse\t").append(alteredStr ? defaultIndexCapacity : 0)
                        .append("\tfalse\t0\t0\tfalse\tfalse\n");
            }
            if (addColumn) {
                b.append("str_new\tSTRING\tfalse\t").append(defaultIndexCapacity).append("\tfalse\t0\t0\tfalse\tfalse\n");
            }
        }
        assertSql(b, "show columns from tango");
    }

    enum ColumnChaos {
        DROP_STR, DROP_SYM, CHANGE_TYPE, RENAME_AND_ADD
    }

    enum WrongNameChoice {
        NONE, CAST, INDEX, TIMESTAMP, DEDUP
    }
}
