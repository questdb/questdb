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

import static io.questdb.test.griffin.CreateTableFuzzTest.WrongNameChoice.*;
import static io.questdb.test.tools.TestUtils.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CreateTableFuzzTest extends AbstractCairoTest {
    private static final int INDEX_CAPACITY = 512;
    private static final int SYMBOL_CAPACITY = 64;
    private static final String WRONG_NAME = "bork";

    private final Rnd rnd = TestUtils.generateRandom(LOG);
    private final boolean addColumn = rnd.nextBoolean();
    private final ColumnChaos columnChaos = ColumnChaos.values()[rnd.nextInt(ColumnChaos.values().length)];
    private final boolean useCast = rnd.nextBoolean();
    private final boolean useCastSymbolCapacity = useCast && rnd.nextBoolean();
    private final boolean useIfNotExists = rnd.nextBoolean();
    private final boolean useIndex = rnd.nextBoolean();
    private final boolean useIndexCapacity = useIndex && rnd.nextBoolean();
    private final boolean usePartitionBy = rnd.nextBoolean();
    private final boolean useDedup = usePartitionBy && rnd.nextBoolean();
    private final WrongNameChoice wrongName = WrongNameChoice.values()[rnd.nextInt(WrongNameChoice.values().length)];
    private int castPos;
    private int dedupPos;
    private int defaultIndexCapacity;
    private int defaultSymbolCapacity;
    private int indexPos;
    private String strCol = "str";
    private String symCol = "sym";
    private String tsCol = "ts";
    private int tsPos;

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
                    "\n%s\nWrongName %s Cast %s CastSymbolCapacity %s Dedup %s Index %s IndexCapacity %s PartitionBy %s\n",
                    ddl, wrongName, useCast, useCastSymbolCapacity, useDedup, useIndex, useIndexCapacity, usePartitionBy);
            System.out.printf("addColumn %s ColumnChaos %s\n\n", addColumn, columnChaos);
            createSourceTable();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery query;
                try {
                    query = compiler.compile(ddl, sqlExecutionContext);
                } catch (SqlException e) {
                    String message = e.getMessage();
                    if (wrongName != NONE) {
                        int errPos = wrongName == CAST ? castPos
                                : wrongName == INDEX ? indexPos
                                : wrongName == TIMESTAMP ? tsPos
                                : wrongName == DEDUP ? dedupPos
                                : -1;
                        assertEquals(withErrPos(errPos, "Invalid column: bork"), message);
                        return;
                    }
                    throw e;
                }
                try (Operation op = query.getOperation()) {
                    assertNotNull(op);
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    } catch (SqlException e) {
                        String message = e.getMessage();
                        if (wrongName == TIMESTAMP) {
                            assertEquals(withErrPos(tsPos, "designated timestamp column doesn't exist [name=bork]"), message);
                        } else if (wrongName == DEDUP) {
                            assertEquals(withErrPos(dedupPos, "deduplicate key column not found [column=bork]"), message);
                        } else {
                            throw e;
                        }
                        return;
                    }
                    validateCreatedTableAsSelect();
                    if (!useIfNotExists) {
                        drop("DROP TABLE tango");
                    }
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                    validateCreatedTableAsSelect();
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
                        throw e;
                    }
                    return;
                }
                try (Operation op = query.getOperation()) {
                    assertNotNull(op);
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                    validateCreatedTableDirect();
                    if (!useIfNotExists) {
                        drop("DROP TABLE tango");
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
                    drop("DROP TABLE tango");
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                    validateCreatedTableLike(true);
                }
            }
        });
    }

    private static void createSourceTable() throws SqlException {
        engine.ddl("CREATE TABLE samba (ts TIMESTAMP, sym SYMBOL, str STRING)", sqlExecutionContext);
    }

    private static String withErrPos(int pos, String message) {
        return String.format("[%d] %s", pos, message);
    }

    private void appendAsSelect(StringBuilder ddl) {
        ddl.append(" AS (SELECT ")
                .append(tsCol = rndCase("ts")).append(", ")
                .append(symCol = rndCase("sym")).append(", ")
                .append(strCol = rndCase("str"))
                .append(" FROM ").append(rndCase("samba")).append(')');
        if (useCast) {
            ddl.append(", CAST(");
            castPos = ddl.length();
            ddl.append(wrongName == CAST ? WRONG_NAME : rndCase("str")).append(" AS SYMBOL");
            if (useCastSymbolCapacity) {
                ddl.append(" CAPACITY ").append(SYMBOL_CAPACITY);
            }
            ddl.append(')');
        }
        if (useIndex) {
            ddl.append(", INDEX(");
            indexPos = ddl.length();
            ddl.append(wrongName == INDEX ? WRONG_NAME : rndCase("sym"));
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
            case DROP:
                engine.ddl("ALTER TABLE samba DROP COLUMN str");
                break;
            case CHANGE_TYPE:
                engine.ddl("ALTER TABLE samba ALTER COLUMN str TYPE DOUBLE");
                break;
            case RENAME_AND_ADD:
                engine.ddl("ALTER TABLE samba RENAME COLUMN str TO str_old");
                engine.ddl("ALTER TABLE samba ADD COLUMN str DOUBLE");
                break;
        }
        if (addColumn) {
            engine.ddl("ALTER TABLE samba ADD COLUMN str_new STRING");
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

    private void validateCreatedTableAsSelect() throws SqlException {
        StringBuilder b = new StringBuilder(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n");
        b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t")
                .append(usePartitionBy).append('\t').append(useDedup).append('\n');
        b.append(symCol).append("\tSYMBOL\t").append(useIndex).append('\t')
                .append(useIndexCapacity ? INDEX_CAPACITY : useIndex ? defaultIndexCapacity : 0).append('\t')
                .append(!useIndex).append("\t128\tfalse\tfalse\n");
        b.append(strCol).append("\t").append(useCast ? "SYMBOL" : "STRING").append("\tfalse\t0\t")
                .append(useCast).append("\t")
                .append(useCastSymbolCapacity ? SYMBOL_CAPACITY : useCast ? defaultSymbolCapacity : 0)
                .append("\tfalse\tfalse\n");
        assertSql(b, "show columns from tango");
    }

    private void validateCreatedTableDirect() throws SqlException {
        StringBuilder b = new StringBuilder(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n");
        b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t")
                .append(usePartitionBy).append('\t').append(useDedup).append('\n');
        b.append(strCol).append("\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
        assertSql(b, "show columns from tango");
    }

    private void validateCreatedTableLike(boolean afterMessing) throws SqlException {
        StringBuilder b = new StringBuilder(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n");
        b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
        b.append(symCol).append("\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n");
        if (afterMessing) {
            switch (columnChaos) {
                case CHANGE_TYPE:
                    b.append(strCol).append("\tDOUBLE\tfalse\t256\tfalse\t0\tfalse\tfalse\n");
                    break;
                case RENAME_AND_ADD:
                    b.append("str_old\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
                    b.append("str\tDOUBLE\tfalse\t256\tfalse\t0\tfalse\tfalse\n");
                    break;
                case DROP:
            }
            if (addColumn) {
                b.append("str_new\tSTRING\tfalse\t256\tfalse\t0\tfalse\tfalse\n");
            }
        } else {
            b.append(strCol).append("\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
        }
        assertSql(b, "show columns from tango");
    }

    enum ColumnChaos {
        DROP, CHANGE_TYPE, RENAME_AND_ADD
    }

    enum WrongNameChoice {
        NONE, CAST, INDEX, TIMESTAMP, DEDUP
    }
}
