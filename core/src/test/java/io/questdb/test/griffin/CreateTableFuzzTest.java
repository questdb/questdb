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

import static io.questdb.test.griffin.CreateTableFuzzTest.CreateVariant.DIRECT;
import static io.questdb.test.griffin.CreateTableFuzzTest.CreateVariant.LIKE;
import static org.junit.Assert.assertNotNull;

public class CreateTableFuzzTest extends AbstractCairoTest {
    private static final int INDEX_CAPACITY = 512;
    private static final int SYMBOL_CAPACITY = 64;
    private final Rnd rnd = TestUtils.generateRandom(LOG);
    private final boolean useCast = rnd.nextBoolean();
    private final boolean useCastSymbolCapacity = useCast && rnd.nextBoolean();
    private final boolean useIfNotExists = rnd.nextBoolean();
    private final boolean useIndex = rnd.nextBoolean();
    private final boolean useIndexCapacity = useIndex && rnd.nextBoolean();
    private final boolean usePartitionBy = rnd.nextBoolean();
    private final boolean useDedup = usePartitionBy && rnd.nextBoolean();
    private final CreateVariant variant = CreateVariant.values()[rnd.nextInt(CreateVariant.values().length)];
    private int defaultIndexCapacity;
    private int defaultSymbolCapacity;
    private String strCol = "str";
    private String symCol = "sym";
    private String tsCol = "ts";

    @Before
    @Override
    public void setUp() {
        super.setUp();
        defaultSymbolCapacity = engine.getConfiguration().getDefaultSymbolCapacity();
        defaultIndexCapacity = engine.getConfiguration().getIndexValueBlockSize();
    }

    @Test
    public void testCreateTableFuzz() throws Exception {
        assertMemoryLeak(() -> {
            if (variant != DIRECT) {
                engine.ddl("CREATE TABLE samba (ts TIMESTAMP, sym SYMBOL, str STRING)", sqlExecutionContext);
            }
            StringBuilder ddl = generateCreateTable();
            System.out.printf(
                    "\n%s\nVariant %s Cast %s CastSymbolCapacity %s Dedup %s Index %s IndexCapacity %s PartitionBy %s\n\n",
                    ddl, variant, useCast, useCastSymbolCapacity, useDedup, useIndex, useIndexCapacity, usePartitionBy);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery query = compiler.compile(ddl, sqlExecutionContext);
                try (Operation op = query.getOperation()) {
                    assertNotNull(op);
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                    validateCreatedTable();
                    if (!useIfNotExists) {
                        drop("DROP TABLE tango");
                    }
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                    validateCreatedTable();
                }
            }
        });
    }

    private void appendAsSelect(StringBuilder ddl) {
        ddl.append(" AS (SELECT ")
                .append(tsCol = rndCase("ts")).append(", ")
                .append(symCol = rndCase("sym")).append(", ")
                .append(strCol = rndCase("str"))
                .append(" FROM ").append(rndCase("samba")).append(')');
        if (useCast) {
            ddl.append(", CAST(").append(rndCase("str")).append(" AS SYMBOL");
            if (useCastSymbolCapacity) {
                ddl.append(" CAPACITY ").append(SYMBOL_CAPACITY);
            }
            ddl.append(')');
        }
        if (useIndex) {
            ddl.append(", INDEX(").append(rndCase("sym"));
            if (useIndexCapacity) {
                ddl.append(" CAPACITY ").append(INDEX_CAPACITY);
            }
            ddl.append(')');
        }
    }

    private StringBuilder generateCreateTable() {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ");
        if (useIfNotExists) {
            ddl.append("IF NOT EXISTS ");
        }
        ddl.append("tango");
        switch (variant) {
            case DIRECT:
                ddl.append(" (ts TIMESTAMP, str STRING)");
                break;
            case LIKE:
                ddl.append(" (LIKE ").append(rndCase("samba")).append(')');
                break;
            case SELECT:
                appendAsSelect(ddl);
                break;
            default:
        }
        if (variant == LIKE || !usePartitionBy) {
            return ddl;
        }
        ddl.append(" TIMESTAMP(").append(rndCase("ts")).append(") PARTITION BY HOUR WAL");
        if (useDedup) {
            ddl.append(" DEDUP UPSERT KEYS(").append(rndCase("ts")).append(')');
        }
        return ddl;
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

    private void validateCreatedTable() throws SqlException {
        StringBuilder b = new StringBuilder(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n");
        switch (variant) {
            case DIRECT:
                b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t")
                        .append(usePartitionBy).append('\t').append(useDedup).append('\n');
                b.append(strCol).append("\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
                break;
            case LIKE:
                b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
                b.append(symCol).append("\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n");
                b.append(strCol).append("\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
                break;
            case SELECT:
                b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t")
                        .append(usePartitionBy).append('\t').append(useDedup).append('\n');
                b.append(symCol).append("\tSYMBOL\t").append(useIndex).append('\t')
                        .append(useIndexCapacity ? INDEX_CAPACITY : useIndex ? defaultIndexCapacity : 0).append('\t')
                        .append(!useIndex).append("\t128\tfalse\tfalse\n");
                b.append(strCol).append("\t").append(useCast ? "SYMBOL" : "STRING").append("\tfalse\t0\t")
                        .append(useCast).append("\t")
                        .append(useCastSymbolCapacity ? SYMBOL_CAPACITY : useCast ? defaultSymbolCapacity : 0)
                        .append("\tfalse\tfalse\n");
                break;
            default:
        }
        assertSql(b, "show columns from tango");
    }

    enum CreateVariant {
        DIRECT, LIKE, SELECT
    }
}
