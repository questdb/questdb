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
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class CreateTableFuzzTest extends AbstractCairoTest {
    public static final int VARIANT_DIRECT = 0;
    public static final int VARIANT_LIKE = 1;
    public static final int VARIANT_SELECT = 2;

    private final Rnd rnd = TestUtils.generateRandom(LOG);
    private final boolean useCast = rnd.nextBoolean();
    private final boolean useCastSymbolCapacity = useCast && rnd.nextBoolean();
    private final boolean useIfNotExists = rnd.nextBoolean();
    private final boolean useIndexClause = rnd.nextBoolean();
    private final boolean useIndexCapacity = useIndexClause && rnd.nextBoolean();
    private final boolean usePartitionBy = rnd.nextBoolean();
    private final boolean useDedup = usePartitionBy && rnd.nextBoolean();
    private final int variantSelector = rnd.nextInt(3);
    private String strCol = "str";
    private String symCol = "sym";
    private String tsCol = "ts";

    @Test
    @Ignore
    public void testX() throws Exception {
        assertMemoryLeak(() -> {
            if (variantSelector != VARIANT_DIRECT) {
                createSourceTable();
            }
            StringBuilder ddl = generateCreateTable();
            System.out.println();
            System.out.println(ddl);
            System.out.printf("Variant %d Cast %s CastSymbolCapacity %s Dedup %s IndexCapacity %s IndexClause %s PartitionBy %s\n",
                    variantSelector, useCast, useCastSymbolCapacity, useDedup, useIndexCapacity, useIndexClause, usePartitionBy);
            System.out.println();
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
                ddl.append(" CAPACITY 64");
            }
            ddl.append(')');
        }
        if (useIndexClause) {
            ddl.append(", INDEX(").append(rndCase("sym"));
            if (useIndexCapacity) {
                ddl.append(" CAPACITY 512");
            }
            ddl.append(')');
        }
    }

    private void appendDedup(StringBuilder ddl) {
        ddl.append(" DEDUP UPSERT KEYS(").append(rndCase("ts")).append(')');
    }

    private void appendTsAndPartition(StringBuilder ddl) {
        ddl.append(" TIMESTAMP(").append(rndCase("ts")).append(')');
        if (usePartitionBy) {
            ddl.append(" PARTITION BY HOUR");
        }
        ddl.append(" WAL");
    }

    private void createSourceTable() throws Exception {
        engine.ddl("CREATE TABLE samba (ts TIMESTAMP, sym SYMBOL, str STRING)", sqlExecutionContext);
    }

    private StringBuilder generateCreateTable() {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ");
        if (useIfNotExists) {
            ddl.append("IF NOT EXISTS ");
        }
        ddl.append(rndCase("tango"));
        switch (variantSelector) {
            case VARIANT_DIRECT:
                ddl.append(" (ts TIMESTAMP, str STRING)");
                break;
            case VARIANT_LIKE:
                ddl.append(" (LIKE ").append(rndCase("samba")).append(')');
                break;
            case VARIANT_SELECT:
                appendAsSelect(ddl);
                break;
            default:
        }
        if (variantSelector == VARIANT_LIKE || !usePartitionBy) {
            return ddl;
        }
        appendTsAndPartition(ddl);
        if (useDedup) {
            appendDedup(ddl);
        }
        return ddl;
    }

    private String rndCase(String word) {
        StringBuilder result = new StringBuilder(word.length());
        for (int i = VARIANT_DIRECT; i < word.length(); i++) {
            char c = word.charAt(i);
            if (c <= 'z' && Character.isLetter(c) && rnd.nextBoolean()) {
                c = (char) (c ^ 32); // flips the case
            }
            result.append(c);
        }
        return result.toString();
    }

    private void validateCreatedTable() throws SqlException {
        StringBuilder b = new StringBuilder("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n");
        if (variantSelector == VARIANT_SELECT) {
            b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t")
                    .append(usePartitionBy).append('\t').append(useDedup).append('\n');
            b.append(symCol).append("\tSYMBOL\t").append(useIndexClause).append('\t')
                    .append(useIndexCapacity ? 512 : useIndexClause ? 256 : 0).append('\t').append(!useIndexClause).append("\t128\tfalse\tfalse\n");
            b.append(strCol).append("\t").append(useCast ? "SYMBOL" : "STRING").append("\tfalse\t0\t").append(useCast).append("\t")
                    .append(useCastSymbolCapacity ? 64 : useCast ? 128 : 0).append("\tfalse\tfalse\n");
        } else if (variantSelector == VARIANT_LIKE) {
            b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
            b.append(symCol).append("\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n");
            b.append(strCol).append("\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
        } else {
            b.append(tsCol).append("\tTIMESTAMP\tfalse\t0\tfalse\t0\t")
                    .append(usePartitionBy).append('\t').append(useDedup).append('\n');
            b.append(strCol).append("\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n");
        }
        assertSql(b, "show columns from tango");
    }
}
