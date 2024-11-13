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
import io.questdb.mp.SCSequence;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class CreateTableFuzzTest extends AbstractCairoTest {
    public static final int VARIANT_DIRECT = 0;
    public static final int VARIANT_LIKE = 1;
    public static final int VARIANT_SELECT = 2;

    private final Rnd rnd = TestUtils.generateRandom(LOG);
    private final boolean useCast = rnd.nextBoolean();
    private final boolean useCastSymbolCapacity = rnd.nextBoolean();
    private final boolean useDedup = rnd.nextBoolean();
    private final boolean useIfNotExists = rnd.nextBoolean();
    private final boolean useIndexCapacity = rnd.nextBoolean();
    private final boolean useIndexClause = rnd.nextBoolean();
    private final boolean usePartitionBy = rnd.nextBoolean();
    private final int variantSelector = rnd.nextInt(3);

    @Test
    public void testX() throws Exception {
        SCSequence seq = new SCSequence();
        assertMemoryLeak(() -> {
            try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
                createSourceTable(sqlCompiler);
                StringBuilder ddl = generateCreateTable();
                System.out.println();
                System.out.println(ddl);
                System.out.println();
                CompiledQuery query = sqlCompiler.compile(ddl, sqlExecutionContext);
                try (OperationFuture fut = query.execute(seq)) {
                    fut.await();
                }
                if (!useIfNotExists) {
                    drop("DROP TABLE tango");
                }
                if (variantSelector != VARIANT_SELECT) {
                    try (OperationFuture fut = query.execute(seq)) {
                        fut.await();
                    }
                }
            }
        });
    }

    private void appendAsSelect(StringBuilder ddl) {
        ddl.append(" AS (SELECT ")
                .append(rndCase("ts")).append(", ")
                .append(rndCase("sym")).append(", ")
                .append(rndCase("x"))
                .append(" FROM ").append(rndCase("samba")).append(')');
        if (useIndexClause) {
            ddl.append(", INDEX(").append(rndCase("sym"));
            if (useIndexCapacity) {
                ddl.append(" CAPACITY 256");
            }
            ddl.append(')');
        }
        if (useCast) {
            ddl.append(", CAST(").append(rndCase("x")).append(" AS SYMBOL");
            if (useCastSymbolCapacity) {
                ddl.append(" CAPACITY 256");
            }
            ddl.append(')');
        }
    }

    private void appendDedup(StringBuilder ddl) {
        ddl.append(" DEDUP UPSERT KEYS(").append(rndCase("ts")).append(')');
    }

    private void appendDirectTableDef(StringBuilder ddl) {
        ddl.append(" (").append(rndCase("ts")).append(" TIMESTAMP, ")
                .append(rndCase("x")).append(" STRING)");
    }

    private void appendLikeTable(StringBuilder ddl) {
        ddl.append(" (LIKE ").append(rndCase("samba")).append(')');
    }

    private void appendTsAndPartition(StringBuilder ddl) {
        ddl.append(" TIMESTAMP(").append(rndCase("ts")).append(')');
        if (usePartitionBy) {
            ddl.append(" PARTITION BY HOUR");
        }
        ddl.append(" WAL");
    }

    private void createSourceTable(SqlCompiler sqlCompiler) throws Exception {
        try (OperationFuture fut = sqlCompiler.compile(
                        "CREATE TABLE samba (ts TIMESTAMP, sym SYMBOL, x STRING)", sqlExecutionContext)
                .execute(null)
        ) {
            fut.await();
        }
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
                appendDirectTableDef(ddl);
                break;
            case VARIANT_LIKE:
                appendLikeTable(ddl);
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
}
