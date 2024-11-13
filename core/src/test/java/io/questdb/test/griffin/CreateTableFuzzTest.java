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

    @Test
    public void testX() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        SCSequence seq = new SCSequence();
        assertMemoryLeak(() -> {
            try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
                createSourceTable(sqlCompiler);
                StringBuilder ddl = generateCreateTable(rnd);
                System.out.println();
                System.out.println(ddl);
                System.out.println();
                CompiledQuery query = sqlCompiler.compile(ddl, sqlExecutionContext);
                try (OperationFuture fut = query.execute(seq)) {
                    fut.await();
                }
            }
        });
    }

    private void appendAsSelect(StringBuilder ddl, Rnd rnd) {
        ddl.append(" AS (SELECT ")
                .append(rndCase("ts", rnd)).append(", ")
                .append(rndCase("sym", rnd)).append(", ")
                .append(rndCase("x", rnd))
                .append(" FROM ").append(rndCase("samba", rnd)).append(')');
        if (rnd.nextBoolean()) {
            ddl.append(", INDEX(").append(rndCase("sym", rnd));
            if (rnd.nextBoolean()) {
                ddl.append(" CAPACITY 256");
            }
            ddl.append(')');
        }
        if (rnd.nextBoolean()) {
            ddl.append(", CAST(").append(rndCase("x", rnd)).append(" AS SYMBOL");
            if (rnd.nextBoolean()) {
                ddl.append(" CAPACITY 256");
            }
            ddl.append(')');
        }
    }

    private void appendDirectTableDef(StringBuilder ddl, Rnd rnd) {
        ddl.append(" (").append(rndCase("ts", rnd)).append(" TIMESTAMP, ")
                .append(rndCase("x", rnd)).append(" STRING)");
    }

    private void appendLikeTable(StringBuilder ddl, Rnd rnd) {
        ddl.append(" (LIKE ").append(rndCase("samba", rnd)).append(')');
    }

    private void appendTsAndPartition(StringBuilder ddl, Rnd rnd) {
        ddl.append(" TIMESTAMP(").append(rndCase("ts", rnd)).append(')');
        if (rnd.nextBoolean()) {
            return;
        }
        ddl.append(" PARTITION BY HOUR");
    }

    private void createSourceTable(SqlCompiler sqlCompiler) throws Exception {
        try (OperationFuture fut = sqlCompiler.compile(
                        "CREATE TABLE samba (ts TIMESTAMP, sym SYMBOL, x STRING)", sqlExecutionContext)
                .execute(null)
        ) {
            fut.await();
        }
    }

    private StringBuilder generateCreateTable(Rnd rnd) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ");
        if (rnd.nextBoolean()) {
            ddl.append("IF NOT EXISTS ");
        }
        ddl.append(rndCase("tango", rnd));
        int createVariant = rnd.nextInt(3);
        switch (createVariant) {
            case 0:
                appendDirectTableDef(ddl, rnd);
                break;
            case 1:
                appendAsSelect(ddl, rnd);
                break;
            case 2:
                appendLikeTable(ddl, rnd);
                break;
            default:
        }
        if (createVariant == 2 || rnd.nextBoolean()) {
            return ddl;
        }
        if (rnd.nextBoolean()) {
            appendTsAndPartition(ddl, rnd);
        }
        return ddl;
    }

    private String rndCase(String word, Rnd rnd) {
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
}
