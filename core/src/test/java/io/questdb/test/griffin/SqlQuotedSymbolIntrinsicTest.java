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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SqlQuotedSymbolIntrinsicTest extends AbstractCairoTest {
    @Test
    public void testLiteralKeysDecodeQuotesOnce() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertIndexedAndScalar("s=''''", "id\n3\n7\n");
            assertIndexedAndScalar("s='''edge'''", "id\n4\n");
            assertIndexedAndScalar("s='O''Reilly'", "id\n5\n");
            assertIndexedAndScalar("s=''", "id\n2\n");
            assertIndexedAndScalar("s=null", "id\n1\n");
            assertQuery("SELECT id FROM quoted_keys WHERE s='''' LATEST ON ts PARTITION BY s")
                    .sizeMayVary().returns("id\n7\n");
        });
    }

    @Test
    public void testListsAndExclusionsCompareDecodedKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertIndexedAndScalar("s IN ('','''','''edge''','O''Reilly',null)", "id\n1\n2\n3\n4\n5\n7\n");
            assertIndexedAndScalar("s IN ('''','O''Reilly') AND s != ''''", "id\n5\n");
            assertIndexedAndScalar("s IN ('''','O''Reilly') AND s NOT IN ('O''Reilly')", "id\n3\n7\n");
            assertIndexedAndScalar("s NOT IN ('''','O''Reilly')", "id\n1\n2\n4\n6\n");
            assertIndexedAndScalar("s IN ('''','O''Reilly') AND s=concat('O', '''', 'Reilly')", "id\n5\n");
            assertIndexedAndScalar("s IN ('''edge''','O''Reilly') AND s != concat('O', '''', 'Reilly')", "id\n4\n");
        });
    }

    @Test
    public void testFunctionValuesAndBindValuesRemainDecoded() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT concat('''','edge','''') AS value").expectSize().returns("value\n'edge'\n");
            assertIndexedAndScalar("s=concat('''','edge','''')", "id\n4\n");
            assertIndexedAndScalar("s=left('O''ReillyZ',8)", "id\n5\n");
            bindVariableService.setStr(0, "'edge'");
            try (RecordCursorFactory factory = select("SELECT id FROM quoted_keys WHERE s=$1 ORDER BY id")) {
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns("id\n4\n");
                bindVariableService.setStr(0, "'");
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns("id\n3\n7\n");
                bindVariableService.setStr(0, null);
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns("id\n1\n");
                bindVariableService.setStr(0, "");
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns("id\n2\n");
            }
        });
    }

    @Test
    public void testRetainedKeysSurviveCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<RecordCursorFactory> retained = new ObjList<>();
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained.add(compiler.compile("SELECT id FROM quoted_keys WHERE s='''edge''' ORDER BY id", sqlExecutionContext)
                            .getRecordCursorFactory());
                    retained.add(compiler.compile("SELECT id FROM quoted_keys WHERE s=concat('''','edge','''') ORDER BY id", sqlExecutionContext)
                            .getRecordCursorFactory());
                    retained.add(compiler.compile("SELECT id FROM quoted_keys WHERE s IN ('''','O''Reilly') AND s!='O''Reilly' ORDER BY id", sqlExecutionContext)
                            .getRecordCursorFactory());
                    for (int i = 0; i < 32; i++) {
                        try (RecordCursorFactory ignored = compiler.compile(
                                "SELECT id FROM quoted_keys WHERE s IN ('other''" + i + "','replacement''value')", sqlExecutionContext
                        ).getRecordCursorFactory()) {
                            compiler.clear();
                        }
                    }
                }
                assertFactory(retained.getQuick(0)).withContext(sqlExecutionContext).inferRandomAccess()
                        .sizeMayVary().returns("id\n4\n");
                assertFactory(retained.getQuick(1)).withContext(sqlExecutionContext).inferRandomAccess()
                        .sizeMayVary().returns("id\n4\n");
                assertFactory(retained.getQuick(2)).withContext(sqlExecutionContext).inferRandomAccess()
                        .sizeMayVary().returns("id\n3\n7\n");
            } finally {
                Misc.freeObjList(retained);
            }
        });
    }

    private void assertIndexedAndScalar(String predicate, String expected) throws Exception {
        assertQuery("SELECT id FROM quoted_keys WHERE " + predicate + " ORDER BY id").sizeMayVary().returns(expected);
        assertQuery("SELECT /*+ no_index */ id FROM quoted_keys WHERE " + predicate + " ORDER BY id").sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE quoted_keys(id INT,s SYMBOL INDEX,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO quoted_keys VALUES
                (1,null,1),(2,'',2),(3,'''',3),(4,'''edge''',4),
                (5,'O''Reilly',5),(6,'A',6),(7,'''',7)
                """);
    }
}
