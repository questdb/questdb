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

import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.CompiledQueryImpl;
import io.questdb.griffin.EmptyRecordMetadata;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.*;

public class CompiledQueryTest extends AbstractCairoTest {

    @Test
    public void testExecutedAtParseTime() throws Exception {
        assertMemoryLeak(() -> {
            final CompiledQueryImpl cq = new CompiledQueryImpl(engine);

            cq.clear();
            cq.ofNone();
            assertEquals(CompiledQuery.NONE, cq.getType());
            assertFalse(cq.executedAtParseTime());
            assertFalse(cq.isCacheable());

            cq.clear();
            cq.ofSelect(new EmptyTableRecordCursorFactory(EmptyRecordMetadata.INSTANCE), false);
            assertEquals(CompiledQuery.SELECT, cq.getType());
            assertFalse(cq.executedAtParseTime());
            assertFalse(cq.isCacheable());

            cq.clear();
            cq.ofSelect(new EmptyTableRecordCursorFactory(EmptyRecordMetadata.INSTANCE), true);
            assertEquals(CompiledQuery.SELECT, cq.getType());
            assertFalse(cq.executedAtParseTime());
            assertTrue(cq.isCacheable());

            cq.clear();
            cq.ofCreateUser();
            assertEquals(CompiledQuery.CREATE_USER, cq.getType());
            assertTrue(cq.executedAtParseTime());
            assertFalse(cq.isCacheable());

            cq.clear();
            cq.ofAlterUser();
            assertEquals(CompiledQuery.ALTER_USER, cq.getType());
            assertTrue(cq.executedAtParseTime());
            assertFalse(cq.isCacheable());

            cq.clear();
            cq.ofAlterStoragePolicy();
            assertEquals(CompiledQuery.ALTER_STORAGE_POLICY, cq.getType());
            assertTrue(cq.executedAtParseTime());
            assertFalse(cq.isCacheable());

            cq.clear();
            cq.ofEmpty();
            assertEquals(CompiledQuery.EMPTY, cq.getType());
            assertFalse(cq.executedAtParseTime());
            assertFalse(cq.isCacheable());

            cq.clear();
            cq.ofExplain(new EmptyTableRecordCursorFactory(EmptyRecordMetadata.INSTANCE));
            assertEquals(CompiledQuery.EXPLAIN, cq.getType());
            assertFalse(cq.executedAtParseTime());
            assertFalse(cq.isCacheable());

            cq.clear();
            cq.ofCommit();
            assertEquals(CompiledQuery.COMMIT, cq.getType());
            assertFalse(cq.executedAtParseTime());
            assertFalse(cq.isCacheable());

            cq.clear();
            cq.ofRollback();
            assertEquals(CompiledQuery.ROLLBACK, cq.getType());
            assertFalse(cq.executedAtParseTime());
            assertFalse(cq.isCacheable());
        });
    }
}
