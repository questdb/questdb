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

package io.questdb.test.cairo;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlExecutionContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Twin of {@link AbstractRecordCursorFactoryTest} for the other factory base class: a
 * {@link ProjectableRecordCursorFactory} is closed by the same ownership chains, so its
 * {@code close()} owes the same idempotence. Before the guard existed, a second close was a no-op
 * only where the subclass's {@code _close()} happened to detach every field it freed first -- an
 * accident of each implementation, not a contract any of them declares.
 */
public class ProjectableRecordCursorFactoryTest {

    @Test
    public void testCloseIsIdempotent() {
        CloseCountingFactory factory = new CloseCountingFactory();
        factory.close();
        factory.close();
        Assert.assertEquals("_close() must run exactly once across repeated close() calls", 1, factory.closeCount);
    }

    @Test
    public void testCloseIsIdempotentWhenCloseThrows() {
        ThrowingCloseCountingFactory factory = new ThrowingCloseCountingFactory();
        Assert.assertThrows(RuntimeException.class, factory::close);
        factory.close();
        Assert.assertEquals("_close() must not run again after throwing", 1, factory.closeCount);
    }

    private static class CloseCountingFactory extends ProjectableRecordCursorFactory {
        int closeCount;

        CloseCountingFactory() {
            super(new GenericRecordMetadata());
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        protected void _close() {
            closeCount++;
        }
    }

    private static class ThrowingCloseCountingFactory extends CloseCountingFactory {
        @Override
        protected void _close() {
            super._close();
            throw new RuntimeException("expected");
        }
    }
}
