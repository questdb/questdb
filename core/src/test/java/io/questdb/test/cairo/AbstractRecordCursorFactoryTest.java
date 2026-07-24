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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlExecutionContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link AbstractRecordCursorFactory#close()} must be idempotent: factory ownership chains close
 * the same instance from more than one owner on error paths (a failing factory constructor closes
 * its adopted base factory, and the generator catch then frees its own reference to that base),
 * and {@code _close()} implementations free adopted functions and native resources that must not
 * be freed twice.
 */
public class AbstractRecordCursorFactoryTest {

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

    private static class CloseCountingFactory extends AbstractRecordCursorFactory {
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
