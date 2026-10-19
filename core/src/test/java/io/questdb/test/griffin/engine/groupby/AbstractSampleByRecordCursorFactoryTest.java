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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.groupby.AbstractNoRecordSampleByCursor;
import io.questdb.griffin.engine.groupby.AbstractSampleByRecordCursorFactory;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exercises the close path of {@link AbstractSampleByRecordCursorFactory}: the factory owns the
 * record functions, the base factory, and the four temporal parameter functions, and its _close()
 * must attempt every one of them even when an earlier close() throws.
 */
public class AbstractSampleByRecordCursorFactoryTest {

    @Test
    public void testCloseSurvivesThrowingRecordFunction() {
        final TrackingBaseFactory base = new TrackingBaseFactory();
        final ThrowingCloseFunction throwing = new ThrowingCloseFunction("close failure");
        final TrackingFunction recordFunction = new TrackingFunction();
        final ObjList<Function> recordFunctions = new ObjList<>();
        recordFunctions.add(throwing);
        recordFunctions.add(recordFunction);
        final TrackingFunction timezoneNameFunc = new TrackingFunction();
        final TrackingFunction offsetFunc = new TrackingFunction();
        final TrackingFunction sampleFromFunc = new TrackingFunction();
        final TrackingFunction sampleToFunc = new TrackingFunction();

        final AbstractSampleByRecordCursorFactory factory = new AbstractSampleByRecordCursorFactory(
                base,
                null,
                recordFunctions,
                timezoneNameFunc,
                offsetFunc,
                sampleFromFunc,
                sampleToFunc
        ) {
            @Override
            protected AbstractNoRecordSampleByCursor getRawCursor() {
                return null;
            }
        };

        final RuntimeException e = Assert.assertThrows(RuntimeException.class, factory::close);
        Assert.assertEquals("close failure", e.getMessage());
        Assert.assertEquals("later record functions must close despite the earlier failure", 1, recordFunction.closeCount);
        Assert.assertEquals("the base factory must close despite the earlier failure", 1, base.closeCount);
        Assert.assertEquals(1, timezoneNameFunc.closeCount);
        Assert.assertEquals(1, offsetFunc.closeCount);
        Assert.assertEquals(1, sampleFromFunc.closeCount);
        Assert.assertEquals(1, sampleToFunc.closeCount);
        Assert.assertEquals(1, throwing.closeAttempts);

        // AbstractRecordCursorFactory marks the factory closed before _close() runs, so a
        // second close from another owner is a no-op: nothing double-frees and nothing
        // re-throws, because the single _close() attempt already reached every resource
        factory.close();
        Assert.assertEquals(1, recordFunction.closeCount);
        Assert.assertEquals(1, base.closeCount);
        Assert.assertEquals(1, throwing.closeAttempts);
    }

    private static class ThrowingCloseFunction extends LongFunction {
        private final String message;
        private int closeAttempts;

        private ThrowingCloseFunction(String message) {
            this.message = message;
        }

        @Override
        public void close() {
            closeAttempts++;
            throw new RuntimeException(message);
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }
    }

    private static class TrackingBaseFactory extends AbstractRecordCursorFactory {
        private int closeCount;

        private TrackingBaseFactory() {
            super(null);
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

    private static class TrackingFunction extends LongFunction {
        private int closeCount;

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }
    }
}
