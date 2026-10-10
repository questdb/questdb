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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopFullJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.griffin.model.QueryModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

public class JoinOwnershipTest {
    @Test
    public void testFullJoinPreparationFailureClosesEveryOwnerOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertConstructorFailure(0, false);
            assertConstructorFailure(0, true);
        });
    }

    @Test
    public void testLightJoinPreparationFailureClosesEveryOwnerOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertConstructorFailure(1, false);
            assertConstructorFailure(1, true);
        });
    }

    @Test
    public void testNestedFullJoinPreparationFailureClosesEveryOwnerOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertConstructorFailure(2, false);
            assertConstructorFailure(2, true);
        });
    }

    private void assertConstructorFailure(int factoryKind, boolean isThrowingClose) {
        final RuntimeException failure = new RuntimeException("join map preparation");
        final RuntimeException closeFailure = new RuntimeException("ON function close");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(".") {
            @Override
            public int getSqlSmallMapKeyCapacity() {
                throw failure;
            }
        };
        final TrackingMetadata metadata = new TrackingMetadata();
        metadata.add(new TableColumnMetadata("a.id", ColumnType.INT));
        metadata.add(new TableColumnMetadata("b.id", ColumnType.INT));
        final GenericRecordMetadata childMetadata = new GenericRecordMetadata();
        childMetadata.add(new TableColumnMetadata("id", ColumnType.INT));
        final TrackingFactory master = new TrackingFactory(childMetadata);
        final TrackingFactory slave = new TrackingFactory(childMetadata);
        final TrackingFilter filter = new TrackingFilter(isThrowingClose ? closeFailure : null);
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.INT);
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        valueTypes.add(factoryKind == 1 ? ColumnType.INT : ColumnType.LONG);
        final RuntimeException actual = Assert.assertThrows(RuntimeException.class, () -> {
            // The configuration throws before either constructor uses its record sinks.
            if (factoryKind == 2) {
                new NestedLoopFullJoinRecordCursorFactory(configuration, metadata, master, slave, 1, filter,
                        NullRecordFactory.getInstance(childMetadata), NullRecordFactory.getInstance(childMetadata));
            } else if (factoryKind == 1) {
                new HashOuterJoinFilteredLightRecordCursorFactory(configuration, metadata, master, slave,
                        keyTypes, valueTypes, null, null, 1, filter, null, QueryModel.JOIN_LEFT_OUTER, null, null);
            } else {
                new HashOuterJoinFilteredRecordCursorFactory(configuration, metadata, master, slave,
                        keyTypes, valueTypes, null, null, null, 1, filter, null, QueryModel.JOIN_LEFT_OUTER, null, null);
            }
        });
        Assert.assertSame(failure, actual);
        Assert.assertEquals(1, metadata.closeCount);
        Assert.assertEquals(1, master.closeCount);
        Assert.assertEquals(1, slave.closeCount);
        Assert.assertEquals(1, filter.closeCount);
        Assert.assertEquals(isThrowingClose ? 1 : 0, failure.getSuppressed().length);
        if (isThrowingClose) {
            Assert.assertSame(closeFailure, failure.getSuppressed()[0]);
        }
    }

    private static final class TrackingFactory implements RecordCursorFactory {
        private final RecordMetadata metadata;
        private int closeCount;

        private TrackingFactory(RecordMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public void close() {
            Assert.assertEquals("input closed twice", 1, ++closeCount);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("input");
        }
    }

    private static final class TrackingFilter extends BooleanFunction {
        private final RuntimeException failure;
        private int closeCount;

        private TrackingFilter(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void close() {
            Assert.assertEquals("ON function closed twice", 1, ++closeCount);
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public boolean getBool(Record record) {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("ON predicate");
        }
    }

    private static final class TrackingMetadata extends GenericRecordMetadata implements Closeable {
        private int closeCount;

        @Override
        public void close() {
            Assert.assertEquals("metadata closed twice", 1, ++closeCount);
        }
    }
}
