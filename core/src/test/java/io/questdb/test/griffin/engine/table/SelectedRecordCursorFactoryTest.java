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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.groupby.DistinctRecordCursorFactory;
import io.questdb.griffin.engine.groupby.DistinctTimeSeriesRecordCursorFactory;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.RetimestampedRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.std.Misc;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

public class SelectedRecordCursorFactoryTest {

    @Test
    public void testDistinctConstructorsCloseTransferredResourcesOnFailure() {
        final RecordMetadata metadata = (RecordMetadata) Proxy.newProxyInstance(
                RecordMetadata.class.getClassLoader(),
                new Class[]{RecordMetadata.class},
                (proxy, method, args) -> null
        );

        final AtomicInteger timeSeriesBaseCloseCount = new AtomicInteger();
        final AtomicInteger timeSeriesMetadataCallCount = new AtomicInteger();
        final RecordCursorFactory timeSeriesBase = newFailingMetadataFactory(
                metadata,
                timeSeriesMetadataCallCount,
                timeSeriesBaseCloseCount
        );
        try {
            new DistinctTimeSeriesRecordCursorFactory(null, timeSeriesBase, null, null);
            Assert.fail();
        } catch (OutOfMemoryError expected) {
            Assert.assertEquals("test", expected.getMessage());
        }
        Assert.assertEquals(0, timeSeriesBaseCloseCount.get());
        Misc.free(timeSeriesBase);
        Assert.assertEquals(1, timeSeriesBaseCloseCount.get());

        final AtomicInteger baseCloseCount = new AtomicInteger();
        final AtomicInteger metadataCallCount = new AtomicInteger();
        final AtomicInteger limitHiCloseCount = new AtomicInteger();
        final AtomicInteger limitLoCloseCount = new AtomicInteger();
        final RecordCursorFactory base = newFailingMetadataFactory(metadata, metadataCallCount, baseCloseCount);
        final Function limitHi = newCloseCountingFunction(limitHiCloseCount);
        final Function limitLo = newCloseCountingFunction(limitLoCloseCount);
        try {
            new DistinctRecordCursorFactory(null, base, null, null, limitLo, limitHi);
            Assert.fail();
        } catch (OutOfMemoryError expected) {
            Assert.assertEquals("test", expected.getMessage());
        }
        Assert.assertEquals(0, baseCloseCount.get());
        Assert.assertEquals(0, limitHiCloseCount.get());
        Assert.assertEquals(0, limitLoCloseCount.get());
        Misc.free(base);
        Misc.free(limitHi);
        Misc.free(limitLo);
        Assert.assertEquals(1, baseCloseCount.get());
        Assert.assertEquals(1, limitHiCloseCount.get());
        Assert.assertEquals(1, limitLoCloseCount.get());
    }

    @Test
    public void testBaseFactoryClosedWhenRetimestampedWrapperConstructionFails() {
        final AtomicInteger closeCount = new AtomicInteger();
        final RecordCursorFactory baseFactory = (RecordCursorFactory) Proxy.newProxyInstance(
                RecordCursorFactory.class.getClassLoader(),
                new Class[]{RecordCursorFactory.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        closeCount.incrementAndGet();
                    }
                    return null;
                }
        );

        try {
            RetimestampedRecordCursorFactory.create(
                    baseFactory,
                    0,
                    (factory, timestampIndex) -> {
                        throw new OutOfMemoryError("test");
                    }
            );
            Assert.fail();
        } catch (OutOfMemoryError expected) {
            Assert.assertEquals("test", expected.getMessage());
        }
        Assert.assertEquals(1, closeCount.get());
    }

    @Test
    public void testConcurrentTimeFrameCursorClosedWhenWrapperConstructionFails() {
        final AtomicInteger closeCount = new AtomicInteger();
        final ConcurrentTimeFrameCursor baseCursor = (ConcurrentTimeFrameCursor) Proxy.newProxyInstance(
                ConcurrentTimeFrameCursor.class.getClassLoader(),
                new Class[]{ConcurrentTimeFrameCursor.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        closeCount.incrementAndGet();
                    }
                    return null;
                }
        );

        try {
            SelectedRecordCursorFactory.newSelectedConcurrentTimeFrameCursor(
                    baseCursor,
                    0,
                    (cursor, timestampIndex) -> {
                        throw new OutOfMemoryError("test");
                    }
            );
            Assert.fail();
        } catch (OutOfMemoryError expected) {
            Assert.assertEquals("test", expected.getMessage());
        }
        Assert.assertEquals(1, closeCount.get());
    }

    @Test
    public void testRetimestampedSharedCursorDelegates() throws Exception {
        final RecordMetadata metadata = (RecordMetadata) Proxy.newProxyInstance(
                RecordMetadata.class.getClassLoader(),
                new Class[]{RecordMetadata.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getTimestampIndex" -> 0;
                    default -> null;
                }
        );
        final RecordCursor sharedCursor = (RecordCursor) Proxy.newProxyInstance(
                RecordCursor.class.getClassLoader(),
                new Class[]{RecordCursor.class},
                (proxy, method, args) -> null
        );
        final RecordCursorFactory baseFactory = (RecordCursorFactory) Proxy.newProxyInstance(
                RecordCursorFactory.class.getClassLoader(),
                new Class[]{RecordCursorFactory.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getMetadata" -> metadata;
                    case "getSharedCursor" -> sharedCursor;
                    case "supportsSharedCursors" -> true;
                    default -> null;
                }
        );

        try (RetimestampedRecordCursorFactory factory = RetimestampedRecordCursorFactory.create(baseFactory, 0)) {
            Assert.assertTrue(factory.supportsSharedCursors());
            Assert.assertSame(sharedCursor, factory.getSharedCursor(null, 7));
        }
    }

    private static Function newCloseCountingFunction(AtomicInteger closeCount) {
        return (Function) Proxy.newProxyInstance(
                Function.class.getClassLoader(),
                new Class[]{Function.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        closeCount.incrementAndGet();
                    }
                    return null;
                }
        );
    }

    private static RecordCursorFactory newFailingMetadataFactory(
            RecordMetadata metadata,
            AtomicInteger metadataCallCount,
            AtomicInteger closeCount
    ) {
        return (RecordCursorFactory) Proxy.newProxyInstance(
                RecordCursorFactory.class.getClassLoader(),
                new Class[]{RecordCursorFactory.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "close":
                            closeCount.incrementAndGet();
                            return null;
                        case "getMetadata":
                            if (metadataCallCount.incrementAndGet() == 1) {
                                return metadata;
                            }
                            throw new OutOfMemoryError("test");
                        case "recordCursorSupportsRandomAccess":
                            return true;
                        default:
                            return null;
                    }
                }
        );
    }
}
