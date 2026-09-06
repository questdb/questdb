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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

/**
 * Guards hazard class (c) of the composite-partitioning merge checklist.
 * <p>
 * This branch added {@code supportsConcurrentTimeFrameCursor()} to {@link RecordCursorFactory}, and
 * it DEFAULTS to {@code supportsTimeFrameCursor()}. Any wrapper factory that delegates
 * {@code supportsTimeFrameCursor()} to a base but inherits that default will advertise a concurrent
 * (per-worker) time-frame cursor its base cannot produce. For a composite base that is a lie:
 * {@code CompositePageFrameRecordCursorFactory} returns {@code supportsTimeFrameCursor() == forward}
 * but {@code supportsConcurrentTimeFrameCursor() == false} and {@code newTimeFrameCursor() == null},
 * so the async WINDOW / HORIZON atom would NPE on the null per-worker cursor.
 * <p>
 * Upstream cannot know this flag exists, so every master merge can reintroduce the omission. It
 * already happened once: {@code LiveViewRecordCursorFactory} arrived from master delegating
 * {@code supportsTimeFrameCursor()} and {@code newTimeFrameCursor()} but not the concurrent flag.
 * <p>
 * NOT checked here (deliberately): {@code supportsPageFrameCursorForUnorderedAggregation()} defaults
 * to {@code supportsPageFrameCursor()}, which is {@code false} for a composite base. A wrapper that
 * fails to delegate that one only loses vectorised aggregation -- it cannot lie about safety.
 */
public class CompositeCapabilityDelegationTest {

    @Test
    public void testLiveViewDelegatesConcurrentTimeFrameFlag() {
        assertDelegatesConcurrentFlag(LiveViewRecordCursorFactory.class);
    }

    /**
     * Asserts that a factory which overrides {@code supportsTimeFrameCursor()} also overrides
     * {@code supportsConcurrentTimeFrameCursor()}, rather than silently inheriting the default.
     */
    private static void assertDelegatesConcurrentFlag(Class<? extends RecordCursorFactory> cls) {
        Assert.assertNotNull(
                "precondition: " + cls.getSimpleName() + " must override supportsTimeFrameCursor()"
                        + " for this guard to be meaningful",
                findDeclared(cls, "supportsTimeFrameCursor")
        );
        Assert.assertNotNull(
                cls.getSimpleName() + " overrides supportsTimeFrameCursor() but NOT"
                        + " supportsConcurrentTimeFrameCursor(). The default makes the latter return the"
                        + " former, so over a composite base this factory would advertise a concurrent"
                        + " time-frame cursor it cannot produce (newTimeFrameCursor() would return the"
                        + " base's null). Add: @Override public boolean supportsConcurrentTimeFrameCursor()"
                        + " { return base.supportsConcurrentTimeFrameCursor(); }",
                findDeclared(cls, "supportsConcurrentTimeFrameCursor")
        );
    }

    private static Method findDeclared(Class<?> cls, String name) {
        try {
            return cls.getDeclaredMethod(name);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }
}
