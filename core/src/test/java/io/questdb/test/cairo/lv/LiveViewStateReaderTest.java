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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewStateReader;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for {@link LiveViewStateReader#getInvalidationReason()}.
 * <p>
 * The catalogue cursor ({@code LiveViewsFunctionFactory}) reads the invalidation
 * reason lock-free while the invalidation writer overwrites it under
 * {@code synchronized(instance)}. The read therefore must return an immutable
 * snapshot: if it handed back a live mutable buffer, a concurrent {@code clear()}
 * +{@code put()} could be observed torn (garbled text or an
 * ArrayIndexOutOfBoundsException that fails the whole {@code live_views()} query).
 * These tests lock the immutability contract without threads - a later write must
 * not mutate a value a reader already holds.
 */
public class LiveViewStateReaderTest {

    @Test
    public void testClearResetsInvalidationReason() {
        LiveViewStateReader reader = new LiveViewStateReader();
        reader.setInvalidationReason("boom");
        Assert.assertNotNull(reader.getInvalidationReason());
        reader.clear();
        Assert.assertNull(reader.getInvalidationReason());
    }

    @Test
    public void testInvalidationReasonAcceptsMutableSourceButSnapshotsImmutably() {
        // The writer hands a reusable StringSink (the real invalidation path may too):
        // the reader must copy it, so mutating the sink afterwards leaves the stored
        // reason untouched.
        LiveViewStateReader reader = new LiveViewStateReader();
        StringSink source = new StringSink();
        source.put("base table dropped");
        reader.setInvalidationReason(source);

        CharSequence stored = reader.getInvalidationReason();
        Assert.assertNotNull(stored);
        Assert.assertEquals("base table dropped", stored.toString());

        source.clear();
        source.put("something completely different");
        Assert.assertEquals("base table dropped", reader.getInvalidationReason().toString());
    }

    @Test
    public void testInvalidationReasonNullAndEmptyReadAsNull() {
        LiveViewStateReader reader = new LiveViewStateReader();
        Assert.assertNull("no reason by default", reader.getInvalidationReason());

        reader.setInvalidationReason("boom");
        Assert.assertNotNull(reader.getInvalidationReason());

        reader.setInvalidationReason(null);
        Assert.assertNull("null reason reads back as null", reader.getInvalidationReason());

        reader.setInvalidationReason("boom");
        reader.setInvalidationReason("");
        Assert.assertNull("empty reason reads back as null", reader.getInvalidationReason());
    }

    @Test
    public void testInvalidationReasonSnapshotIsImmutable() {
        LiveViewStateReader reader = new LiveViewStateReader();
        reader.setInvalidationReason("base table does not exist");

        // A reader captures the reason lock-free; it may still hold this reference when
        // the invalidation writer overwrites the field with a new reason.
        CharSequence first = reader.getInvalidationReason();
        Assert.assertNotNull(first);
        Assert.assertEquals("base table does not exist", first.toString());

        // A second invalidation must not mutate the value the earlier reader observed.
        // A mutable StringSink would (clear()+put() in place) - the crux of the fix.
        reader.setInvalidationReason("column x was dropped");
        Assert.assertEquals("base table does not exist", first.toString());
        Assert.assertEquals("column x was dropped", reader.getInvalidationReason().toString());
    }
}
