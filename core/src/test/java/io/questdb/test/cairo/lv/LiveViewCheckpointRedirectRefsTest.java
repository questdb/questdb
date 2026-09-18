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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointFunctionRootBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.std.MemoryTracker;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class LiveViewCheckpointRedirectRefsTest extends AbstractLiveViewTest {
    @Test
    public void testRedirectRefsKeepEveryCountWithinTheirRetentionLimit() throws Exception {
        assertMemoryLeak(() -> {
            final Method acquireShells = LiveViewCheckpointTimelineStoreWriter.class.getDeclaredMethod(
                    "acquirePublicationShells",
                    MemoryTracker.class
            );
            final Method releaseShells =
                    LiveViewCheckpointTimelineStoreWriter.class.getDeclaredMethod("releasePublicationShells");
            acquireShells.setAccessible(true);
            releaseShells.setAccessible(true);
            // The even counts from 2 to 512 add up to 65,792 reference slots, and three wider counts
            // bring that to exactly 262,144: what the redirect cache may keep once a publication ends.
            final int[] otherCounts = {65_535, 65_533, 65_284};
            long retainedRefLimit = 65_792;
            for (int count : otherCounts) {
                retainedRefLimit += count;
            }
            Assert.assertEquals(262_144, retainedRefLimit);

            try (LiveViewCheckpointTimelineStoreWriter writer = new LiveViewCheckpointTimelineStoreWriter(configuration)) {
                final Object[] evenPass = new Object[256];
                Object roots = rootBuilders(acquireShells.invoke(writer, new Object[]{null}));
                try {
                    fillRedirectRefs(roots, evenPass, otherCounts);
                    Assert.assertEquals(retainedRefLimit, writer.getRetainedRedirectStatePageRefCountForTest());
                } finally {
                    releaseShells.invoke(writer);
                }

                // A cache at its limit keeps every count past the end of its publication.
                roots = rootBuilders(acquireShells.invoke(writer, new Object[]{null}));
                try {
                    Assert.assertEquals(retainedRefLimit, writer.getRetainedRedirectStatePageRefCountForTest());
                    Assert.assertSame(evenPass[0], redirectRefs(roots, 2));
                    Assert.assertSame(evenPass[255], redirectRefs(roots, 512));
                    // One more reference slot takes the cache past its limit.
                    redirectRefs(roots, 1);
                    Assert.assertEquals(retainedRefLimit + 1, writer.getRetainedRedirectStatePageRefCountForTest());
                } finally {
                    releaseShells.invoke(writer);
                }

                // A cache past its limit drops every count when its publication ends.
                final Object[] refillPass = new Object[256];
                roots = rootBuilders(acquireShells.invoke(writer, new Object[]{null}));
                try {
                    Assert.assertEquals(0, writer.getRetainedRedirectStatePageRefCountForTest());
                    fillRedirectRefs(roots, refillPass, otherCounts);
                    Assert.assertNotSame("a count the cache dropped must get a fresh array", evenPass[0], refillPass[0]);
                    Assert.assertEquals(retainedRefLimit, writer.getRetainedRedirectStatePageRefCountForTest());
                } finally {
                    releaseShells.invoke(writer);
                }

                // The refill counts from zero, so the cache sits at its limit again and keeps every count.
                roots = rootBuilders(acquireShells.invoke(writer, new Object[]{null}));
                try {
                    Assert.assertEquals(retainedRefLimit, writer.getRetainedRedirectStatePageRefCountForTest());
                    Assert.assertSame(refillPass[0], redirectRefs(roots, 2));
                    Assert.assertSame(refillPass[255], redirectRefs(roots, 512));
                } finally {
                    releaseShells.invoke(writer);
                }
            }
        });
    }

    @Test
    public void testRedirectRefsUsesOneExactWidthLookupPerPartition() throws Exception {
        assertMemoryLeak(() -> {
            final Method acquireShells = LiveViewCheckpointTimelineStoreWriter.class.getDeclaredMethod(
                    "acquirePublicationShells",
                    MemoryTracker.class
            );
            final Method releaseShells =
                    LiveViewCheckpointTimelineStoreWriter.class.getDeclaredMethod("releasePublicationShells");
            acquireShells.setAccessible(true);
            releaseShells.setAccessible(true);

            try (
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration);
                    Path dir = new Path()
            ) {
                final Object shells = acquireShells.invoke(writer, new Object[]{null});
                try {
                    final Field rootsField = shells.getClass().getDeclaredField("roots");
                    rootsField.setAccessible(true);
                    final Object roots = rootsField.get(shells);
                    final Field builderField = roots.getClass().getDeclaredField("functionRootBuilder");
                    final Method redirectRefs = roots.getClass().getDeclaredMethod("redirectRefs", int.class);
                    builderField.setAccessible(true);
                    redirectRefs.setAccessible(true);

                    final LiveViewCheckpointFunctionRootBuilder builder =
                            (LiveViewCheckpointFunctionRootBuilder) builderField.get(roots);
                    builder.of(
                            dir.of(configuration.getDbRoot()),
                            new LiveViewCheckpointPageRef(),
                            new byte[]{1},
                            1,
                            new byte[0]
                    );

                    final int widthCount = 256;
                    final LiveViewCheckpointStatePageRef[][] firstPass =
                            new LiveViewCheckpointStatePageRef[widthCount][];
                    for (int partition = 0; partition < widthCount; partition++) {
                        final int count = 2 * (partition + 1);
                        final LiveViewCheckpointStatePageRef[] refs =
                                (LiveViewCheckpointStatePageRef[]) redirectRefs.invoke(roots, count);
                        firstPass[partition] = refs;
                        for (int i = 0; i < count; i++) {
                            refs[i].of(partition + 1L, i * 64L, 32, 64, 1, 0, 1, 0);
                        }
                        builder.putPartition(
                                new byte[]{(byte) partition, (byte) (partition >>> 8)},
                                new byte[]{1, 2, 3, 4},
                                refs
                        );
                    }
                    Assert.assertEquals(
                            "each first use must perform one direct exact-width lookup",
                            widthCount,
                            writer.getRedirectRefWidthLookupCountForTest()
                    );

                    for (int partition = widthCount - 1; partition >= 0; partition--) {
                        final LiveViewCheckpointStatePageRef[] refs =
                                (LiveViewCheckpointStatePageRef[]) redirectRefs.invoke(
                                        roots,
                                        2 * (partition + 1)
                                );
                        Assert.assertSame(firstPass[partition], refs);
                    }
                    Assert.assertEquals(
                            "warmed widths must still perform one direct lookup",
                            2 * widthCount,
                            writer.getRedirectRefWidthLookupCountForTest()
                    );

                    final LiveViewCheckpointStatePageRef[] empty =
                            (LiveViewCheckpointStatePageRef[]) redirectRefs.invoke(roots, 0);
                    Assert.assertSame(empty, redirectRefs.invoke(roots, 0));
                    Assert.assertEquals(0, empty.length);
                    Assert.assertEquals(
                            "zero-width reuse must use the same direct index",
                            2 * widthCount + 2,
                            writer.getRedirectRefWidthLookupCountForTest()
                    );
                    // The even counts from 2 to 512 add up to 65,792 reference slots.
                    Assert.assertEquals(65_792, writer.getRetainedRedirectStatePageRefCountForTest());
                } finally {
                    releaseShells.invoke(writer);
                }
            }
        });
    }

    // Requests every even reference count from 2 to 512, keeping each array in pass, then otherCounts.
    private static void fillRedirectRefs(Object roots, Object[] pass, int[] otherCounts) throws Exception {
        for (int i = 0; i < pass.length; i++) {
            pass[i] = redirectRefs(roots, 2 * (i + 1));
        }
        for (int count : otherCounts) {
            Assert.assertEquals(count, redirectRefs(roots, count).length);
        }
    }

    private static LiveViewCheckpointStatePageRef[] redirectRefs(Object roots, int count) throws Exception {
        final Method redirectRefs = roots.getClass().getDeclaredMethod("redirectRefs", int.class);
        redirectRefs.setAccessible(true);
        return (LiveViewCheckpointStatePageRef[]) redirectRefs.invoke(roots, count);
    }

    private static Object rootBuilders(Object shells) throws Exception {
        final Field rootsField = shells.getClass().getDeclaredField("roots");
        rootsField.setAccessible(true);
        return rootsField.get(shells);
    }
}
