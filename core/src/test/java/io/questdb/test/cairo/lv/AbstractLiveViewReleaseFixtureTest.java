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

import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.std.str.Path;
import org.junit.Assert;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Shared harness for the cases that read a checkpoint tree written by a released build
 * back through this one: unpacking a whole database root over the test's own, and reading
 * the published checkpoint state of a view inside it.
 * <p>
 * Every helper here reads the released tree rather than asserting anything about it. What
 * each fixture is evidence for belongs to the subclass that owns it.
 */
public abstract class AbstractLiveViewReleaseFixtureTest extends AbstractLiveViewTest {

    /**
     * Unpacks a database root over the test's own and points the engine at it, the way
     * {@code EngineMigrationTest} does for its migration fixtures.
     * <p>
     * The catalogue has to be rebuilt as well as the name registry. A process that opens
     * this root for real hydrates {@link io.questdb.cairo.MetadataCache} from it at
     * startup, but the engine here has already declared its own (empty) catalogue
     * complete, and {@code hydrateAllTables()} short-circuits on that flag. Skipping the
     * wipe leaves {@code LiveViewRefreshJob.buildColumnMappings} reading a catalogue with
     * no base table in it, which throws {@code table does not exist} on a table every SQL
     * cursor can read - an artifact of hot-swapping the root under a live engine, not of
     * the fixture.
     */
    protected static void replaceDbContent(String resourcePath) throws IOException {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
        engine.closeNameRegistry();

        final byte[] buffer = new byte[1024 * 1024];
        try (InputStream is = AbstractLiveViewReleaseFixtureTest.class.getResourceAsStream(resourcePath)) {
            Assert.assertNotNull("missing fixture resource " + resourcePath, is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry entry;
                while ((entry = zip.getNextEntry()) != null) {
                    if (!entry.isDirectory()) {
                        final File dest = new File(root, entry.getName());
                        final File parent = dest.getParentFile();
                        Assert.assertTrue("cannot create " + parent, parent.isDirectory() || parent.mkdirs());
                        try (OutputStream os = new FileOutputStream(dest)) {
                            int read;
                            while ((read = zip.read(buffer)) > 0) {
                                os.write(buffer, 0, read);
                            }
                        }
                    }
                    zip.closeEntry();
                }
            }
        }

        engine.reloadTableNames();
        try (MetadataCacheWriter cacheRW = engine.getMetadataCache().writeLock()) {
            cacheRW.clearCache();
        }
        engine.getMetadataCache().hydrateAllTables();
    }

    protected Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(engine.getConfiguration().getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    /**
     * How many logical boundaries the named view's selected generation holds. A restart
     * that fell back to a from-base rebuild retires the timeline first, so this drops
     * rather than grows.
     */
    protected int countSealedBoundaries(String viewName) {
        final LiveViewInstance instance = instance(viewName);
        final int[] count = {0};
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointGenerationPin pin = store.pin()
        ) {
            timeline.iterateAll(pin.getTimelineRootRef(), entry -> count[0]++);
        }
        return count[0];
    }

    protected LiveViewInstance instance(String viewName) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(viewName);
        Assert.assertNotNull("live view '" + viewName + "' must be registered", instance);
        return instance;
    }

    /**
     * Whether the named view's newest sealed boundary carries a fused window root rather
     * than the separate roots the released build wrote.
     */
    protected boolean isFusedHead(String viewName) {
        final LiveViewInstance instance = instance(viewName);
        try (
                Path checkpointsDir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin();
                LiveViewCheckpointTimelineReader timeline = openTimelineReader(instance);
                LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(engine.getConfiguration());
                LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(engine.getConfiguration())
        ) {
            final LiveViewCheckpointTimelineEntry newest = new LiveViewCheckpointTimelineEntry();
            Assert.assertTrue(
                    viewName + " must have a sealed boundary",
                    timeline.last(pin.getTimelineRootRef(), newest)
            );
            root.of(checkpointsDir, newest.rootRef);
            final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
            root.getStateRootRef(stateRootRef);
            return !stateRootRef.isNull() && windowRoot.ofIfWindowRoot(checkpointsDir, stateRootRef);
        }
    }

    protected LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(engine.getConfiguration());
        try (Path dir = checkpointsDir(instance)) {
            store.of(dir);
        }
        return store;
    }

    protected LiveViewCheckpointTimelineReader openTimelineReader(LiveViewInstance instance) {
        final LiveViewCheckpointTimelineReader reader =
                new LiveViewCheckpointTimelineReader(engine.getConfiguration());
        try (Path dir = checkpointsDir(instance)) {
            reader.of(dir);
        }
        return reader;
    }
}
