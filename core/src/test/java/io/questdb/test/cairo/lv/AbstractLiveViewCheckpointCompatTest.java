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
import io.questdb.cairo.TableNameRegistryStore;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.mig.EngineMigrationTest;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.stream.Stream;

import static io.questdb.cairo.wal.WalUtils.TABLE_REGISTRY_NAME_FILE;

/**
 * Shared harness for the cases that read a checkpoint tree some <i>other</i> build wrote:
 * unpacking a whole database root over the test's own, and reading the published checkpoint
 * state of a view inside it.
 * <p>
 * Both directions live here. {@link LiveViewCheckpointReleaseCompatTest} and
 * {@link LiveViewCheckpointReleaseShapesCompatTest} read trees an older, released build wrote
 * and expect the format block; {@link LiveViewCheckpointForwardCompatTest} reads a tree shaped
 * the way a newer build would write one and expects either the same block or a refusal and a
 * rebuild, depending on which gate the shape trips. They need the same things - a database root
 * the engine will open, a way to see which shape the head carries, a way to read the format
 * version a directory declares, and a way to state that not one of its files moved.
 * <p>
 * Every helper here reads the tree rather than asserting anything about it. What each case is
 * evidence for belongs to the subclass that owns it.
 */
public abstract class AbstractLiveViewCheckpointCompatTest extends AbstractLiveViewTest {

    /**
     * Unpacks a database root over the test's own and points the engine at it. The unpack
     * and the name-registry reload are {@link EngineMigrationTest#replaceDbContent} - the
     * same routine the migration fixtures use - so a fix there reaches this caller too.
     * This harness also aligns the extracted name registry to the host page size and
     * restores the live-view catalogue.
     * <p>
     * The resource check goes first because the shared routine asserts non-null without a
     * message: a fixture that failed to build, or one a rename left behind, otherwise fails
     * as a bare {@code assertNotNull} naming neither the {@code /lv} zip nor these tests.
     * <p>
     * The registry has to drop its instances next, because they hold the roots the
     * unpack is about to overwrite.
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
        Assert.assertNotNull(
                "missing live-view checkpoint fixture resource " + resourcePath,
                AbstractLiveViewCheckpointCompatTest.class.getResource(resourcePath)
        );
        engine.getLiveViewRegistry().clear();

        EngineMigrationTest.replaceDbContent(resourcePath);

        // Linux fixtures can carry a 4 KiB name registry on a host with 16 KiB pages.
        // QueryAssertion cleanup recreates it at the host page size, which otherwise
        // looks like a leak to the query's memory check. Pad only the registry before
        // taking that baseline; preserve the checkpoint files exactly as released.
        engine.closeNameRegistry();
        try (Path registryPath = new Path().of(engine.getConfiguration().getDbRoot())) {
            final int version = TableNameRegistryStore.findLastTablesFileVersion(
                    engine.getConfiguration().getFilesFacade(), registryPath, new StringSink()
            );
            registryPath.concat(TABLE_REGISTRY_NAME_FILE).putAscii('.').put(version);
            try (RandomAccessFile registry = new RandomAccessFile(registryPath.toString(), "rw")) {
                registry.setLength(Files.ceilPageSize(registry.length()));
            }
        }
        engine.reloadTableNames();

        try (MetadataCacheWriter cacheRW = engine.getMetadataCache().writeLock()) {
            cacheRW.clearCache();
        }
        engine.getMetadataCache().hydrateAllTables();
    }

    /**
     * Counts every file under a directory tree. The blocked disposition is about what does
     * <b>not</b> happen to those files, and a count of them is the cheapest statement of it that
     * an accidental partial reset cannot satisfy.
     */
    protected static long countFiles(File root) throws IOException {
        try (Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(root.toPath())) {
            return walk.filter(java.nio.file.Files::isRegularFile).count();
        }
    }

    /**
     * Reads the format version a {@code _timeline} declares, longhand - a little-endian INT at a
     * fixed offset - rather than through the production probe the cases are about. A case whose
     * whole premise is "this directory was written in another layout version" has to say so from
     * the bytes, or it merely re-states what the code under test decided.
     */
    protected static int readSuperblockFormatVersion(File checkpointsRoot) throws IOException {
        final File timeline = new File(checkpointsRoot, LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        final byte[] bytes = java.nio.file.Files.readAllBytes(timeline.toPath());
        final int offset = LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION_OFFSET;
        return (bytes[offset] & 0xff)
                | ((bytes[offset + 1] & 0xff) << 8)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 3] & 0xff) << 24);
    }

    protected File checkpointsRoot(String viewName) {
        try (Path dir = checkpointsDir(instance(viewName))) {
            return new File(dir.toString());
        }
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

    /**
     * The constructor already holds native memory - the store's own {@link Path} and one per
     * reader it owns - before {@code of()} maps anything, and the caller's try-with-resources
     * only takes ownership of a store this method returns. So a failed open closes the store
     * here, or a corrupt fixture leaks every one of those allocations on its way out.
     */
    protected LiveViewCheckpointMetaStore openStore(LiveViewInstance instance) {
        final LiveViewCheckpointMetaStore store = new LiveViewCheckpointMetaStore(engine.getConfiguration());
        try (Path dir = checkpointsDir(instance)) {
            store.of(dir);
        } catch (Throwable th) {
            store.close();
            throw th;
        }
        return store;
    }

    /**
     * Closes a reader whose open failed, for the reason {@link #openStore} states.
     */
    protected LiveViewCheckpointTimelineReader openTimelineReader(LiveViewInstance instance) {
        final LiveViewCheckpointTimelineReader reader =
                new LiveViewCheckpointTimelineReader(engine.getConfiguration());
        try (Path dir = checkpointsDir(instance)) {
            reader.of(dir);
        } catch (Throwable th) {
            reader.close();
            throw th;
        }
        return reader;
    }
}
