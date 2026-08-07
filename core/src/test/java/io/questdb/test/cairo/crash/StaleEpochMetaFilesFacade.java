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

package io.questdb.test.cairo.crash;

import io.questdb.cairo.TableUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Substitutes the content of a durable epoch's {@code _meta.epoch.N} as the writer publishes it, so the
 * epoch's copies end up describing two different table shapes.
 * <p>
 * Substituting at PUBLISH time rather than editing the file afterwards is the whole point. The epoch is
 * protected by two integrity layers — each payload's own A/B checksum, and
 * {@link io.questdb.cairo.DurableEpochManifest}, which records the size and checksum of all three payloads.
 * A test that edited {@code _meta.epoch.N} after the fact would be rejected by the manifest, for the wrong
 * reason, and would "pass" whether or not the semantic check under test exists — a vacuous control.
 * <p>
 * Swapping the bytes mid-publish avoids that: {@code TableWriter.writeEpochCopy} writes the copy, this
 * facade replaces its content, and the PRODUCT then computes the manifest over what is actually on disk. The
 * result is an epoch that is perfectly self-consistent to every checksum and still semantically skewed —
 * which is exactly the state the write-side defect produced, and the only state in which the semantic
 * cross-check in {@code RecoveryCoordinator.epochCopiesValid} is the thing standing between recovery and an
 * unopenable table.
 * <p>
 * Armed explicitly and for one epoch only; while disarmed it is a plain pass-through facade.
 */
public class StaleEpochMetaFilesFacade extends TestFilesFacadeImpl {

    // fd -> path, for _meta.epoch.* files opened for writing while armed. Entries are removed on close, so
    // a later reuse of the same descriptor number cannot inherit a substitution.
    private final java.util.HashMap<Long, String> epochMetaFds = new java.util.HashMap<>();
    private String substitutePath;
    /** Paths whose content was replaced. Tests assert this is non-empty, so a silently-dead facade fails. */
    public final java.util.List<String> substituted = new java.util.ArrayList<>();

    /**
     * From now until {@link #disarm()}, every {@code _meta.epoch.*} the writer publishes gets the content of
     * {@code path} instead of the live {@code _meta}.
     */
    public void armWith(String path) {
        this.substitutePath = path;
    }

    @Override
    public boolean close(long fd) {
        // Replace BEFORE the descriptor goes away, so the writer's own post-copy fsync (writeEpochCopy
        // reopens the destination to fsync it) and the manifest's later checksum read both see these bytes.
        final String path = takePath(fd);
        final boolean closed = super.close(fd);
        if (path != null) {
            substitute(path);
        }
        return closed;
    }

    public void disarm() {
        this.substitutePath = null;
        epochMetaFds.clear();
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        final long fd = super.openRW(name, opts);
        if (fd > -1 && substitutePath != null) {
            final String path = toPath(name);
            if (isEpochMeta(path)) {
                epochMetaFds.put(fd, path);
            }
        }
        return fd;
    }

    /**
     * LPSZ.toString() does NOT yield the path text -- it must be decoded through Utf8String and cut at the
     * NUL terminator, exactly as CrashFaultFilesFacade does. Getting this wrong is silent: every match test
     * simply returns false and the facade becomes a no-op that still looks armed.
     */
    private static String toPath(LPSZ name) {
        String path = io.questdb.std.str.Utf8String.newInstance(name).toString();
        final int nul = path.indexOf('\0');
        return nul > -1 ? path.substring(0, nul) : path;
    }

    private static boolean isEpochMeta(String path) {
        // "<table>/_meta.epoch.<generation>" -- never the live "_meta", and never the _txn/_cv copies.
        return path.contains(TableUtils.META_FILE_NAME + TableUtils.EPOCH_COPY_SUFFIX + '.');
    }

    private String takePath(long fd) {
        return epochMetaFds.remove(fd);
    }

    private void substitute(String path) {
        if (substitutePath == null) {
            return;
        }
        try {
            Files.copy(Paths.get(substitutePath), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
            substituted.add(path);
        } catch (Exception e) {
            throw new AssertionError("could not substitute epoch _meta content [dst=" + path + ']', e);
        }
    }
}
