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

package io.questdb.test.cairo.sql;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionAllRecordCursorFactory;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.stream.Stream;

/**
 * Forcing function for the INT-width answer PAIR.
 * <p>
 * {@link RecordCursorFactory#isColumnIntWidthStable(int)} and
 * {@link RecordCursorFactory#isColumnRowStable(int)} answer two halves of one question, and their
 * defaults point in opposite directions - true and false respectively. A factory that overrides the
 * first and forgets the second therefore reports its column width-unstable while claiming it cannot
 * be read twice, and the consumers that read a width-unstable column at both widths
 * ({@code NullIfIntFunctionFactory}, {@code CoalesceFunctionFactory},
 * {@code InLongFunctionFactory}) pick a different comparison width for it. The same expression then
 * yields different values depending on whether that factory sits between the projection and its
 * base - the plan-shape dependence the width rule exists to remove.
 * <p>
 * Per-shape tests can only cover the factories someone thought of, which is exactly how the width
 * enumeration was missed the first time round. This walks every compiled class instead, so a new
 * factory that answers one question and not the other reddens here rather than in a user's query.
 * Reflection is the only route: there is no public API that reaches the class hierarchy.
 * <p>
 * Narrow unit test: loads classes without initialising them, so no engine and no native memory.
 */
public class IntWidthAnswerPairingTest {

    // A few factories the walk must have reached. Without them a loader change that made every
    // class fail to link would leave the scan empty and the test green.
    private static final Class<?>[] SENTINELS = {
            LimitRecordCursorFactory.class,
            SelectedRecordCursorFactory.class,
            UnionAllRecordCursorFactory.class
    };

    @Test
    public void testEveryFactoryAnsweringWidthAlsoAnswersRowStability() throws Exception {
        final Path classesRoot = compiledClassesRoot();
        // Running from a jar (or any layout without a classes directory) leaves nothing to walk.
        Assume.assumeNotNull(classesRoot);

        final ObjList<String> offenders = new ObjList<>();
        final CharSequenceHashSet reached = new CharSequenceHashSet();
        int factoriesScanned = 0;
        int pairsFound = 0;
        try (Stream<Path> paths = Files.walk(classesRoot)) {
            for (Path path : paths.filter(p -> p.toString().endsWith(".class")).toList()) {
                final Class<?> candidate = loadQuietly(classesRoot, path);
                if (candidate == null || !RecordCursorFactory.class.isAssignableFrom(candidate)) {
                    continue;
                }
                factoriesScanned++;
                reached.add(candidate.getName());
                if (!declaresMethod(candidate, "isColumnIntWidthStable")) {
                    continue;
                }
                if (declaresMethod(candidate, "isColumnRowStable")) {
                    pairsFound++;
                } else {
                    offenders.add(candidate.getName());
                }
            }
        }

        // The walk itself must have worked, or the test would pass by scanning nothing.
        Assert.assertTrue("no RecordCursorFactory implementations found under " + classesRoot, factoriesScanned > 50);
        Assert.assertTrue("no width overrides found - the scan is not reaching the factories", pairsFound > 10);
        for (Class<?> sentinel : SENTINELS) {
            Assert.assertTrue("the walk never reached " + sentinel.getName() + " - class loading is failing silently",
                    reached.contains(sentinel.getName()));
        }
        if (offenders.size() > 0) {
            Assert.fail("these factories override isColumnIntWidthStable but not isColumnRowStable, so a"
                    + " column they report width-unstable is also reported row-unstable, which changes"
                    + " what nullif / coalesce / IN return for it: " + offenders);
        }
    }

    @Test
    public void testRowStabilityDefaultsAreOppositeAndConservative() {
        // The pairing above only matters because the two defaults point in opposite directions.
        // Pin them, so a change to either one is a deliberate act rather than a silent flip.
        final RecordCursorFactory defaults = new RecordCursorFactoryDefaults();
        Assert.assertTrue(defaults.isColumnIntWidthStable(0));
        Assert.assertFalse(defaults.isColumnRowStable(0));
        final ColumnTypes types = new ColumnTypesDefaults();
        Assert.assertTrue(types.isColumnIntWidthStable(0));
        Assert.assertFalse(types.isColumnRowStable(0));
    }

    private static Path compiledClassesRoot() {
        final CodeSource codeSource = RecordCursorFactory.class.getProtectionDomain().getCodeSource();
        if (codeSource == null || codeSource.getLocation() == null) {
            return null;
        }
        try {
            // Through the URI, not getPath(): the latter keeps percent-encoding (a build directory
            // with a space in it would resolve to nothing) and on Windows it carries a leading slash
            // before the drive letter, which Paths.get rejects outright.
            final Path root = Paths.get(codeSource.getLocation().toURI());
            return Files.isDirectory(root) ? root : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean declaresMethod(Class<?> type, String name) {
        try {
            type.getDeclaredMethod(name, int.class);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static Class<?> loadQuietly(Path root, Path classFile) {
        final String relative = root.relativize(classFile).toString();
        final String className = relative
                .substring(0, relative.length() - ".class".length())
                .replace(File.separatorChar, '.');
        try {
            // initialize=false: the walk touches every class in the module, and running static
            // initialisers would load native libraries and build engine-wide state.
            return Class.forName(className, false, IntWidthAnswerPairingTest.class.getClassLoader());
        } catch (Throwable th) {
            // A class that cannot be loaded at all cannot be a factory this test has to police.
            // The sentinel check above catches a loader change that made this the common case.
            return null;
        }
    }

    private static class ColumnTypesDefaults implements ColumnTypes {
        @Override
        public int getColumnCount() {
            return 1;
        }

        @Override
        public int getColumnType(int columnIndex) {
            return 0;
        }
    }

    private static class RecordCursorFactoryDefaults implements RecordCursorFactory {
        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordMetadata getMetadata() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }
    }
}
