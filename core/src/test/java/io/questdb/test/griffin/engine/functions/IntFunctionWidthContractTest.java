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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.IntFunction;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Pins {@link Function#isIntWidthStable()} against the one thing that can silently break it: a new
 * INT function that overrides {@code getLong()} to compute at long width and forgets to flip the
 * flag. {@link io.questdb.griffin.engine.functions.bool.InLongFunctionFactory} reads the flag to
 * decide whether an IN key must be read once per element width or once per row, so a stale {@code
 * true} makes {@code (a * b) IN (1, 5000000000)} compare the wrong key value against the wrong
 * element - the exact divergence the per-element width was added to close.
 * <p>
 * Rather than trust a hand-kept list, this walks the compiled classes: every {@link IntFunction}
 * subclass that declares {@code getLong(Record)} must also declare {@code isIntWidthStable()}.
 * Classes load without initialization, so no static initializer runs.
 */
public class IntFunctionWidthContractTest {

    @Test
    public void testEveryIntFunctionOverridingGetLongDeclaresIsIntWidthStable() throws Exception {
        final List<String> offenders = new ArrayList<>();
        final List<String> covered = new ArrayList<>();
        for (Class<?> clazz : loadQuestdbClasses()) {
            if (!IntFunction.class.isAssignableFrom(clazz) || clazz == IntFunction.class) {
                continue;
            }
            if (!declaresMethod(clazz, "getLong", Record.class)) {
                continue;
            }
            if (declaresMethod(clazz, "isIntWidthStable")) {
                covered.add(clazz.getName());
            } else {
                offenders.add(clazz.getName());
            }
        }

        Collections.sort(offenders);
        Assert.assertEquals(
                "an IntFunction that overrides getLong() computes at a width getInt() does not carry, "
                        + "so it must also override isIntWidthStable() to return false: " + offenders,
                Collections.emptyList(),
                offenders
        );
        // Guards the scan itself: if it stops finding classes, the assertion above passes vacuously.
        Assert.assertTrue("the class scan found no IntFunction with a getLong() override", covered.size() >= 11);
    }

    private static boolean declaresMethod(Class<?> clazz, String name, Class<?>... params) {
        try {
            clazz.getDeclaredMethod(name, params);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static List<Class<?>> loadQuestdbClasses() throws URISyntaxException, IOException {
        final Path root = Paths.get(IntFunction.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        final List<Class<?>> classes = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : (Iterable<Path>) paths.filter(p -> p.toString().endsWith(".class"))::iterator) {
                final String name = root.relativize(path).toString()
                        .replace(java.io.File.separatorChar, '.')
                        .replace(".class", "");
                try {
                    // Do not initialize: a static initializer could load a native library.
                    classes.add(Class.forName(name, false, IntFunction.class.getClassLoader()));
                } catch (Throwable ignore) {
                    // A class that cannot be linked here (missing optional dependency) cannot be an
                    // IntFunction in this module either.
                }
            }
        }
        Assert.assertFalse("no compiled classes found under " + root, classes.isEmpty());
        return classes;
    }
}
