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
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
 * true} makes {@code (a * b) IN (1, 5_000_000_000)} compare the wrong key value against the wrong
 * element - the exact divergence the per-element width was added to close.
 * <p>
 * Rather than trust a hand-kept list, this walks the compiled classes: every {@link IntFunction}
 * subclass that declares {@code getLong(Record)} must also declare {@code isIntWidthStable()} AND
 * report {@code false} from it. Checking only the declaration would miss the other half of the
 * hazard - a class that overrides {@code getLong()} and then writes {@code isIntWidthStable()}
 * returning {@code true}, which reads exactly as broken as inheriting the default. The few classes
 * that override {@code getLong()} and are genuinely width-stable are listed, with their argument, in
 * {@link #WIDTH_STABLE_BY_DESIGN}. Classes load without initialization, so no static initializer runs.
 */
public class IntFunctionWidthContractTest {
    // The IntFunctions that override getLong(Record) and are nevertheless width-stable. Each one is
    // reviewed: its getLong() carries exactly what Numbers.intToLong(getInt()) would, so the flag is
    // honestly true. A new class only lands here after someone proves the same, which is the point -
    // reporting true is the dangerous answer (it is also the interface default), so it must be
    // argued for rather than inherited by accident.
    private static final List<String> WIDTH_STABLE_BY_DESIGN = List.of(
            // Folds a symbol switch to a constant int: getLong() widens that same constant.
            "io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory$SymbolSwitchConstIntFunction",
            // Caches whatever the wrapped arg reports at each width, so it is width-stable exactly
            // when the arg is - it answers from the arg, and cannot be evaluated without one.
            "io.questdb.griffin.engine.functions.RuntimeConstFunction$IntRuntimeConstFunction"
    );
    // The scan must keep finding the width-unstable functions (the arithmetic and bitwise INT
    // operators); if it stops, every assertion here passes vacuously.
    private static final int MIN_EXPECTED_GETLONG_OVERRIDES = 11;

    @Test
    public void testEveryIntFunctionOverridingGetLongReportsWidthUnstable() throws Exception {
        final List<String> undeclared = new ArrayList<>();
        final List<String> misreported = new ArrayList<>();
        final List<String> covered = new ArrayList<>();
        for (Class<?> clazz : loadQuestdbClasses()) {
            if (!IntFunction.class.isAssignableFrom(clazz) || clazz == IntFunction.class) {
                continue;
            }
            if (!declaresMethod(clazz, "getLong", Record.class)) {
                continue;
            }
            if (!declaresMethod(clazz, "isIntWidthStable")) {
                undeclared.add(clazz.getName());
                continue;
            }
            covered.add(clazz.getName());
            // Declaring the override is not enough: a class that declares it and returns true is
            // just as broken as one that inherits the default true, and reads the IN key at the
            // wrong width. Ask the class what it actually reports.
            if (!isWidthStable(clazz) || WIDTH_STABLE_BY_DESIGN.contains(clazz.getName())) {
                continue;
            }
            misreported.add(clazz.getName());
        }

        Collections.sort(undeclared);
        Collections.sort(misreported);
        Assert.assertEquals(
                "an IntFunction that overrides getLong() computes at a width getInt() does not carry, "
                        + "so it must also override isIntWidthStable() to return false: " + undeclared,
                Collections.emptyList(),
                undeclared
        );
        Assert.assertEquals(
                "an IntFunction that overrides getLong() reports isIntWidthStable() == true. InLongFunctionFactory "
                        + "then reads the IN key once per row instead of once per element width, so an overflowing "
                        + "key compares at the wrong width. Return false, or add it to WIDTH_STABLE_BY_DESIGN with "
                        + "the argument for why getLong() carries what getInt() does: " + misreported,
                Collections.emptyList(),
                misreported
        );
        Assert.assertTrue(
                "the class scan found only " + covered.size() + " IntFunctions with a getLong() override; it is "
                        + "no longer reaching the arithmetic operators, so this test guards nothing",
                covered.size() >= MIN_EXPECTED_GETLONG_OVERRIDES
        );
    }

    private static boolean declaresMethod(Class<?> clazz, String name, Class<?>... params) {
        try {
            clazz.getDeclaredMethod(name, params);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    /**
     * What {@code clazz} actually reports from {@code isIntWidthStable()}. The instance comes from
     * {@code allocateInstance}, not a constructor: these functions are built by their factories from
     * parsed arguments, and the flag is a per-class constant that reads no state. A class whose
     * answer does depend on state (it delegates to an arg) throws on the null field and is reported
     * as width-stable, i.e. it has to be argued for in {@link #WIDTH_STABLE_BY_DESIGN} - the
     * conservative direction, since that is the list a human reviews.
     */
    private static boolean isWidthStable(Class<?> clazz) throws Exception {
        if (Modifier.isAbstract(clazz.getModifiers())) {
            return true;
        }
        final Method method = clazz.getMethod("isIntWidthStable");
        method.setAccessible(true);
        final Object instance = Unsafe.getUnsafe().allocateInstance(clazz);
        try {
            return (boolean) method.invoke(instance);
        } catch (InvocationTargetException stateDependent) {
            return true;
        }
    }

    /**
     * Loads every compiled class under the module output, so the scan sees the whole IntFunction
     * inventory rather than a hand-kept list. A class this cannot load is reported, not skipped:
     * swallowing the failure is what would let a broken - or simply new - IntFunction drop out of the
     * inventory while every assertion above it still passes. The only names expected to fail are the
     * synthetic {@code module-info} / {@code package-info}, which are not classes anyone can load and
     * cannot be IntFunctions either.
     */
    private static List<Class<?>> loadQuestdbClasses() throws URISyntaxException, IOException {
        final Path root = Paths.get(IntFunction.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        final List<Class<?>> classes = new ArrayList<>();
        final List<String> unloadable = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : (Iterable<Path>) paths.filter(p -> p.toString().endsWith(".class"))::iterator) {
                final String name = root.relativize(path).toString()
                        .replace(java.io.File.separatorChar, '.')
                        .replace(".class", "");
                if (name.equals("module-info") || name.endsWith(".package-info")) {
                    continue;
                }
                try {
                    // Do not initialize: a static initializer could load a native library.
                    classes.add(Class.forName(name, false, IntFunction.class.getClassLoader()));
                } catch (Throwable th) {
                    unloadable.add(name + ": " + th);
                }
            }
        }
        Collections.sort(unloadable);
        Assert.assertEquals(
                "a compiled class could not be loaded, so the IntFunction inventory below is incomplete and "
                        + "the contract it pins may be unenforced for the missing classes: " + unloadable,
                Collections.emptyList(),
                unloadable
        );
        Assert.assertFalse("no compiled classes found under " + root, classes.isEmpty());
        return classes;
    }
}
