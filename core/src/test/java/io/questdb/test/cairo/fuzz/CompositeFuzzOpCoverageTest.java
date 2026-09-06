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

package io.questdb.test.cairo.fuzz;

import io.questdb.test.fuzz.FuzzTransactionOperation;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Operation-coverage guard for {@link CompositeFuzzRunner.Support} (Task 5's classification map).
 * <p>
 * A hard-coded list of {@code Fuzz*Operation} classes here would defeat the whole point: a NEW
 * operation type landing in {@code io.questdb.test.fuzz} would silently never be checked, and the
 * fuzz harness would silently either apply it unchecked to a composite table (risking a false green
 * or, worse, real corruption if the new op happens to be composite-unsafe) or never generate it at
 * all. Instead this scans the compiled {@code io.questdb.test.fuzz} package ON THE TEST CLASSPATH --
 * the actual, current set of operation classes -- and cross-checks each one against {@link
 * CompositeFuzzRunner#classify}.
 */
public class CompositeFuzzOpCoverageTest {
    private static final String FUZZ_PACKAGE = "io.questdb.test.fuzz";
    private static final String FUZZ_PACKAGE_PATH = FUZZ_PACKAGE.replace('.', '/');

    @Test
    public void testEveryFuzzOperationIsClassified() throws Exception {
        List<Class<?>> discovered = discoverFuzzOperationClasses();
        // A guard that silently discovers zero classes (e.g. because the classpath scan itself broke
        // on some future build layout) would pass vacuously, proving nothing -- fail loudly instead,
        // the same anti-vacuity discipline the rest of this harness applies to fuzz RUNS.
        Assert.assertFalse(
                "package scan under " + FUZZ_PACKAGE + " discovered zero Fuzz*Operation classes -- " +
                        "the scan itself is broken, not merely the classification (this guard would " +
                        "otherwise pass vacuously)",
                discovered.isEmpty());

        List<String> unclassified = new ArrayList<>();
        for (Class<?> clazz : discovered) {
            @SuppressWarnings("unchecked")
            Class<? extends FuzzTransactionOperation> opClass = (Class<? extends FuzzTransactionOperation>) clazz;
            if (CompositeFuzzRunner.classify(opClass) == null) {
                unclassified.add(clazz.getName());
            }
        }

        if (!unclassified.isEmpty()) {
            Assert.fail(
                    "the following " + FUZZ_PACKAGE + " classes implement FuzzTransactionOperation but " +
                            "have no CompositeFuzzRunner.Support classification -- each must be added to " +
                            "CompositeFuzzRunner's OPERATION_SUPPORT map as either SUPPORTED (safe to apply " +
                            "unchanged to both twins) or GATED (must only be exercised via " +
                            "applyGatedOperation, expecting a throw and no damage): " + unclassified);
        }
    }

    /**
     * Scans the compiled {@code io.questdb.test.fuzz} package for every concrete class implementing
     * {@link FuzzTransactionOperation}, via the classloader's resource listing for that package --
     * i.e. whatever classes actually exist on the test classpath right now, not a list maintained by
     * hand. Handles both an exploded-directory classpath entry (the ordinary {@code mvn test} shape:
     * {@code target/test-classes/io/questdb/test/fuzz/*.class}) and a jar classpath entry, since
     * either is possible depending on how this module is built/run.
     */
    private static List<Class<?>> discoverFuzzOperationClasses() throws Exception {
        List<Class<?>> result = new ArrayList<>();
        Enumeration<URL> roots = Thread.currentThread().getContextClassLoader().getResources(FUZZ_PACKAGE_PATH);
        while (roots.hasMoreElements()) {
            URL root = roots.nextElement();
            URLConnection connection = root.openConnection();
            if (connection instanceof JarURLConnection jarConnection) {
                try (JarFile jarFile = jarConnection.getJarFile()) {
                    Enumeration<JarEntry> entries = jarFile.entries();
                    while (entries.hasMoreElements()) {
                        JarEntry entry = entries.nextElement();
                        String name = entry.getName();
                        if (name.startsWith(FUZZ_PACKAGE_PATH + "/") && name.endsWith(".class")) {
                            addIfConcreteOperation(result, classNameFromEntry(name));
                        }
                    }
                }
            } else {
                File dir = new File(root.toURI());
                File[] files = dir.listFiles((d, name) -> name.endsWith(".class"));
                if (files != null) {
                    for (File f : files) {
                        String simpleName = f.getName().substring(0, f.getName().length() - ".class".length());
                        addIfConcreteOperation(result, FUZZ_PACKAGE + "." + simpleName);
                    }
                }
            }
        }
        return result;
    }

    private static String classNameFromEntry(String entryName) {
        String withoutExt = entryName.substring(0, entryName.length() - ".class".length());
        return withoutExt.replace('/', '.');
    }

    /**
     * Filters a discovered class name down to "a real, concrete Fuzz*Operation": excludes nested /
     * inner / anonymous / synthetic classes (name contains {@code $} -- e.g. a lambda or local class
     * some operation's implementation happens to declare), the {@link FuzzTransactionOperation}
     * interface itself, and any abstract class, since none of those are things the fuzz generator
     * could ever actually instantiate and apply.
     */
    private static void addIfConcreteOperation(List<Class<?>> result, String className) throws ClassNotFoundException {
        if (className.indexOf('$') >= 0) {
            return;
        }
        Class<?> clazz = Class.forName(className);
        if (FuzzTransactionOperation.class.isAssignableFrom(clazz)
                && clazz != FuzzTransactionOperation.class
                && !clazz.isInterface()
                && !Modifier.isAbstract(clazz.getModifiers())) {
            result.add(clazz);
        }
    }
}
