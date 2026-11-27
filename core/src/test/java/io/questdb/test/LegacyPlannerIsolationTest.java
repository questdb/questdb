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

package io.questdb.test;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class LegacyPlannerIsolationTest {
    private static final byte[] LEGACY_PACKAGE = "io/questdb/legacy/".getBytes(StandardCharsets.US_ASCII);

    @Test
    public void testOnlyTheCompilerFactoryReachesTheLegacyPlanner() throws IOException {
        // The legacy planner is deleted as a whole once the new planner has shipped, so the rest of the code reaches
        // it through SqlCompilerFactory.newLegacyPlanner() alone. A class that names a legacy class in its constant
        // pool would stop compiling then, or worse, would keep running the legacy code.
        final Path classes = Paths.get("target", "classes");
        Assume.assumeTrue(Files.isDirectory(classes));
        final Path legacy = classes.resolve(Paths.get("io", "questdb", "legacy"));
        // the module exports the legacy packages Enterprise extends, which names them without reaching any class
        final Path moduleInfo = classes.resolve("module-info.class");
        final List<String> referencing = new ArrayList<>();
        try (Stream<Path> files = Files.walk(classes)) {
            files.filter(file -> file.toString().endsWith(".class") && !file.startsWith(legacy) && !file.equals(moduleInfo)).sorted().forEach(file -> {
                try {
                    if (contains(Files.readAllBytes(file), LEGACY_PACKAGE)) {
                        referencing.add(classes.relativize(file).toString());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        Assert.assertEquals(
                "[" + Paths.get("io", "questdb", "griffin", "SqlCompilerFactory.class") + "]",
                referencing.toString()
        );
    }

    private static boolean contains(byte[] bytes, byte[] pattern) {
        outer:
        for (int i = 0, n = bytes.length - pattern.length; i <= n; i++) {
            for (int j = 0; j < pattern.length; j++) {
                if (bytes[i + j] != pattern[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }
}
