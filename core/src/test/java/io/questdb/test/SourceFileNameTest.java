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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class SourceFileNameTest {

    @Test
    public void testEverySourceCompilesToClassOfItsName() throws IOException {
        // Maven's compiler plugin pairs Foo.java with Foo.class. A source without that class, such as an
        // empty file or one whose only top-level type has another name, looks changed on every build and
        // makes Maven recompile the whole module.
        Path mainClasses = Paths.get("target", "classes");
        Path testClasses = Paths.get("target", "test-classes");
        Assume.assumeTrue(Files.isDirectory(mainClasses) && Files.isDirectory(testClasses));
        List<String> sourcesWithoutClass = new ArrayList<>();
        collectSourcesWithoutClass(Paths.get("src", "main", "java"), mainClasses, sourcesWithoutClass);
        collectSourcesWithoutClass(Paths.get("src", "test", "java"), testClasses, sourcesWithoutClass);
        Assert.assertEquals(
                "rename each file after its top-level type, or delete it if it is empty",
                "[]",
                sourcesWithoutClass.toString()
        );
    }

    private static void collectSourcesWithoutClass(Path sourceRoot, Path classRoot, List<String> sink) throws IOException {
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            files.filter(file -> file.toString().endsWith(".java")).sorted().forEach(file -> {
                String source = sourceRoot.relativize(file).toString();
                Path classFile = classRoot.resolve(source.substring(0, source.length() - ".java".length()) + ".class");
                if (!Files.exists(classFile)) {
                    sink.add(file.toString());
                }
            });
        }
    }
}
