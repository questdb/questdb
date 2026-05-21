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

package io.questdb.test.cutlass.pgwire;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

public final class PGFuzzCorpusMaterializer {

    private PGFuzzCorpusMaterializer() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: PGFuzzCorpusMaterializer <source-corpus-dir> <target-corpus-dir>");
        }
        final Path sourceRoot = Path.of(args[0]).toAbsolutePath();
        final Path targetRoot = Path.of(args[1]).toAbsolutePath();
        if (!Files.isDirectory(sourceRoot)) {
            throw new IllegalArgumentException("source corpus directory does not exist: " + sourceRoot);
        }

        deleteIfExists(targetRoot);
        try (Stream<Path> paths = Files.walk(sourceRoot)) {
            paths
                    .filter(Files::isRegularFile)
                    .sorted()
                    .forEach(path -> materialize(sourceRoot, targetRoot, path));
        }
    }

    private static void deleteIfExists(Path targetRoot) throws IOException {
        if (!Files.exists(targetRoot)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(targetRoot)) {
            final Path[] ordered = paths
                    .sorted(Comparator.reverseOrder())
                    .toArray(Path[]::new);
            for (Path path : ordered) {
                Files.delete(path);
            }
        }
    }

    private static void materialize(Path sourceRoot, Path targetRoot, Path source) {
        try {
            final Path relative = sourceRoot.relativize(source);
            final Path target = targetRoot.resolve(rewritePgHexExtension(relative));
            Files.createDirectories(target.getParent());
            Files.write(target, PGFuzzCorpus.readCorpusInput(source));
        } catch (IOException e) {
            throw new RuntimeException("could not materialize pgwire corpus input: " + source, e);
        }
    }

    private static Path rewritePgHexExtension(Path relative) {
        final Path fileName = relative.getFileName();
        if (fileName == null) {
            return relative;
        }
        final String name = fileName.toString();
        if (!name.endsWith(".pghex")) {
            return relative;
        }
        final Path parent = relative.getParent();
        final Path rewritten = Path.of(name.substring(0, name.length() - ".pghex".length()) + ".bin");
        return parent == null ? rewritten : parent.resolve(rewritten);
    }
}
