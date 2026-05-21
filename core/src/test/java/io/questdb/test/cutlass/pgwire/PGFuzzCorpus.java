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

import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

final class PGFuzzCorpus {

    private PGFuzzCorpus() {
    }

    static int replay(Class<?> owner, String resourcePath, CorpusInputConsumer consumer) throws Exception {
        final URL corpusResource = owner.getResource(resourcePath);
        Assert.assertNotNull("missing pgwire corpus resource directory: " + resourcePath, corpusResource);

        final int[] count = {0};
        final Path corpusRoot = toPath(corpusResource);
        try (Stream<Path> paths = Files.walk(corpusRoot)) {
            paths
                    .filter(Files::isRegularFile)
                    .sorted()
                    .forEach(path -> replay(path, consumer, count));
        }
        return count[0];
    }

    static byte[] decodeHex(CharSequence hex, Path path) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream(hex.length() / 2);
        int hi = -1;
        boolean comment = false;
        for (int i = 0, n = hex.length(); i < n; i++) {
            final char c = hex.charAt(i);
            if (comment) {
                if (c == '\n' || c == '\r') {
                    comment = false;
                }
                continue;
            }
            if (c == '#') {
                comment = true;
                continue;
            }
            if (Character.isWhitespace(c)) {
                continue;
            }
            final int nibble = Character.digit(c, 16);
            if (nibble == -1) {
                throw new IllegalArgumentException("invalid hex digit in " + path + " at char " + i);
            }
            if (hi == -1) {
                hi = nibble;
            } else {
                out.write((hi << 4) | nibble);
                hi = -1;
            }
        }
        if (hi != -1) {
            throw new IllegalArgumentException("odd number of hex digits in " + path);
        }
        return out.toByteArray();
    }

    static byte[] readCorpusInput(Path path) throws IOException {
        final byte[] bytes = Files.readAllBytes(path);
        if (path.getFileName().toString().endsWith(".pghex")) {
            return decodeHex(new String(bytes, StandardCharsets.US_ASCII), path);
        }
        return bytes;
    }

    private static void replay(Path path, CorpusInputConsumer consumer, int[] count) {
        try {
            consumer.accept(readCorpusInput(path));
            count[0]++;
        } catch (Throwable th) {
            throw new AssertionError("pgwire corpus input failed: " + path, th);
        }
    }

    private static Path toPath(URL resource) throws URISyntaxException {
        return Paths.get(resource.toURI());
    }

    interface CorpusInputConsumer {
        void accept(byte[] input) throws Exception;
    }
}
