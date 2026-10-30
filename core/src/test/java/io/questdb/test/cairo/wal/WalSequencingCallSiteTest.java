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

package io.questdb.test.cairo.wal;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A WAL writer sequences only through {@code WalWriterBase.nextSeqTxn}, which first flushes every event record the
 * writer has not flushed yet. A writer that called the sequencer append directly could put a sequencer record
 * ahead of the {@code _event} entry it names in a durable mode; this scan fails when such a call appears.
 */
public class WalSequencingCallSiteTest {
    private static final String MAIN_SOURCES = "core/src/main/java";
    private static final Pattern SEQUENCER_APPEND = Pattern.compile("\\.nextTxn\\(");
    private static final String WAL_WRITER_BASE = MAIN_SOURCES + "/io/questdb/cairo/wal/WalWriterBase.java";

    @Test
    public void testOnlyWalWriterBaseAppendsToTheSequencer() throws IOException {
        final Path root = repoRoot();
        // The sources are not on disk when Enterprise runs the OSS test jar.
        Assume.assumeNotNull(root);
        final List<String> callers = new ArrayList<>();
        try (Stream<Path> files = Files.walk(root.resolve(MAIN_SOURCES))) {
            for (Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".java"))::iterator) {
                if (SEQUENCER_APPEND.matcher(Files.readString(file)).find()) {
                    callers.add(file.getFileName().toString());
                }
            }
        }
        callers.sort(String::compareTo);
        // TableSequencerAPI delegates to the sequencer it locks; every other caller must be the writer base.
        Assert.assertEquals(List.of("TableSequencerAPI.java", "WalWriterBase.java"), callers);
    }

    private static Path repoRoot() {
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        for (int i = 0; i < 4 && dir != null; i++) {
            if (Files.exists(dir.resolve(WAL_WRITER_BASE))) {
                return dir;
            }
            dir = dir.getParent();
        }
        return null;
    }
}
