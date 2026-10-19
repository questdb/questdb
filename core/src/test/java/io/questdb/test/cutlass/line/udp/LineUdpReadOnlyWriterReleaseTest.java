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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpLexer;
import io.questdb.cutlass.line.udp.LineUdpParserImpl;
import io.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies the C8 fix: LineUdpParserImpl releases its cached writers on the read-only (demoting)
 * branch of commitAll(), so a demote on a UDP-ingesting node is no longer refused forever.
 * <p>
 * The parser caches a TableWriter per ingested table under the "ilpUdp" lock for the receiver's
 * lifetime; that lock is correctly NOT classified as an internal lock reason, so the demote drain's
 * getBusyWriterCount() counts each cached writer as a busy client.
 * <p>
 * RED state (before the fix): the read-only branch of commitAll() cleared commitList but left the
 * writers cached and held. getBusyWriterCount() stayed at 1 forever -- the demote drain could never
 * settle. This test's post-release assertion (busy count back to 0) fails before the fix.
 * <p>
 * GREEN state (after the fix): the read-only branch rolls back, frees each cached writer back to the
 * pool, and clears the cache; getBusyWriterCount() returns to 0 and the parser keeps running.
 */
public class LineUdpReadOnlyWriterReleaseTest extends AbstractCairoTest {

    private final AtomicBoolean readOnly = new AtomicBoolean(false);

    /**
     * Drives an idempotent read-only commitAll() tick after release: the cache is empty, so the
     * branch must not NPE and the busy count must stay at 0. Proves the parser survives subsequent
     * read-only ticks.
     */
    @Test
    public void testReadOnlyCommitAllIdempotentAfterRelease() throws Exception {
        assertMemoryLeak(() -> {
            readOnly.set(false);
            final CairoConfiguration cfg = new DefaultTestCairoConfiguration(root);
            final LineUdpReceiverConfiguration udpCfg = new DefaultLineUdpReceiverConfiguration();
            try (CairoEngine engine = buildFlippableEngine(cfg)) {
                try (LineUdpParserImpl parser = new LineUdpParserImpl(engine, udpCfg)) {
                    ingest(parser, "udp_idem,tag=a field=1i 100000000000\n");
                    Assert.assertEquals(1, engine.getBusyWriterCount());

                    readOnly.set(true);
                    parser.commitAll();
                    Assert.assertEquals("first read-only tick must release the cache", 0, engine.getBusyWriterCount());

                    // A second read-only tick with an emptied cache must be a harmless no-op.
                    parser.commitAll();
                    Assert.assertEquals("second read-only tick must stay at 0 and not throw", 0, engine.getBusyWriterCount());
                }
            }
        });
    }

    /**
     * Ingests over UDP (caching a writer), flips the engine read-only, drives commitAll(), and asserts
     * the cached writer was released: getBusyWriterCount() returns to 0, so a subsequent demote drain
     * is not refused by a pinned "ilpUdp" writer.
     */
    @Test
    public void testReadOnlyCommitAllReleasesCachedWriter() throws Exception {
        assertMemoryLeak(() -> {
            readOnly.set(false);
            final CairoConfiguration cfg = new DefaultTestCairoConfiguration(root);
            final LineUdpReceiverConfiguration udpCfg = new DefaultLineUdpReceiverConfiguration();
            try (CairoEngine engine = buildFlippableEngine(cfg)) {
                try (LineUdpParserImpl parser = new LineUdpParserImpl(engine, udpCfg)) {
                    // Ingest as PRIMARY: this creates the table and caches its writer under "ilpUdp".
                    ingest(parser, "udp_release,tag=a field=1i 100000000000\n");
                    Assert.assertEquals(
                            "the cached udp writer must register as one busy client while held",
                            1, engine.getBusyWriterCount()
                    );

                    // Demote: the engine flips read-only. The next commitAll() tick must release the cache.
                    readOnly.set(true);
                    parser.commitAll();

                    Assert.assertEquals(
                            "the read-only branch must release the cached writer so the demote drain can settle",
                            0, engine.getBusyWriterCount()
                    );
                }
            }
        });
    }

    private CairoEngine buildFlippableEngine(CairoConfiguration cfg) {
        // completeInit defaults to true: the UDP ingest path creates a table and needs the table
        // name registry, unlike the writer-proxy fence tests that never touch it.
        return new CairoEngine(cfg) {
            @Override
            public boolean isReadOnlyMode() {
                return readOnly.get();
            }
        };
    }

    private void ingest(LineUdpParserImpl parser, String lines) {
        byte[] bytes = lines.getBytes(Files.UTF_8);
        int len = bytes.length;
        long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.putByte(mem + i, bytes[i]);
            }
            try (LineUdpLexer lexer = new LineUdpLexer(4096)) {
                lexer.withParser(parser);
                lexer.parse(mem, mem + len);
                lexer.parseLast();
                parser.commitAll();
            }
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
