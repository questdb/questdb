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

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.std.Misc;
import io.questdb.std.Zip;
import io.questdb.test.TestOs;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.TestOnly;

import java.io.IOException;
import java.nio.file.Path;

final class PGStandaloneFuzzSupport {
    private static final Object LOCK = new Object();

    private static PGFuzzHarness cleartextAuthHarness;
    private static CairoEngine engine;
    private static PGFuzzHarness parseHarness;
    private static Path root;
    private static PGFuzzHarness sessionHarness;
    private static boolean shutdownHookInstalled;
    private static PGFuzzHarness startupHarness;

    private PGStandaloneFuzzSupport() {
    }

    static PGFuzzHarness cleartextAuthHarness() throws Exception {
        synchronized (LOCK) {
            ensureStarted();
            if (cleartextAuthHarness == null) {
                cleartextAuthHarness = PGFuzzHarness.newCleartextAuth(engine);
            }
            return cleartextAuthHarness;
        }
    }

    @TestOnly
    static void closeForTest() {
        synchronized (LOCK) {
            close();
        }
    }

    static PGFuzzHarness parseHarness() throws Exception {
        synchronized (LOCK) {
            ensureStarted();
            if (parseHarness == null) {
                parseHarness = new PGFuzzHarness(engine);
            }
            return parseHarness;
        }
    }

    static PGFuzzHarness sessionHarness() throws Exception {
        synchronized (LOCK) {
            ensureStarted();
            if (sessionHarness == null) {
                sessionHarness = new PGFuzzHarness(engine);
            }
            return sessionHarness;
        }
    }

    static PGFuzzHarness startupHarness() throws Exception {
        synchronized (LOCK) {
            ensureStarted();
            if (startupHarness == null) {
                startupHarness = new PGFuzzHarness(engine, false);
            }
            return startupHarness;
        }
    }

    private static void close() {
        cleartextAuthHarness = Misc.free(cleartextAuthHarness);
        startupHarness = Misc.free(startupHarness);
        sessionHarness = Misc.free(sessionHarness);
        parseHarness = Misc.free(parseHarness);
        engine = Misc.free(engine);
        if (root != null) {
            TestUtils.removeTestPath(root.toString());
            root = null;
        }
    }

    private static void ensureStarted() throws Exception {
        if (engine != null) {
            return;
        }
        TestOs.init();
        Zip.init();
        Metrics.ENABLED.clear();
        TestFilesFacadeImpl.resetTracking();
        root = createRoot();
        java.nio.file.Files.createDirectories(root.resolve("db"));
        engine = new CairoEngine(new DefaultTestCairoConfiguration(root.resolve("db").toString(), root.toString()));
        engine.load();
        ColumnType.makeUtf8DefaultString();
        if (!shutdownHookInstalled) {
            Runtime.getRuntime().addShutdownHook(new Thread(PGStandaloneFuzzSupport::close, "pgwire-fuzz-shutdown"));
            shutdownHookInstalled = true;
        }
    }

    private static Path createRoot() throws IOException {
        final String configuredRoot = System.getProperty("questdb.pgwire.fuzz.root");
        if (configuredRoot != null && !configuredRoot.isEmpty()) {
            return Path.of(configuredRoot).toAbsolutePath();
        }
        return java.nio.file.Files.createTempDirectory("questdb-pgwire-fuzz-").toAbsolutePath();
    }
}
