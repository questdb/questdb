/*******************************************************************************
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

package io.questdb.test.cairo.crash;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The TRIPWIRE for the routing of {@code io.questdb.test.cairo.crash} across CI suites.
 *
 * <p>Tests in this package reach CI through two disjoint doors. Deterministic ones run on PRs via the
 * {@code **&#47;cairo/crash/**} leg of {@code ci/test-pipeline.yml}. The long sweeps and power-loss fuzzes
 * opt OUT of that leg with a fast assumption on {@code -Dquestdb.fuzz.nightly=true}, and are named ONE BY
 * ONE in the nightly job of {@code ci/test-fuzz-adaptive.yml}.
 *
 * <p>That hand-maintained list is the hazard. A new sweep subclass is skipped by PR CI (the assumption)
 * AND absent from the nightly list, so it runs in NO pipeline — exactly the hole that once swallowed this
 * whole package. The reverse drifts too: surefire is invoked with {@code -DfailIfNoTests=false} and
 * {@code -Dsurefire.failIfNoSpecifiedTests=false}, so a renamed class leaves an entry matching nothing and
 * the shard goes GREEN having run less than it claims.
 *
 * <p>So this asserts both directions, and asserts the scan itself found classes — a scan that silently
 * matches nothing would pass against any pipeline at all.
 */
public class NightlyCrashTestSchedulingTest {
    private static final String ADAPTIVE_PIPELINE = "ci/test-fuzz-adaptive.yml";
    private static final String CRASH_PACKAGE = "core/src/test/java/io/questdb/test/cairo/crash";
    // The nightly-gated sweeps that existed when this tripwire was written. A LOWER bound only: it
    // proves the scan below reached real source, so a broken scan cannot pass vacuously.
    private static final int MIN_EXPECTED_GATED = 10;
    private static final String NIGHTLY_PROPERTY = "questdb.fuzz.nightly";
    private static final Pattern SCHEDULED_CLASS = Pattern.compile("\\*\\*/cairo/crash/(\\w+)\\.java");
    // This scanner has to spell out the constructs it hunts for, so its own source matches them and it
    // would classify ITSELF as a nightly sweep. Excluded by name, explicitly, rather than by relying on
    // some incidental difference in how it happens to be written.
    private static final String SELF = NightlyCrashTestSchedulingTest.class.getSimpleName();

    /**
     * Every class the nightly pipeline names must still exist. A dead entry runs nothing, silently.
     */
    @Test
    public void testEveryScheduledClassStillExists() throws IOException {
        Path repoRoot = repoRoot();
        Assume.assumeNotNull(repoRoot);

        String pipeline = read(repoRoot.resolve(ADAPTIVE_PIPELINE));
        Path packageDir = repoRoot.resolve(CRASH_PACKAGE);

        List<String> scheduled = new ArrayList<>();
        Matcher m = SCHEDULED_CLASS.matcher(pipeline);
        while (m.find()) {
            scheduled.add(m.group(1));
        }

        Assert.assertTrue(
                "found no **/cairo/crash/*.java entries in " + ADAPTIVE_PIPELINE
                        + " — either the nightly job lost its test selection, or this scan is broken",
                scheduled.size() >= MIN_EXPECTED_GATED
        );

        for (String className : scheduled) {
            Assert.assertTrue(
                    ADAPTIVE_PIPELINE + " schedules " + className + ".java, which no longer exists in "
                            + CRASH_PACKAGE + ". Surefire runs with -DfailIfNoTests=false, so this entry"
                            + " matches nothing and the nightly shard passes WITHOUT running it.",
                    Files.exists(packageDir.resolve(className + ".java"))
            );
        }
    }

    /**
     * Every class that opts out of PR CI must be named in the nightly job — else it runs nowhere.
     */
    @Test
    public void testEveryNightlyGatedCrashTestIsScheduled() throws IOException {
        Path repoRoot = repoRoot();
        Assume.assumeNotNull(repoRoot);

        String pipeline = read(repoRoot.resolve(ADAPTIVE_PIPELINE));
        List<String> gated = new ArrayList<>();
        List<String> unscheduled = new ArrayList<>();

        try (Stream<Path> sources = Files.list(repoRoot.resolve(CRASH_PACKAGE))) {
            for (Path source : sources.sorted().toArray(Path[]::new)) {
                String name = source.getFileName().toString();
                if (!name.endsWith(".java")) {
                    continue;
                }
                String className = name.substring(0, name.length() - ".java".length());
                if (SELF.equals(className) || !isNightlyGatedTest(read(source), className)) {
                    continue;
                }
                gated.add(className);
                if (!pipeline.contains("/cairo/crash/" + className + ".java")) {
                    unscheduled.add(className);
                }
            }
        }

        Assert.assertTrue(
                "scanned " + CRASH_PACKAGE + " and found only " + gated.size() + " nightly-gated tests"
                        + " (expected at least " + MIN_EXPECTED_GATED + ") — the scan, not the pipeline,"
                        + " is what to fix here",
                gated.size() >= MIN_EXPECTED_GATED
        );
        Assert.assertEquals(
                "these tests skip PR CI on -Dquestdb.fuzz.nightly and are NOT named in " + ADAPTIVE_PIPELINE
                        + ", so they run in no pipeline at all. Add them to a nightly matrix leg: " + unscheduled,
                0,
                unscheduled.size()
        );
    }

    /**
     * Gated == opts out of the PR leg: either it extends the sweep base (which asserts the flag for every
     * subclass) or it reads the flag itself, as the exact property literal handed to {@code getBoolean}.
     * Abstract helpers and the facades are not tests.
     *
     * <p>Classification runs with comments stripped, so a javadoc paragraph about the nightly flag does not
     * route a test into the nightly. String literals are deliberately KEPT: the gate itself is a literal,
     * and erasing literals would erase the very signal being looked for.
     */
    private static boolean isNightlyGatedTest(String source, String className) {
        String code = stripComments(source);
        if (!code.contains("@Test")
                || Pattern.compile("\\babstract\\s+class\\s+" + Pattern.quote(className) + "\\b").matcher(code).find()) {
            return false;
        }
        return code.contains("extends AbstractAdaptiveCrashSweepTest") || code.contains('"' + NIGHTLY_PROPERTY + '"');
    }

    /**
     * Erases {@code //} and block comments so prose cannot vote on which suite a test belongs to. It does
     * not need to parse Java, only to remove the one construct that carries prose.
     */
    private static String stripComments(String source) {
        StringBuilder code = new StringBuilder(source.length());
        int i = 0;
        while (i < source.length()) {
            char c = source.charAt(i);
            if (c == '/' && i + 1 < source.length() && source.charAt(i + 1) == '/') {
                while (i < source.length() && source.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < source.length() && source.charAt(i + 1) == '*') {
                int end = source.indexOf("*/", i + 2);
                i = end < 0 ? source.length() : end + 2;
            } else {
                code.append(c);
                i++;
            }
        }
        return code.toString();
    }

    private static String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    /**
     * Walks up from the module dir to the checkout. Returns null when the sources are not on disk (an
     * Enterprise run against the OSS test-jar), which is the one legitimate reason to skip.
     */
    private static Path repoRoot() {
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        for (int i = 0; i < 4 && dir != null; i++) {
            if (Files.exists(dir.resolve(ADAPTIVE_PIPELINE)) && Files.isDirectory(dir.resolve(CRASH_PACKAGE))) {
                return dir;
            }
            dir = dir.getParent();
        }
        return null;
    }
}
