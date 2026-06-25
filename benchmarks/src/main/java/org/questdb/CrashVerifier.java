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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContextImpl;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * PROCESS-CRASH-CONSISTENCY VERIFIER
 *
 * PURPOSE: After CrashIngestWriter is hard-killed (kill -9), this tool reopens the same
 * QuestDB database root and verifies:
 *   1. The engine reopens successfully (or reports a LOUD detected-corruption exception).
 *   2. All rows from 0 to count-1 have exactly the expected deterministic values:
 *        row[i].id == i
 *        row[i].v  == i * 2654435761L
 *        row[i].s  == SYMBOLS[i % SYMBOLS.length]
 *   3. count >= watermark (rows acknowledged before the kill survived).
 *   4. count is on a clean K-row commit boundary (no partial commits leaked through).
 *      Because a process kill does not flush the OS page cache, partially-written data
 *      from the in-flight commit (if any) must be rolled back by QuestDB's recovery.
 *
 * WHAT THIS TESTS — PROCESS-CRASH-CONSISTENCY, NOT POWER-LOSS DURABILITY:
 *   kill -9 does not wipe the OS page cache. Data that was committed and msync'd before
 *   the kill persists in the page cache and IS visible to this process. The test validates
 *   that QuestDB's recovery (torn-aux guards, _txn/_cv A/B metadata, _todo) leaves a
 *   CONSISTENT state: no torn values, no silent corruption, no gaps between rows, and
 *   no partial in-flight commit visible. Power-loss durability (page-cache flush to
 *   persistent storage) requires a separate dm-log-writes harness; this test does NOT
 *   cover that scenario.
 *
 * VERDICTS (printed to stdout; harness parses the first word):
 *   CONSISTENT count=<n> watermark=<w>
 *       All rows correct, no gaps, count on commit boundary, count >= watermark.
 *   LOUD_FAILURE: <msg>
 *       CairoException on engine open or query — detected corruption.  Acceptable but
 *       notable: means QuestDB's guards caught a torn state and refused to serve it.
 *   SILENT_CORRUPTION row=<i> expected_id=<ei> actual_id=<ai> ...
 *       Row values wrong or gap detected — silent corruption, exit code 2 (SERIOUS).
 *
 * Usage: java -cp benchmarks/target/benchmarks.jar org.questdb.CrashVerifier <db-root>
 */
public class CrashVerifier {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CrashVerifier <db-root>");
            System.exit(1);
        }
        final String dbRoot = args[0];
        final int K = CrashIngestWriter.K;
        final String[] SYMBOLS = CrashIngestWriter.SYMBOLS;

        // Read the acknowledged watermark written by CrashIngestWriter._progress
        // This is the count of rows the writer confirmed committed before it was killed.
        // It may be slightly behind the actual committed count (kill could land between
        // commit() returning and the _progress file being written/renamed), which is fine.
        long watermark = 0L;
        final File progressFile = new File(dbRoot, "_progress");
        if (progressFile.exists()) {
            try {
                watermark = Long.parseLong(
                        Files.readString(progressFile.toPath(), StandardCharsets.US_ASCII).trim());
            } catch (NumberFormatException e) {
                System.out.println("WARN: could not parse _progress file: " + e.getMessage());
            }
        } else {
            System.out.println("WARN: no _progress file found (killed before first commit?)");
        }

        System.out.println("watermark=" + watermark + " (acknowledged committed rows before kill)");

        // Open the engine in SYNC mode (same config as writer; mode is not stored on disk,
        // so we must pass it consistently — though SYNC only affects commit, not reopen)
        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public int getCommitMode() {
                return CommitMode.SYNC;
            }
        };

        final long count;
        try (CairoEngine engine = new CairoEngine(cfg)) {
            final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null
                    );

            // Query all rows ordered by id (natural insert order for this monotonic schema).
            // We read id (col 0), v (col 1), s (col 2) and verify each row deterministically.
            final String sql = "select id, v, s from " + CrashIngestWriter.TABLE_NAME
                    + " order by ts asc";

            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {

                long rowIndex = 0L;
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    final Record rec = cursor.getRecord();
                    while (cursor.hasNext()) {
                        final long actualId = rec.getLong(0);
                        final long actualV  = rec.getLong(1);
                        final CharSequence actualS = rec.getSymA(2);

                        // Expected deterministic values — same formulas as CrashIngestWriter
                        final long expectedId = rowIndex;
                        final long expectedV  = rowIndex * 2_654_435_761L;
                        final String expectedS = SYMBOLS[(int)(rowIndex % SYMBOLS.length)];

                        if (actualId != expectedId || actualV != expectedV
                                || !expectedS.equals(String.valueOf(actualS))) {
                            // SILENT_CORRUPTION: wrong value at an index the engine didn't
                            // detect. This is the critical failure mode — data looks readable
                            // but has incorrect content (torn write, metadata mismatch, etc.)
                            System.out.printf(
                                    "SILENT_CORRUPTION row=%d"
                                            + " expected_id=%d actual_id=%d"
                                            + " expected_v=%d actual_v=%d"
                                            + " expected_s=%s actual_s=%s%n",
                                    rowIndex, expectedId, actualId, expectedV, actualV,
                                    expectedS, actualS);
                            System.exit(2);
                        }
                        rowIndex++;
                    }
                }
                count = rowIndex;
            }
        } catch (CairoException e) {
            // Detected corruption or recovery failure — engine or query threw.
            // Acceptable but notable: QuestDB's guards caught a torn state.
            System.out.println("LOUD_FAILURE: " + e.getMessage());
            System.exit(1);
            return; // unreachable
        }

        // Verify commit-boundary alignment: count must be a multiple of K (each commit
        // is exactly K rows). A partial count means an in-flight commit leaked through —
        // that would be silent corruption even if all values happen to be correct.
        if (count % K != 0) {
            System.out.printf(
                    "SILENT_CORRUPTION count=%d is not a multiple of K=%d"
                            + " — partial in-flight commit visible (torn commit boundary)%n",
                    count, K);
            System.exit(2);
        }

        // Verify acknowledged rows survived (count >= watermark).
        // count may legally EXCEED watermark (writer was committed but hadn't written
        // _progress yet when killed), but it must never be less.
        if (count < watermark) {
            System.out.printf(
                    "SILENT_CORRUPTION count=%d < watermark=%d"
                            + " — acknowledged committed rows were lost%n",
                    count, watermark);
            System.exit(2);
        }

        System.out.printf("CONSISTENT count=%d watermark=%d%n", count, watermark);
    }
}
