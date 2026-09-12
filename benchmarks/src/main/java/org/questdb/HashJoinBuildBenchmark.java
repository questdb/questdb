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

package org.questdb;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.LongChain;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Os;
import io.questdb.std.PerQueryMemoryTrackerProvider;
import io.questdb.std.Rnd;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

/**
 * RFC 130 task 2 storage comparison. Uses the light join's actual map/LongChain
 * row-ID layout and native-table recordAt, versus copied SYMBOL/DOUBLE payloads.
 * Single-threaded component timings; this does not measure the fused SQL pipeline.
 */
public class HashJoinBuildBenchmark {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;

    /** Arguments: distinct keys, duplicate fanout, probe count, source revision. */
    public static void main(String[] args) throws Exception {
        int keys = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
        int fanout = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        int probes = args.length > 2 ? Integer.parseInt(args[2]) : 2_000_000;
        if (args.length > 4 || keys < 1 || keys > 10_000_000 || fanout < 1 || fanout > 1000 || probes < 1) {
            throw new IllegalArgumentException("usage: HashJoinBuildBenchmark [keys=10000] [fanout=1] [probes=2000000] [revision]");
        }
        Os.init();
        Path root = Files.createTempDirectory("hash-join-build-");
        System.out.println("# data_directory=" + root + " (retained for inspection)");
        System.out.printf(Locale.ROOT, "# revision=%s java=%s vm_args=%s keys=%d fanout=%d probes=%d seed=130 match_rate=10%% workers=1 warmups=3 runs=10 repetitions=2%n",
                args.length > 3 ? args[3] : "unspecified", System.getProperty("java.version"),
                ManagementFactory.getRuntimeMXBean().getInputArguments(), keys, fanout, probes);
        int[] probeKeys = new int[probes];
        Rnd rnd = new Rnd(130, 131);
        for (int i = 0; i < probes; i++) {
            probeKeys[i] = rnd.nextInt(keys * 10);
        }
        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        try (CairoEngine engine = new CairoEngine(configuration);
             PerQueryMemoryTrackerProvider trackers = new PerQueryMemoryTrackerProvider(configuration)) {
            engine.load();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
                @Override
                public boolean shouldLogSql() {
                    return false;
                }
            }.with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)) {
                String ddl = "create table build_input as (select ((x-1)/" + fanout + ")::int k, "
                        + "(case when x%2=0 then 'ES' else 'IT' end)::symbol country, "
                        + "(x%800)/8.0 capacity from long_sequence(" + (long) keys * fanout + "))";
                System.out.println("# generator=" + ddl);
                engine.execute(ddl, context);
                IntList payloadColumns = new IntList();
                payloadColumns.add(1);
                payloadColumns.add(2);
                ArrayColumnTypes payloadTypes = new ArrayColumnTypes().add(ColumnType.SYMBOL).add(ColumnType.DOUBLE);
                try (RecordCursorFactory source = engine.select("build_input", context);
                     IntHashJoinBuild copied = new IntHashJoinBuild(payloadTypes, payloadColumns, 16, 4096);
                     Map light = MapFactory.createUnorderedMap(configuration, new ArrayColumnTypes().add(ColumnType.INT),
                             new ArrayColumnTypes().add(ColumnType.INT).add(ColumnType.INT), false, false);
                     LongChain chain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages(), true)) {
                    Sample expected = null;
                    System.out.println("arm,repetition,run,build_ns,probe_ns,build_bytes,matched_pairs,checksum");
                    for (int repetition = 0; repetition < 2; repetition++) {
                        long[][] buildNanos = new long[2][10];
                        long[][] probeNanos = new long[2][10];
                        for (int run = -3; run < 10; run++) {
                            for (int turn = 0; turn < 2; turn++) {
                                int arm = ((run + 3 + repetition) & 1) == 0 ? turn : 1 - turn;
                                Sample sample;
                                try (MemoryTracker tracker = trackers.acquire(AllowAllSecurityContext.INSTANCE, 130, MemoryTrackerWorkload.QUERY)) {
                                    sample = arm == 0
                                            ? light(source, context, light, chain, tracker, probeKeys)
                                            : copied(source, context, copied, tracker, probeKeys);
                                    if (tracker.getUsed() != 0) {
                                        throw new IllegalStateException("storage leaked query memory");
                                    }
                                }
                                if (expected == null) {
                                    expected = sample;
                                } else if (expected.matches != sample.matches || expected.checksum != sample.checksum) {
                                    throw new IllegalStateException("storage results differ: " + expected + " vs " + sample);
                                }
                                if (run >= 0) {
                                    buildNanos[arm][run] = sample.buildNanos;
                                    probeNanos[arm][run] = sample.probeNanos;
                                    System.out.printf(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%.2f%n", arm == 0 ? "row-id" : "copied",
                                            repetition, run, sample.buildNanos, sample.probeNanos, sample.bytes, sample.matches, sample.checksum);
                                }
                            }
                        }
                        for (int arm = 0; arm < 2; arm++) {
                            Arrays.sort(buildNanos[arm]);
                            Arrays.sort(probeNanos[arm]);
                            System.out.printf(Locale.ROOT, "# summary arm=%s repetition=%d build_median_ns=%d build_min_ns=%d build_max_ns=%d probe_median_ns=%d probe_min_ns=%d probe_max_ns=%d%n",
                                    arm == 0 ? "row-id" : "copied", repetition, median(buildNanos[arm]), buildNanos[arm][0], buildNanos[arm][9],
                                    median(probeNanos[arm]), probeNanos[arm][0], probeNanos[arm][9]);
                        }
                    }
                }
            }
        }
    }

    private static Sample copied(RecordCursorFactory source, SqlExecutionContextImpl context, IntHashJoinBuild build, MemoryTracker tracker, int[] keys) throws Exception {
        try {
            long buildNanos;
            FrozenHashJoinBuild frozen;
            try (RecordCursor cursor = source.getCursor(context)) {
                long start = System.nanoTime();
                build.open(tracker, NOOP);
                frozen = build.build(cursor, 0);
                buildNanos = System.nanoTime() - start;
            }
            long bytes = tracker.getUsed();
            if (bytes != frozen.getSizeInBytes()) {
                throw new IllegalStateException("copied storage accounting differs");
            }
            FrozenHashJoinBuild.Probe probe = frozen.newProbe(NOOP);
            Record record = probe.getRecord();
            long matches = 0;
            double checksum = 0;
            long start = System.nanoTime();
            for (int key : keys) {
                probe.find(key);
                while (probe.hasNext()) {
                    probe.next();
                    matches++;
                    checksum += record.getDouble(1) + record.getSymA(0).charAt(0);
                }
            }
            return new Sample(buildNanos, System.nanoTime() - start, bytes, matches, checksum);
        } finally {
            build.close();
        }
    }

    private static Sample light(RecordCursorFactory source, SqlExecutionContextImpl context, Map map, LongChain chain, MemoryTracker tracker, int[] keys) throws Exception {
        try (RecordCursor cursor = source.getCursor(context)) {
            long start = System.nanoTime();
            map.setMemoryTracker(tracker);
            chain.setMemoryTracker(tracker);
            map.reopen();
            chain.reopen();
            Record sourceRecord = cursor.getRecord();
            while (cursor.hasNext()) {
                NOOP.statefulThrowExceptionIfTripped();
                MapKey key = map.withKey();
                key.putInt(sourceRecord.getInt(0));
                MapValue value = key.createValue();
                if (value.isNew()) {
                    value.putInt(0, chain.put(sourceRecord.getRowId(), -1));
                    value.putInt(1, 1);
                } else {
                    value.putInt(0, chain.put(sourceRecord.getRowId(), value.getInt(0)));
                    value.addInt(1, 1);
                }
            }
            long buildNanos = System.nanoTime() - start;
            long bytes = tracker.getUsed();
            Record record = cursor.getRecordB();
            long matches = 0;
            double checksum = 0;
            start = System.nanoTime();
            for (int k : keys) {
                NOOP.statefulThrowExceptionIfTripped();
                MapKey key = map.withKey();
                key.putInt(k);
                MapValue value = key.findValue();
                if (value != null) {
                    LongChain.Cursor duplicates = chain.getCursor(value.getInt(0));
                    while (duplicates.hasNext()) {
                        NOOP.statefulThrowExceptionIfTripped();
                        cursor.recordAt(record, duplicates.next());
                        matches++;
                        checksum += record.getDouble(2) + record.getSymA(1).charAt(0);
                    }
                }
            }
            return new Sample(buildNanos, System.nanoTime() - start, bytes, matches, checksum);
        } finally {
            map.close();
            chain.close();
        }
    }

    private static long median(long[] values) {
        return (values[4] + values[5]) / 2;
    }

    private record Sample(long buildNanos, long probeNanos, long bytes, long matches, double checksum) {
    }
}
