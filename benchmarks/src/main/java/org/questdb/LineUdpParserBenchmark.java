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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpLexer;
import io.questdb.cutlass.line.udp.LineUdpParserImpl;
import io.questdb.griffin.SqlException;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Parses ILP over UDP datagrams as a UDP receiver does, through the lexer and the parser. Both
 * benchmarks report the time per line.
 * <ul>
 * <li>existingTable: datagrams of 200 lines, three tags and five fields each, into a table that
 * exists, committed after every datagram: the steady state of UDP ingestion.</li>
 * <li>newTableDropped: a line that would create a table but ends in an empty value, so the parser
 * drops it and every copy takes the table-creating path again.</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class LineUdpParserBenchmark {
    private static final int LINES = 200;

    public static void main(String[] args) throws RunnerException {
        new Runner(new OptionsBuilder().include(LineUdpParserBenchmark.class.getSimpleName()).build()).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    @OperationsPerInvocation(LINES)
    public void existingTable(ExistingTable state) {
        state.parse();
        state.parser.commitAll();
    }

    @Benchmark
    public void newTableDropped(NewTableDropped state) {
        state.parse();
    }

    private static void deleteRecursively(Path root) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    @State(Scope.Thread)
    public static class ExistingTable extends UdpState {

        @Setup(Level.Trial)
        public void setUp() throws IOException, SqlException {
            final StringBuilder lines = new StringBuilder();
            for (int i = 0; i < LINES; i++) {
                lines.append("x,host=h").append(i % 10).append(",region=r").append(i % 3).append(",dc=d").append(i % 2)
                        .append(" cpu=").append(i % 100).append(".5,mem=").append(i % 50).append(".25,load=0.").append(i % 9)
                        .append(",procs=").append(i).append("i,msg=\"ok ").append(i % 5).append("\"\n");
            }
            open(lines);
            engine.execute(
                    "CREATE TABLE x (host SYMBOL, region SYMBOL, dc SYMBOL, cpu DOUBLE, mem DOUBLE, load DOUBLE, procs LONG," +
                            " msg VARCHAR, timestamp TIMESTAMP) TIMESTAMP(timestamp) PARTITION BY DAY BYPASS WAL"
            );
        }

        @Setup(Level.Iteration)
        public void setUpIteration() {
            openParser();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            close();
        }

        // the parser holds the table's writer; the table starts each iteration empty
        @TearDown(Level.Iteration)
        public void tearDownIteration() throws SqlException {
            closeParser();
            engine.execute("TRUNCATE TABLE x");
        }
    }

    @State(Scope.Thread)
    public static class NewTableDropped extends UdpState {
        @Param({"50", "300"})
        public int names;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            final StringBuilder line = new StringBuilder("nt ");
            for (int i = 0; i < names; i++) {
                line.append('c').append(i).append(i == names - 1 ? "=" : "=1,");
            }
            open(line.append('\n'));
            openParser();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            closeParser();
            close();
        }
    }

    public abstract static class UdpState {
        CairoEngine engine;
        LineUdpLexer lexer;
        LineUdpParserImpl parser;
        private long datagram;
        private int datagramSize;
        private Path root;

        void close() throws IOException {
            Unsafe.free(datagram, datagramSize, MemoryTag.NATIVE_DEFAULT);
            engine = Misc.free(engine);
            deleteRecursively(root);
        }

        void closeParser() {
            lexer = Misc.free(lexer);
            parser = Misc.free(parser);
        }

        void open(CharSequence text) throws IOException {
            root = Files.createTempDirectory("udp-parser-bench-");
            engine = new CairoEngine(new DefaultCairoConfiguration(root.toString()));
            final byte[] bytes = text.toString().getBytes(StandardCharsets.UTF_8);
            datagramSize = bytes.length;
            datagram = Unsafe.malloc(datagramSize, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < datagramSize; i++) {
                Unsafe.putByte(datagram + i, bytes[i]);
            }
        }

        void openParser() {
            parser = new LineUdpParserImpl(engine, new DefaultLineUdpReceiverConfiguration());
            lexer = new LineUdpLexer(64 * 1024);
            lexer.withParser(parser);
        }

        void parse() {
            lexer.parse(datagram, datagram + datagramSize);
            lexer.parseLast();
        }
    }
}
