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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.SymbolColumnIndexer;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.PostingIndexBuildValidator;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/**
 * Compares unconditional replay builds with validation and reuse, including incomplete-file fallback.
 * Use an ERROR-only logging configuration via -Dout to exclude console IO.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {"--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED"})
public class PostingIndexResumeBenchmark {
    @Param({"true", "false"})
    public boolean isComplete;
    @Param({"true", "false"})
    public boolean isCovering;
    @Param({"false", "true"})
    public boolean isReuseEnabled;
    @Param({"1000", "1000000"})
    public int rowCount;
    private final ColumnVersionReader columnVersions = new ColumnVersionReader();
    private final IntList coverColumns = new IntList();
    private final ObjList<CharSequence> coverNames = new ObjList<>();
    private final IntList coverShifts = new IntList();
    private final LongList coverTops = new LongList();
    private final LongList coverTxns = new LongList();
    private final IntList coverTypes = new IntList();
    private final MemoryCMARW keyMem = Vm.getCMARWInstance();
    private DefaultCairoConfiguration configuration;
    private CairoEngine engine;
    private FilesFacade ff;
    private Path path;
    private int plen;
    private String root;
    private SqlExecutionContextImpl sqlContext;
    private long symbolFd = -1;

    @Benchmark
    public boolean resume() {
        if (isReuseEnabled && isReusable()) {
            return true;
        }
        rebuild();
        return false;
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        root = Files.createTempDirectory("posting-resume-bench").toString();
        configuration = new DefaultCairoConfiguration(root);
        ff = configuration.getFilesFacade();
        engine = new CairoEngine(configuration);
        sqlContext = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null);
        engine.execute("CREATE TABLE x AS (SELECT ('s' || (x % 1024))::SYMBOL s, x v FROM long_sequence(" + rowCount + "))", sqlContext);
        engine.releaseAllWriters();
        path = new Path().of(root).concat(engine.verifyTableName("x")).concat("default");
        plen = path.size();
        symbolFd = ff.openRO(TableUtils.dFile(path, "s", -1));
        if (symbolFd < 0) {
            throw new IllegalStateException("cannot open symbol data");
        }
        if (isCovering) {
            coverColumns.add(1);
            coverNames.add("v");
            coverShifts.add(3);
            coverTops.add(0);
            coverTxns.add(-1);
            coverTypes.add(ColumnType.LONG);
        }
        try (MemoryCMARW keyMem = Vm.getCMARWInstance()) {
            keyMem.smallFile(ff, PostingIndexUtils.keyFileName(path.trimTo(plen), "s", -1), MemoryTag.MMAP_INDEX_WRITER);
            PostingIndexWriter.initKeyMemory(keyMem);
        }
        path.trimTo(plen);
        rebuild();
        if (!isReusable()) {
            throw new IllegalStateException("benchmark fixture is not a complete index");
        }
    }

    @Setup(Level.Invocation)
    public void setUpInvocation() {
        if (!isComplete) {
            final long sealTxn = PostingIndexUtils.readSealTxnFromKeyFile(ff, PostingIndexUtils.keyFileName(path, "s", -1));
            if (!ff.removeQuiet(PostingIndexUtils.valueFileName(path.trimTo(plen), "s", -1, sealTxn))) {
                throw new IllegalStateException("cannot remove the value file");
            }
            path.trimTo(plen);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (symbolFd >= 0) {
            ff.close(symbolFd);
        }
        columnVersions.close();
        keyMem.close();
        sqlContext.close();
        engine.close();
        ff.rmdir(path.of(root));
        path.close();
    }

    private boolean isReusable() {
        return PostingIndexBuildValidator.isComplete(ff, path, "s", -1, 1, rowCount,
                coverColumns, columnVersions, 0);
    }

    private void rebuild() {
        // As ADD INDEX does for a partition it cannot reuse: start over from an empty chain.
        PostingIndexUtils.resetIndex(ff, path, plen, "s", -1, keyMem);
        try (SymbolColumnIndexer indexer = new SymbolColumnIndexer(configuration, IndexType.POSTING)) {
            indexer.configureWriter(path, "s", -1, 0, 0, -1);
            if (isCovering) {
                indexer.configureCovering(coverNames, coverTxns, coverTops, coverShifts, coverColumns, coverTypes, -1);
            }
            indexer.getWriter().setNextTxnAtSeal(1);
            indexer.index(ff, symbolFd, 0, rowCount);
            indexer.seal();
        }
    }
}
