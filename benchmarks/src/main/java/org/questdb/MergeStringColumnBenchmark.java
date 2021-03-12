/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.vm.ReadOnlyVirtualMemory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.*;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MergeStringColumnBenchmark {
    private static final Log LOG;

    private static final CairoConfiguration configuration;
    private final static long SMALL_MERGE_SIZE = 600;
    private final static long TABLE_SIZE = 10_000;

    private static CairoEngine engine;
    private static SqlCompiler compiler;
    private static SqlExecutionContextImpl sqlExecutionContext;
    private static Path tempPath;

    private static ColumnMerge smallStrMerge;
    private static ColumnMerge bigStrMerge;

    public static void main(String[] args) throws RunnerException, SqlException, IOException {

        Options opt = new OptionsBuilder()
                .include(MergeStringColumnBenchmark.class.getSimpleName())
                .warmupIterations(0)
                .measurementIterations(2)
                .forks(1)
                .build();

        new Runner(opt).run();
    }


    @Setup(Level.Trial)
    public void resetEngine() throws SqlException {
        SharedRandom.RANDOM.set(new Rnd());

        LOG.info().$("engine init").$();
        CairoEngine e = new CairoEngine(configuration);
        SqlCompiler c = new SqlCompiler(e);
        SqlExecutionContextImpl ec = new SqlExecutionContextImpl(
                e, 1, e.getMessageBus())
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        new BindVariableServiceImpl(configuration),
                        null,
                        -1,
                        null);
        engine = e;
        compiler = c;
        sqlExecutionContext = ec;

        compiler.compile(
                "create table x as (" +
                        "select" +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_str(100, 2048, 8192, 10) bigStr," +
                        " timestamp_sequence(500000000000L,100000000L) ts" +
                        " from long_sequence(" + TABLE_SIZE + ")" +
                        ") timestamp (ts)",
                sqlExecutionContext
        );

        compiler.compile(
                "create table append as (" +
                        "select" +
                        " rnd_str(4, 1, 128, 10) c," +
                        " rnd_str(100, 2048, 8192, 10) bigStr," +
                        " timestamp_sequence(518390000000L,100000L) ts" +
                        " from long_sequence(" + TABLE_SIZE + ")" +
                        ") timestamp (ts)",
                sqlExecutionContext
        );

        engine.releaseAllWriters();
        smallStrMerge = initializeBuffers("c");
        bigStrMerge = initializeBuffers("bigStr");
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        smallStrMerge.close();
        bigStrMerge.close();

        LOG.info().$("engine close").$();
        sqlExecutionContext.close();
        compiler.close();
        engine.close();

        try (io.questdb.std.str.Path path = io.questdb.std.str.Path.getThreadLocal(tempPath.toString())) {
            if (!io.questdb.std.Files.rmdir(path.$())) {
                LOG.info().$("temp folder delete failed [path=").$(path).$(",errno=").$(Os.errno()).$();
            }
        }
    }

//    @Benchmark
//    public void testBigInlAMemcpy() {
//        Vect.oooMergeCopyStrColumnInlAMemcpy(
//                bigStrMerge.mergeIndex,
//                bigStrMerge.mergeIndexSize,
//                bigStrMerge.addressesTbl1.fixedAddress,
//                bigStrMerge.addressesTbl1.varAddress,
//                bigStrMerge.addressesTbl2.fixedAddress,
//                bigStrMerge.addressesTbl2.varAddress,
//                bigStrMerge.destFixAddress,
//                bigStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testBigInlManMemcpy() {
//        Vect.oooMergeCopyStrColumnInlManMemcpy(
//                bigStrMerge.mergeIndex,
//                bigStrMerge.mergeIndexSize,
//                bigStrMerge.addressesTbl1.fixedAddress,
//                bigStrMerge.addressesTbl1.varAddress,
//                bigStrMerge.addressesTbl2.fixedAddress,
//                bigStrMerge.addressesTbl2.varAddress,
//                bigStrMerge.destFixAddress,
//                bigStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testBigInlMemcpy() {
//        Vect.oooMergeCopyStrColumnInlMemcpy(
//                bigStrMerge.mergeIndex,
//                bigStrMerge.mergeIndexSize,
//                bigStrMerge.addressesTbl1.fixedAddress,
//                bigStrMerge.addressesTbl1.varAddress,
//                bigStrMerge.addressesTbl2.fixedAddress,
//                bigStrMerge.addressesTbl2.varAddress,
//                bigStrMerge.destFixAddress,
//                bigStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testBigMvAMemcpy() {
//        Vect.oooMergeCopyStrColumnMvAMemcpy(
//                bigStrMerge.mergeIndex,
//                bigStrMerge.mergeIndexSize,
//                bigStrMerge.addressesTbl1.fixedAddress,
//                bigStrMerge.addressesTbl1.varAddress,
//                bigStrMerge.addressesTbl2.fixedAddress,
//                bigStrMerge.addressesTbl2.varAddress,
//                bigStrMerge.destFixAddress,
//                bigStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testBigMvManMemcpy() {
//        Vect.oooMergeCopyStrColumnMvManMemcpy(
//                bigStrMerge.mergeIndex,
//                bigStrMerge.mergeIndexSize,
//                bigStrMerge.addressesTbl1.fixedAddress,
//                bigStrMerge.addressesTbl1.varAddress,
//                bigStrMerge.addressesTbl2.fixedAddress,
//                bigStrMerge.addressesTbl2.varAddress,
//                bigStrMerge.destFixAddress,
//                bigStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testBigMvMemcpy() {
//        Vect.oooMergeCopyStrColumnMvMemcpy(
//                bigStrMerge.mergeIndex,
//                bigStrMerge.mergeIndexSize,
//                bigStrMerge.addressesTbl1.fixedAddress,
//                bigStrMerge.addressesTbl1.varAddress,
//                bigStrMerge.addressesTbl2.fixedAddress,
//                bigStrMerge.addressesTbl2.varAddress,
//                bigStrMerge.destFixAddress,
//                bigStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testInlAMemcpy() {
//        Vect.oooMergeCopyStrColumnInlAMemcpy(
//                smallStrMerge.mergeIndex,
//                SMALL_MERGE_SIZE,
//                smallStrMerge.addressesTbl1.fixedAddress,
//                smallStrMerge.addressesTbl1.varAddress,
//                smallStrMerge.addressesTbl2.fixedAddress,
//                smallStrMerge.addressesTbl2.varAddress,
//                smallStrMerge.destFixAddress,
//                smallStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testInlManMemcpy() {
//        Vect.oooMergeCopyStrColumnInlManMemcpy(
//                smallStrMerge.mergeIndex,
//                SMALL_MERGE_SIZE,
//                smallStrMerge.addressesTbl1.fixedAddress,
//                smallStrMerge.addressesTbl1.varAddress,
//                smallStrMerge.addressesTbl2.fixedAddress,
//                smallStrMerge.addressesTbl2.varAddress,
//                smallStrMerge.destFixAddress,
//                smallStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testInlMemcpy() {
//        Vect.oooMergeCopyStrColumnInlMemcpy(
//                smallStrMerge.mergeIndex,
//                SMALL_MERGE_SIZE,
//                smallStrMerge.addressesTbl1.fixedAddress,
//                smallStrMerge.addressesTbl1.varAddress,
//                smallStrMerge.addressesTbl2.fixedAddress,
//                smallStrMerge.addressesTbl2.varAddress,
//                smallStrMerge.destFixAddress,
//                smallStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testMvAMemcpy() {
//        Vect.oooMergeCopyStrColumnMvAMemcpy(
//                smallStrMerge.mergeIndex,
//                SMALL_MERGE_SIZE,
//                smallStrMerge.addressesTbl1.fixedAddress,
//                smallStrMerge.addressesTbl1.varAddress,
//                smallStrMerge.addressesTbl2.fixedAddress,
//                smallStrMerge.addressesTbl2.varAddress,
//                smallStrMerge.destFixAddress,
//                smallStrMerge.destVarAddress,
//                0);
//    }

//    @Benchmark
//    public void testMvManMemcpy() {
//        Vect.oooMergeCopyStrColumnMvManMemcpy(
//                smallStrMerge.mergeIndex,
//                SMALL_MERGE_SIZE,
//                smallStrMerge.addressesTbl1.fixedAddress,
//                smallStrMerge.addressesTbl1.varAddress,
//                smallStrMerge.addressesTbl2.fixedAddress,
//                smallStrMerge.addressesTbl2.varAddress,
//                smallStrMerge.destFixAddress,
//                smallStrMerge.destVarAddress,
//                0);
//    }
//
//    @Benchmark
//    public void testMvMemcpy() {
//        Vect.oooMergeCopyStrColumnMvMemcpy(
//                smallStrMerge.mergeIndex,
//                SMALL_MERGE_SIZE,
//                smallStrMerge.addressesTbl1.fixedAddress,
//                smallStrMerge.addressesTbl1.varAddress,
//                smallStrMerge.addressesTbl2.fixedAddress,
//                smallStrMerge.addressesTbl2.varAddress,
//                smallStrMerge.destFixAddress,
//                smallStrMerge.destVarAddress,
//                0);
//    }

//    @Benchmark
//    public void testSetMemoryLong() {
//        Vect.setMemoryLong(
//                bigStrMerge.destVarAddress,
//                -1L,
//                (bigStrMerge.addressesTbl1.varSizeBytes + bigStrMerge.addressesTbl2.varSizeBytes) / Long.BYTES
//        );
//    }
//
//    @Benchmark
//    public void testSetMemoryInt() {
//        Vect.setMemoryInt(
//                bigStrMerge.destVarAddress,
//                -1,
//                (bigStrMerge.addressesTbl1.varSizeBytes + bigStrMerge.addressesTbl2.varSizeBytes) / Integer.BYTES
//        );
//    }

    @Benchmark
    public void testSetMemoryShort() {
        Vect.setMemoryShort(
                bigStrMerge.destVarAddress,
                (short) -1,
                (bigStrMerge.addressesTbl1.varSizeBytes + bigStrMerge.addressesTbl2.varSizeBytes) / Short.BYTES
        );
    }

    @Benchmark
    public void testSetMemoryDouble() {
        Vect.setMemoryDouble(
                bigStrMerge.destVarAddress,
                -1.0,
                (bigStrMerge.addressesTbl1.varSizeBytes + bigStrMerge.addressesTbl2.varSizeBytes) / Double.BYTES
        );
    }
//
//    @Benchmark
//    public void testSetVarColumnRefs32Bit() {
//        Vect.setVarColumnRefs32Bit(
//                bigStrMerge.destVarAddress,
//                0,
//                (bigStrMerge.addressesTbl1.varSizeBytes + bigStrMerge.addressesTbl2.varSizeBytes) / 8
//        );
//    }
//
//    @Benchmark
//    public void testSetVarColumnRefs64Bit() {
//        Vect.setVarColumnRefs64Bit(
//                bigStrMerge.destVarAddress,
//                0,
//                (bigStrMerge.addressesTbl1.varSizeBytes + bigStrMerge.addressesTbl2.varSizeBytes) / 8
//        );
//    }

    private ColumnAddress getAddresses(TableReader readerTbl1, String columnName) {
        ColumnAddress columnAddress = new ColumnAddress();
        int columnIndex1 = readerTbl1.getMetadata().getColumnIndex(columnName);
        long partitionsize = readerTbl1.openPartition(0);
        ReadOnlyVirtualMemory fixed1 = readerTbl1.getColumn(TableReader.getPrimaryColumnIndex(readerTbl1.getColumnBase(0), columnIndex1) + 1);
        columnAddress.fixedAddress = fixed1.getPageAddress(0);
        columnAddress.fixedSizeLongs = partitionsize;

        // Figure out len of variable column
        ReadOnlyVirtualMemory variable1 = readerTbl1.getColumn(TableReader.getPrimaryColumnIndex(readerTbl1.getColumnBase(0), columnIndex1));
        columnAddress.varAddress = variable1.getPageAddress(0);
        columnAddress.varSizeBytes = fixed1.getLong((columnAddress.fixedSizeLongs - 1) * Long.BYTES);
        int lastStrSize = variable1.getInt(columnAddress.varSizeBytes);
        if (lastStrSize > 0) {
            columnAddress.varSizeBytes += variable1.getInt(columnAddress.varSizeBytes) * 2 + Integer.BYTES;
        }
        columnAddress.varSizeBytes += Integer.BYTES;
        return columnAddress;
    }

    private ColumnMerge initializeBuffers(String column) {
        ColumnMerge result = new ColumnMerge();

        result.readerTbl1 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x");
        result.readerTbl2 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "append");

        result.addressesTbl1 = getAddresses(result.readerTbl1, column);
        result.addressesTbl2 = getAddresses(result.readerTbl2, column);

        result.mergeIndexSize = result.addressesTbl1.fixedSizeLongs + result.addressesTbl2.fixedSizeLongs;
        result.mergeIndex = Unsafe.malloc(result.mergeIndexSize * Long.BYTES * 2);

        long mergeWriteAddress = result.mergeIndex + Long.BYTES;
        for (int i = 0, j = 0; i < result.addressesTbl1.fixedSizeLongs || j < result.addressesTbl2.fixedSizeLongs; ) {
            if ((i < result.addressesTbl1.fixedSizeLongs && i <= j) || j >= result.addressesTbl2.fixedSizeLongs) {
                // Set merge to take row from table 1
                Unsafe.getUnsafe().putLong(mergeWriteAddress, i | Long.MIN_VALUE);
                i++;
            } else {
                // Set merge to take row from table 2
                Unsafe.getUnsafe().putLong(mergeWriteAddress, j);
                j++;
            }
            mergeWriteAddress += 2 * Long.BYTES;
        }

        result.destFixAddress = Unsafe.malloc((result.addressesTbl1.fixedSizeLongs + result.addressesTbl2.fixedSizeLongs) * Long.BYTES);
        result.destVarAddress = Unsafe.malloc(result.addressesTbl1.varSizeBytes + result.addressesTbl2.varSizeBytes);

        return result;
    }

    static class ColumnAddress {
        long fixedAddress;
        long fixedSizeLongs;
        long varAddress;
        long varSizeBytes;
    }

    static class ColumnMerge {
        public TableReader readerTbl1;
        public TableReader readerTbl2;
        public long destFixAddress;
        public long destVarAddress;
        ColumnAddress addressesTbl1;
        ColumnAddress addressesTbl2;
        long mergeIndex;
        long mergeIndexSize;

        public void close() {
            readerTbl1.close();
            readerTbl2.close();
            Unsafe.free(mergeIndex, mergeIndexSize * Long.BYTES * 2);
            Unsafe.free(destFixAddress, (addressesTbl1.fixedSizeLongs + addressesTbl2.fixedSizeLongs) * Long.BYTES);
            Unsafe.free(destVarAddress, addressesTbl1.varSizeBytes + addressesTbl2.varSizeBytes);
        }
    }

    static {
        try {
            tempPath = Files.createTempDirectory("jmh-cpp");
        } catch (IOException e) {
            e.printStackTrace();
        }

        configuration = new DefaultCairoConfiguration(tempPath.toString());

        LogFactory.INSTANCE.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO, LogConsoleWriter::new));
        LogFactory.INSTANCE.bind();
        LogFactory.INSTANCE.startThread();
        LOG = LogFactory.getLog(MergeStringColumnBenchmark.class);
    }
}
