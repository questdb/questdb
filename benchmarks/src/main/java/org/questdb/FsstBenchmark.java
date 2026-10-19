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

import io.questdb.cairo.idx.FSSTNative;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 1)
@Fork(1)
public class FsstBenchmark {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FsstBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long decompressBlock(StrideState s) {
        return FSSTNative.decompressBlock(
                s.decoderAddr,
                s.compAddr, s.compOffsetsAddr, Long.BYTES, s.valueCount,
                s.dstAddr, s.dstCapacity,
                s.dstOffsetsAddr
        );
    }

    @Benchmark
    public long lookupOneValueWarm(StrideState s) {
        int idx = s.nextIdx();
        long lo = Unsafe.getLong(s.dstOffsetsAddr + (long) idx * Long.BYTES);
        long hi = Unsafe.getLong(s.dstOffsetsAddr + (long) (idx + 1) * Long.BYTES);
        return s.dstAddr + lo + (hi - lo);
    }

    @Benchmark
    public long scanStride(StrideState s) {
        long decoded = FSSTNative.decompressBlock(
                s.decoderAddr,
                s.compAddr, s.compOffsetsAddr, Long.BYTES, s.valueCount,
                s.dstAddr, s.dstCapacity,
                s.dstOffsetsAddr
        );
        if (decoded < 0) return -1;
        long sum = 0;
        for (int i = 0; i < s.valueCount; i++) {
            long lo = Unsafe.getLong(s.dstOffsetsAddr + (long) i * Long.BYTES);
            long hi = Unsafe.getLong(s.dstOffsetsAddr + (long) (i + 1) * Long.BYTES);
            sum += s.dstAddr + lo + (hi - lo);
        }
        return sum;
    }

    @Benchmark
    public long trainAndCompressBlock(StrideState s) {
        return FSSTNative.trainAndCompressBlock(
                s.rawAddr, s.rawOffsetsAddr, s.valueCount,
                s.cmpScratchAddr, s.cmpScratchCap, s.cmpScratchOffsetsAddr,
                s.tableBufAddr, s.batchScratchAddr
        );
    }

    @State(Scope.Thread)
    public static class StrideState {

        private static final byte[] CORPUS = (
                "GET /api/v1/users/12345 HTTP/1.1 200 OK " +
                        "GET /api/v1/orders/67890 HTTP/1.1 404 NotFound " +
                        "POST /api/v1/sessions HTTP/1.1 201 Created " +
                        "{\"user_id\":42,\"role\":\"admin\",\"status\":\"active\"} " +
                        "{\"user_id\":17,\"role\":\"viewer\",\"status\":\"pending\"} " +
                        "https://questdb.io/docs/reference/sql/select/?lang=en " +
                        "https://example.com/path/to/resource?id=123&type=user " +
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0 " +
                        "trace_id=abcdef01-2345-6789-abcd-ef0123456789 span_id=001122334455 " +
                        "the quick brown fox jumps over the lazy dog "
        ).getBytes(StandardCharsets.UTF_8);

        @Param({"20"})
        int avgLen;
        long batchScratchAddr;
        long batchScratchBytes;
        long cmpScratchAddr;
        long cmpScratchCap;
        long cmpScratchOffsetsAddr;
        long compAddr;
        long compCap;
        long compOffsetsAddr;
        long decoderAddr;
        long dstAddr;
        long dstCapacity;
        long dstOffsetsAddr;
        int idx;
        long offsetsArrayBytes;
        long rawAddr;
        long rawOffsetsAddr;
        long rawTotalLen;
        long tableBufAddr;
        @Param({"128", "1024"})
        int valueCount;

        @Setup(Level.Trial)
        public void setUp() {
            Os.init();

            Random rnd = new Random(0xC0DEC0DEL);
            int minLen = Math.max(1, avgLen - 10);
            int maxLen = avgLen + 10;
            offsetsArrayBytes = (long) (valueCount + 1) * Long.BYTES;

            int[] starts = new int[valueCount];
            int[] lens = new int[valueCount];
            long total = 0;
            for (int i = 0; i < valueCount; i++) {
                int len = minLen + rnd.nextInt(maxLen - minLen + 1);
                int start = rnd.nextInt(CORPUS.length - maxLen);
                starts[i] = start;
                lens[i] = len;
                total += len;
            }
            rawTotalLen = total;
            rawAddr = Unsafe.malloc(rawTotalLen, MemoryTag.NATIVE_DEFAULT);
            rawOffsetsAddr = Unsafe.malloc(offsetsArrayBytes, MemoryTag.NATIVE_DEFAULT);
            long pos = 0;
            for (int i = 0; i < valueCount; i++) {
                Unsafe.putLong(rawOffsetsAddr + (long) i * Long.BYTES, pos);
                long dst = rawAddr + pos;
                int start = starts[i];
                int len = lens[i];
                for (int j = 0; j < len; j++) {
                    Unsafe.putByte(dst + j, CORPUS[start + j]);
                }
                pos += len;
            }
            Unsafe.putLong(rawOffsetsAddr + (long) valueCount * Long.BYTES, pos);

            compCap = rawTotalLen * 2 + 16;
            compAddr = Unsafe.malloc(compCap, MemoryTag.NATIVE_DEFAULT);
            compOffsetsAddr = Unsafe.malloc(offsetsArrayBytes, MemoryTag.NATIVE_DEFAULT);
            tableBufAddr = Unsafe.malloc(FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            batchScratchBytes = (long) valueCount * FSSTNative.BATCH_SCRATCH_BYTES_PER_VALUE;
            batchScratchAddr = Unsafe.malloc(batchScratchBytes, MemoryTag.NATIVE_DEFAULT);
            long packed = FSSTNative.trainAndCompressBlock(
                    rawAddr, rawOffsetsAddr, valueCount,
                    compAddr, compCap, compOffsetsAddr,
                    tableBufAddr, batchScratchAddr);
            if (packed < 0) {
                throw new IllegalStateException("FSST trainAndCompress failed");
            }
            decoderAddr = Unsafe.malloc(FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
            if (FSSTNative.importTable(decoderAddr, tableBufAddr) < 0) {
                throw new IllegalStateException("FSST table import failed");
            }

            dstCapacity = rawTotalLen + 64;
            dstAddr = Unsafe.malloc(dstCapacity, MemoryTag.NATIVE_DEFAULT);
            dstOffsetsAddr = Unsafe.malloc(offsetsArrayBytes, MemoryTag.NATIVE_DEFAULT);

            cmpScratchCap = compCap;
            cmpScratchAddr = Unsafe.malloc(cmpScratchCap, MemoryTag.NATIVE_DEFAULT);
            cmpScratchOffsetsAddr = Unsafe.malloc(offsetsArrayBytes, MemoryTag.NATIVE_DEFAULT);

            // Pre-warm dst + dstOffsets so cache-hit-only benches don't depend on bench execution order.
            long decoded = FSSTNative.decompressBlock(
                    decoderAddr,
                    compAddr, compOffsetsAddr, Long.BYTES, valueCount,
                    dstAddr, dstCapacity,
                    dstOffsetsAddr
            );
            if (decoded < 0) {
                throw new IllegalStateException("FSST cache pre-warm failed");
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            if (decoderAddr != 0) {
                Unsafe.free(decoderAddr, FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
                decoderAddr = 0;
            }
            free(batchScratchAddr, batchScratchBytes);
            batchScratchAddr = 0;
            free(tableBufAddr, FSSTNative.MAX_HEADER_SIZE);
            tableBufAddr = 0;
            free(cmpScratchOffsetsAddr, offsetsArrayBytes);
            cmpScratchOffsetsAddr = 0;
            free(cmpScratchAddr, cmpScratchCap);
            cmpScratchAddr = 0;
            free(dstOffsetsAddr, offsetsArrayBytes);
            dstOffsetsAddr = 0;
            free(dstAddr, dstCapacity);
            dstAddr = 0;
            free(compOffsetsAddr, offsetsArrayBytes);
            compOffsetsAddr = 0;
            free(compAddr, compCap);
            compAddr = 0;
            free(rawOffsetsAddr, offsetsArrayBytes);
            rawOffsetsAddr = 0;
            free(rawAddr, rawTotalLen);
            rawAddr = 0;
        }

        int nextIdx() {
            int i = idx;
            idx = (i + 1 == valueCount) ? 0 : i + 1;
            return i;
        }

        private void free(long addr, long size) {
            if (addr != 0) {
                Unsafe.free(addr, size, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
