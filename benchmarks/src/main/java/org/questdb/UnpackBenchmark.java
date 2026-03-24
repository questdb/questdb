package org.questdb;

import io.questdb.cairo.idx.BitpackUtils;
import io.questdb.cairo.idx.PostingIndexNative;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;

/**
 * Micro-benchmark for BitpackUtils.unpackValuesFrom — measures decode throughput
 * across bitwidths and batch sizes. Native AVX2 dispatch kicks in automatically
 * for naturally byte-aligned widths (8, 16, 32).
 *
 * <pre>
 * java -Xmx4g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.UnpackBenchmark
 * </pre>
 */
public class UnpackBenchmark {

    private static final int WARMUP_ITERS = 3_000;
    private static final int MEASURE_ITERS = 20_000;
    private static final int TOTAL_VALUES = 65_536;
    private static final int[] START_INDICES = {0, 64, 128, 256, 512, 1024};

    public static void main(String[] args) {
        @SuppressWarnings("unused")
        int osType = Os.type;

        boolean nativeAvail = PostingIndexNative.isNativeAvailable();
        System.out.printf("Native AVX2 available: %s%n", nativeAvail);
        System.out.printf("Warmup: %,d iters, measure: %,d iters, averaged over %d start indices%n%n",
                WARMUP_ITERS, MEASURE_ITERS, START_INDICES.length);

        runRawThroughput(64);
        runRawThroughput(256);
        runBatchScaling();
    }

    private static void runRawThroughput(int batchSize) {
        System.out.println("══════════════════════════════════════════════════════════════════");
        System.out.printf("  Raw decode throughput (batch=%d)%n", batchSize);
        System.out.println("══════════════════════════════════════════════════════════════════");
        System.out.printf("%-10s  %10s  %10s  %s%n", "bitWidth", "ns/call", "Mvals/s", "path");
        System.out.println("─".repeat(52));

        int[] bitWidths = {1, 4, 7, 8, 11, 12, 15, 16, 20, 24, 31, 32};

        for (int bw : bitWidths) {
            double nsPerCall = measureUnpack(bw, batchSize);
            double mvalsPerSec = batchSize / nsPerCall * 1e3;
            String path = isAligned(bw) && PostingIndexNative.isNativeAvailable() ? "AVX2" : "scalar";
            System.out.printf("%6d      %,8.0f    %,8.1f    %s%n", bw, nsPerCall, mvalsPerSec, path);
        }
        System.out.println();
    }

    private static void runBatchScaling() {
        System.out.println("══════════════════════════════════════════════════════════════════");
        System.out.println("  Batch scaling: throughput vs batch size");
        System.out.println("══════════════════════════════════════════════════════════════════");

        int[] batchSizes = {32, 64, 128, 256, 512, 1024, 2048, 4096};
        int[] bitWidths = {12, 16, 20, 32};

        for (int bw : bitWidths) {
            String path = isAligned(bw) && PostingIndexNative.isNativeAvailable() ? "AVX2" : "scalar";
            System.out.printf("%n  %d-bit (%s)%n", bw, path);
            System.out.printf("  %-10s  %10s  %10s%n", "batch", "ns/call", "Mvals/s");
            System.out.printf("  %s%n", "─".repeat(36));

            for (int batch : batchSizes) {
                if (batch > TOTAL_VALUES / 2) continue;
                double ns = measureUnpack(bw, batch);
                double mvalsPerSec = batch / ns * 1e3;
                System.out.printf("  %6d      %,8.0f    %,8.1f%n", batch, ns, mvalsPerSec);
            }
        }
        System.out.println();
    }

    private static double measureUnpack(int bitWidth, int batchSize) {
        long minValue = 1_000_000L;
        long maxOffset = bitWidth == 64 ? Long.MAX_VALUE : (1L << bitWidth) - 1;

        int packedSize = BitpackUtils.packedDataSize(TOTAL_VALUES, bitWidth);
        long packedAddr = Unsafe.malloc(packedSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(packedAddr, packedSize, (byte) 0);

        long valuesSize = (long) TOTAL_VALUES * Long.BYTES;
        long valuesAddr = Unsafe.malloc(valuesSize, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < TOTAL_VALUES; i++) {
            Unsafe.getUnsafe().putLong(valuesAddr + (long) i * Long.BYTES,
                    minValue + (i % (maxOffset + 1)));
        }
        BitpackUtils.packValues(valuesAddr, TOTAL_VALUES, minValue, bitWidth, packedAddr);

        long destSize = (long) batchSize * Long.BYTES;
        long destAddr = Unsafe.malloc(destSize, MemoryTag.NATIVE_DEFAULT);
        long totalElapsed = 0;
        int totalMeasurements = 0;

        for (int startIndex : START_INDICES) {
            if (startIndex + batchSize > TOTAL_VALUES) continue;

            for (int i = 0; i < WARMUP_ITERS; i++) {
                BitpackUtils.unpackValuesFrom(packedAddr, startIndex, batchSize, bitWidth, minValue, destAddr);
            }

            long t0 = System.nanoTime();
            for (int i = 0; i < MEASURE_ITERS; i++) {
                BitpackUtils.unpackValuesFrom(packedAddr, startIndex, batchSize, bitWidth, minValue, destAddr);
            }
            totalElapsed += System.nanoTime() - t0;
            totalMeasurements++;

            if (startIndex == START_INDICES[0]) {
                for (int i = 0; i < Math.min(4, batchSize); i++) {
                    long expected = minValue + ((startIndex + i) % (maxOffset + 1));
                    long actual = Unsafe.getUnsafe().getLong(destAddr + (long) i * Long.BYTES);
                    if (actual != expected) {
                        System.err.printf("MISMATCH at bw=%d start=%d i=%d: expected=%d got=%d%n",
                                bitWidth, startIndex, i, expected, actual);
                        break;
                    }
                }
            }
        }

        Unsafe.free(destAddr, destSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(valuesAddr, valuesSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(packedAddr, packedSize, MemoryTag.NATIVE_DEFAULT);
        return (double) totalElapsed / (totalMeasurements * MEASURE_ITERS);
    }

    private static boolean isAligned(int bitWidth) {
        return bitWidth == 8 || bitWidth == 16 || bitWidth == 32;
    }
}
