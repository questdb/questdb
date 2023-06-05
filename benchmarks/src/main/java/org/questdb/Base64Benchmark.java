package org.questdb;

import io.questdb.std.Encoding;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Threads(1)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Base64Benchmark {

    private ByteBuffer bb = ByteBuffer.allocateDirect(64 * 1024);
    private String dataLarge;
    private String dataMedium;
    private String dataSmall;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Base64Benchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public byte[] jdkBaseline() {
        return Base64.getDecoder().decode(dataLarge);
    }

    @Setup
    public void prepareData() {
        Random random = new Random();

        byte[] b = new byte[16];
        random.nextBytes(b);
        dataSmall = Base64.getEncoder().encodeToString(b);

        b = new byte[512];
        random.nextBytes(b);
        dataMedium = Base64.getEncoder().encodeToString(b);

        b = new byte[64 * 1024];
        random.nextBytes(b);
        dataLarge = Base64.getEncoder().encodeToString(b);
    }

    @Benchmark
    public ByteBuffer questDB() {
        bb.clear();
        Encoding.base64Decode(dataLarge, bb);
        return bb;
    }

}
