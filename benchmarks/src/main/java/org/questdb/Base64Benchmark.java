package org.questdb;

import io.questdb.std.Chars;
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

    @Param({"16", "512", "65536"})
    public int size;
    private ByteBuffer bb;
    private String data;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Base64Benchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(3)
                .forks(1)
                .addProfiler("gc")
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public byte[] jdkBaseline() {
        return Base64.getDecoder().decode(data);
    }

    @Setup
    public void prepareData() {
        Random random = new Random();

        byte[] b = new byte[size];
        random.nextBytes(b);
        data = Base64.getEncoder().encodeToString(b);
        bb = ByteBuffer.allocate(size);
    }

    @Benchmark
    public ByteBuffer questDB() {
        bb.clear();
        Chars.base64Decode(data, bb);
        return bb;
    }

}
