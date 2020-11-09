package io.questdb.griffin;

import java.io.Closeable;

import io.questdb.cairo.CairoException;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Unsafe;

public class HttpSqlExecutionInterruptor implements SqlExecutionInterruptor, Closeable {
    private final NetworkFacade nf;
    private final int nIterationsPerCheck;
    private final int bufferSize;
    private long buffer;
    private int nIterationsSinceCheck;
    private long fd = -1;

    public static HttpSqlExecutionInterruptor create(SqlInterruptorConfiguration configuration) {
        if(configuration.isEnabled()) {
            return new HttpSqlExecutionInterruptor(configuration);
        }
        return null;
    }

    public HttpSqlExecutionInterruptor(SqlInterruptorConfiguration configuration) {
        this.nf = configuration.getNetworkFacade();
        this.nIterationsPerCheck = configuration.getCountOfIterationsPerCheck();
        this.bufferSize = configuration.getBufferSize();
        buffer = Unsafe.malloc(bufferSize);
    }

    @Override
    public void checkInterrupted() {
        assert fd != -1;
        if (nIterationsSinceCheck == nIterationsPerCheck) {
            nIterationsSinceCheck = 0;
            checkConnection();
        } else {
            nIterationsSinceCheck++;
        }
    }

    private void checkConnection() {
        int nRead = nf.peek(fd, buffer, bufferSize);
        if (nRead == 0) {
            return;
        }
        if (nRead < 0) {
            throw CairoException.instance(0).put("client fd ").put(fd).put(" is closed").setInterruption(true);
        }

        int index = 0;
        long ptr = buffer;
        while (index < nRead) {
            byte b = Unsafe.getUnsafe().getByte(ptr + index);
            if (b != (byte) '\r' && b != (byte) '\n') {
                break;
            }
            index++;
        }

        if (index > 0) {
            nf.recv(fd, buffer, index);
        }
    }

    public HttpSqlExecutionInterruptor of(long fd) {
        assert buffer != 0;
        nIterationsSinceCheck = 0;
        this.fd = fd;
        return this;
    }

    @Override
    public void close() {
        Unsafe.free(buffer, bufferSize);
        buffer = 0;
        fd = -1;
    }
}
