package io.questdb.griffin;

import java.io.Closeable;

import io.questdb.cairo.CairoException;
import io.questdb.network.NetworkFacade;
import io.questdb.std.Unsafe;

public class HttpSqlExecutionInterruptor implements SqlExecutionInterruptor, Closeable {
    private final NetworkFacade nf;
    private final int nIterrationsPerCheck;
    private final int bufferSize;
    private long buffer;
    private int nIterrationsSinceCheck;
    private long fd;

    public HttpSqlExecutionInterruptor(NetworkFacade nf, int nIterrationsPerCheck, int bufferSize) {
        super();
        this.nf = nf;
        this.nIterrationsPerCheck = nIterrationsPerCheck;
        this.bufferSize = bufferSize;
        buffer = Unsafe.malloc(bufferSize);
    }

    @Override
    public void checkInterrupted() {
        if (nIterrationsSinceCheck == nIterrationsPerCheck) {
            nIterrationsSinceCheck = 0;
            int nRead = nf.peek(fd, buffer, bufferSize);
            if (nRead == 0) {
                return;
            }
            if (nRead < 0) {
                throw CairoException.instance(0).put("Interrupting SQL processings, client fd ").put(fd).put(" is closed");
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
        } else {
            nIterrationsSinceCheck++;
        }
    }

    public HttpSqlExecutionInterruptor of(long fd) {
        nIterrationsSinceCheck = 0;
        this.fd = fd;
        return this;
    }

    @Override
    public void close() {
        Unsafe.free(buffer, bufferSize);
        buffer = 0;
    }
}
