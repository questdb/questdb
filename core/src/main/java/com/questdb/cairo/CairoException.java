package com.questdb.cairo;

import com.questdb.std.Sinkable;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

public class CairoException extends RuntimeException implements Sinkable {
    private static ThreadLocal<CairoException> tlException = new ThreadLocal<>(CairoException::new);
    private final StringSink message = new StringSink();
    private int errno;

    public static CairoException instance(int errno) {
        CairoException ex = tlException.get();
        ex.message.clear();
        ex.errno = errno;
        return ex;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    public CairoException put(long value) {
        message.put(value);
        return this;
    }

    public CairoException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public CairoException put(char c) {
        message.put(c);
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put(errno).put(": ").put(message);
    }
}
