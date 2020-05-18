package io.questdb.cairo.security;

import io.questdb.std.Sinkable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class CairoSecurityException extends RuntimeException implements Sinkable {
    private static final ThreadLocal<CairoSecurityException> tlException = new ThreadLocal<>(CairoSecurityException::new);
    protected final StringSink message = new StringSink();

    public static CairoSecurityException instance() {
        CairoSecurityException ex = tlException.get();
        ex.message.clear();
        return ex;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    public CairoSecurityException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put(message);
    }

}
