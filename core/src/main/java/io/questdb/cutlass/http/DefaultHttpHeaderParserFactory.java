package io.questdb.cutlass.http;

import io.questdb.std.ObjectPool;
import io.questdb.std.str.DirectUtf8String;

public final class DefaultHttpHeaderParserFactory implements HttpHeaderParserFactory {
    public static final DefaultHttpHeaderParserFactory INSTANCE = new DefaultHttpHeaderParserFactory();

    @Override
    public HttpHeaderParser newParser(int bufferLen, ObjectPool<DirectUtf8String> pool) {
        return new HttpHeaderParser(bufferLen, pool);
    }
}
