package io.questdb.cutlass.http;

import io.questdb.std.ObjectPool;
import io.questdb.std.str.DirectUtf8String;

public interface HttpHeaderParserFactory {
    HttpHeaderParser newParser(int bufferLen, ObjectPool<DirectUtf8String> pool);
}
