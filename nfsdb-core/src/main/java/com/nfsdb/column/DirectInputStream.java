package com.nfsdb.column;

import java.io.InputStream;

public abstract class DirectInputStream extends InputStream {
    public abstract int getLength();
    public abstract long copyTo(long address, long start, long length);
}
