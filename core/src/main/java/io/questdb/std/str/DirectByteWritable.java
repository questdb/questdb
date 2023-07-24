package io.questdb.std.str;

public interface DirectByteWritable {
    long reserveForUtf8Write(int byteCount);

    int availForUtf8Write();

    void advanceUtf8WriteAddr(int byteCount);
}
