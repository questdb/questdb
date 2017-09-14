package com.questdb.cairo;

import com.questdb.std.BinarySequence;

import java.io.Closeable;

public interface ReadOnlyColumn extends Closeable {

    @Override
    void close();

    BinarySequence getBin(long offset);

    boolean getBool(long offset);

    byte getByte(long offset);

    double getDouble(long offset);

    float getFloat(long offset);

    int getInt(long offset);

    long getLong(long offset);

    short getShort(long offset);

    CharSequence getStr(long offset);

    CharSequence getStr2(long offset);

    void trackFileSize();
}
