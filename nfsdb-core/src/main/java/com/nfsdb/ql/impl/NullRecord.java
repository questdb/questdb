package com.nfsdb.ql.impl;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.utils.Numbers;

import java.io.OutputStream;

public class NullRecord extends AbstractRecord {

    public NullRecord(RecordMetadata metadata) {
        super(metadata);
    }

    @Override
    public byte get(int col) {
        return 0;
    }

    @Override
    public void getBin(int col, OutputStream s) {
    }

    @Override
    public DirectInputStream getBin(int col) {
        return null;
    }

    @Override
    public boolean getBool(int col) {
        return false;
    }

    @Override
    public long getDate(int col) {
        return Numbers.LONG_NaN;
    }

    @Override
    public double getDouble(int col) {
        return Double.NaN;
    }

    @Override
    public float getFloat(int col) {
        return Float.NaN;
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return null;
    }

    @Override
    public int getInt(int col) {
        return Numbers.INT_NaN;
    }

    @Override
    public long getLong(int col) {
        return Numbers.LONG_NaN;
    }

    @Override
    public long getRowId() {
        return -1;
    }

    @Override
    public short getShort(int col) {
        return 0;
    }

    @Override
    public CharSequence getStr(int col) {
        return null;
    }

    @Override
    public void getStr(int col, CharSink sink) {

    }

    @Override
    public String getSym(int col) {
        return null;
    }
}
