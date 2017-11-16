package com.questdb.cutlass.receiver.parser;

public interface CachedCharSequence extends CharSequence {
    long getCacheAddress();
}
