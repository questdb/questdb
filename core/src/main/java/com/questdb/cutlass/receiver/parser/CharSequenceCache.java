package com.questdb.cutlass.receiver.parser;

@FunctionalInterface
public interface CharSequenceCache {
    CharSequence get(long address);
}
