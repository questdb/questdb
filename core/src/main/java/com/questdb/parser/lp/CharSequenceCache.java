package com.questdb.parser.lp;

@FunctionalInterface
public interface CharSequenceCache {
    CharSequence get(long address);
}
