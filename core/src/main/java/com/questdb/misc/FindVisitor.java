package com.questdb.misc;

@FunctionalInterface
public interface FindVisitor {
    void onFind(long name, int type);
}
