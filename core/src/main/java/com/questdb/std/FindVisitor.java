package com.questdb.std;

@FunctionalInterface
public interface FindVisitor {
    void onFind(long name, int type);
}
