package com.questdb.ql.impl.analytic;

import com.questdb.std.ObjHashSet;

public final class AnalyticUtils {
    public static final ThreadLocal<ObjHashSet<String>> HASH_SET = new ThreadLocal<ObjHashSet<String>>() {
        @Override
        protected ObjHashSet<String> initialValue() {
            return new ObjHashSet<>();
        }
    };

    private AnalyticUtils() {
    }
}
