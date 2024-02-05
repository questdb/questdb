package io.questdb.std;

import io.questdb.std.str.LPSZ;

public final class Filewatcher {

    public native static long setup(long path);
    public native static void teardown(long address);
    public native static boolean changed(long address);

}
