package com.questdb.std.microtime;

import com.questdb.std.Os;

public class MicrosecondClockImpl implements MicrosecondClock {
    public static final MicrosecondClock INSTANCE = new MicrosecondClockImpl();

    @Override
    public long getTicks() {
        return Os.currentTimeMicros();
    }
}
