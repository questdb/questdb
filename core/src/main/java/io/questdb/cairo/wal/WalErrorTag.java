package io.questdb.cairo.wal;

import io.questdb.std.Os;

public enum WalErrorTag {
    DISK_FULL("DISK FULL"),
    TOO_MANY_OPEN_FILES("TOO MANY OPEN FILES"),
    OUT_OF_MEMORY("OUT OF MEMORY"),
    FAILED_MEMORY_ALLOCATION("FAILED MEMORY ALLOCATION"),
    OTHER("");

    private final String text;

    WalErrorTag(String text) {
        this.text = text;
    }

    public static WalErrorTag resolveTag(int code) {
        return Os.isWindows() ? windows(code) : linux(code);
    }

    public String text() {
        return text;
    }

    static WalErrorTag linux(int code) {
        switch (code) {
            case 28:
                return DISK_FULL;
            case 24:
                return TOO_MANY_OPEN_FILES;
            case 12:
                return OUT_OF_MEMORY;
            default:
                return OTHER;
        }
    }

    static WalErrorTag windows(int code) {
        switch (code) {
            case 39:
            case 112:
                return DISK_FULL;
            case 4:
                return TOO_MANY_OPEN_FILES;
            case 8:
                return OUT_OF_MEMORY;
            default:
                return OTHER;
        }
    }
}
