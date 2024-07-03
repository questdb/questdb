package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.std.Chars;
import io.questdb.std.Os;

public enum WalErrorTag {
    NONE(""),
    DISK_FULL("DISK FULL"),
    TOO_MANY_OPEN_FILES("TOO MANY OPEN FILES"),
    OUT_OF_MMAP_AREAS("OUT OF MMAP AREAS"),
    OUT_OF_MEMORY("OUT OF MEMORY");

    private final String text;

    WalErrorTag(String text) {
        this.text = text;
    }

    public static WalErrorTag resolveTag(CharSequence text) {
        if (text == null) {
            throw CairoException.nonCritical().put("Invalid WAL error tag [null]");
        } else if (Chars.equals(text, DISK_FULL.text)) {
            return DISK_FULL;
        } else if (Chars.equals(text, TOO_MANY_OPEN_FILES.text)) {
            return TOO_MANY_OPEN_FILES;
        } else if (Chars.equals(text, OUT_OF_MMAP_AREAS.text)) {
            return OUT_OF_MMAP_AREAS;
        } else if (Chars.equals(text, OUT_OF_MEMORY.text)) {
            return OUT_OF_MEMORY;
        } else if (Chars.equals(text, NONE.text)) {
            return NONE;
        } else {
            throw CairoException.nonCritical().put("Invalid WAL error tag [").put(text).put("]");
        }
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
                return OUT_OF_MMAP_AREAS;
            default:
                return NONE;
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
                return OUT_OF_MMAP_AREAS;
            default:
                return NONE;
        }
    }
}
