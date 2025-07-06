package io.questdb.cairo;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("ForLoopReplaceableByForEach")
public enum ErrorTag {
    NONE(""),
    UNSUPPORTED_FILE_SYSTEM("UNSUPPORTED FILE SYSTEM"),
    DISK_FULL("DISK FULL"),
    TOO_MANY_OPEN_FILES("TOO MANY OPEN FILES"),
    OUT_OF_MMAP_AREAS("OUT OF MMAP AREAS"),
    OUT_OF_MEMORY("OUT OF MEMORY");

    private static final CharSequenceObjHashMap<ErrorTag> resolveMap = new CharSequenceObjHashMap<>();
    private final String text;

    ErrorTag(String text) {
        this.text = text;
    }

    public static ErrorTag resolveTag(@NotNull CharSequence text) {
        final ErrorTag errorTag = resolveMap.get(text);
        if (errorTag == null) {
            throw CairoException.nonCritical().put("Invalid WAL error tag [").put(text).put("]");
        }
        return errorTag;
    }

    public static ErrorTag resolveTag(int code) {
        return Os.isWindows() ? windows(code) : linux(code);
    }

    public String text() {
        return text;
    }

    static ErrorTag linux(int code) {
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

    static ErrorTag windows(int code) {
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

    static {
        final ErrorTag[] values = values();
        for (int i = 0; i < values.length; i++) {
            final ErrorTag tag = values[i];
            resolveMap.put(tag.text, tag);
        }
    }
}
