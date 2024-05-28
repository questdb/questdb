package io.questdb.cairo.wal;

import io.questdb.std.Numbers;
import io.questdb.std.Os;

public class WalError {
    public static final WalError OK = new WalError(Numbers.INT_NULL, null, "");

    private final int errorCode;
    private final CharSequence errorMessage;
    private final Tag errorTag;

    public WalError(int errorCode, CharSequence errorMessage) {
        this(errorCode, Tag.resolveTag(errorCode), errorMessage);
    }

    public WalError(int errorCode, Tag errorTag, CharSequence errorMessage) {
        this.errorCode = errorCode;
        this.errorTag = errorTag;
        this.errorMessage = errorMessage;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public CharSequence getErrorMessage() {
        return errorMessage;
    }

    public Tag getErrorTag() {
        return errorTag;
    }

    public enum Tag {
        DISK_FULL,
        TOO_MANY_OPEN_FILES,
        OUT_OF_MEMORY,
        FAILED_MEMORY_ALLOCATION;

        static Tag linux(int code) {
            switch (code) {
                case 28:
                    return DISK_FULL;
                case 24:
                    return TOO_MANY_OPEN_FILES;
                case 12:
                    return OUT_OF_MEMORY;
                default:
                    return null;
            }
        }

        static Tag resolveTag(int code) {
            return Os.isWindows() ? windows(code) : linux(code);
        }

        static Tag windows(int code) {
            switch (code) {
                case 39:
                case 112:
                    return DISK_FULL;
                case 4:
                    return TOO_MANY_OPEN_FILES;
                case 8:
                    return OUT_OF_MEMORY;
                default:
                    return null;
            }
        }
    }
}
