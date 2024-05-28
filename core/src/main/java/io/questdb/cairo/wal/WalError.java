package io.questdb.cairo.wal;

import io.questdb.std.Numbers;
import io.questdb.std.Os;

import static io.questdb.cairo.wal.WalErrorTag.linux;
import static io.questdb.cairo.wal.WalErrorTag.windows;

public class WalError {
    public static final WalError OK = new WalError(Numbers.INT_NULL, "");

    private final int errorCode;
    private final CharSequence errorMessage;
    private final WalErrorTag errorTag;

    public WalError(int errorCode, CharSequence errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;

        errorTag = Os.isWindows() ? windows(errorCode) : linux(errorCode);
    }

    public int getErrorCode() {
        return errorCode;
    }

    public CharSequence getErrorMessage() {
        return errorMessage;
    }

    public WalErrorTag getErrorTag() {
        return errorTag;
    }
}
