package io.questdb.cairo.wal;

public enum WalErrorTag {
    DISK_FULL, TOO_MANY_OPEN_FILES;

    static WalErrorTag linux(int code) {
        switch (code) {
            case 28:
                return DISK_FULL;
            case 24:
                return TOO_MANY_OPEN_FILES;
            default:
                return null;
        }
    }

    static WalErrorTag windows(int code) {
        switch (code) {
            case 39:
            case 112:
                return DISK_FULL;
            case 4:
                return TOO_MANY_OPEN_FILES;
            default:
                return null;
        }
    }
}
