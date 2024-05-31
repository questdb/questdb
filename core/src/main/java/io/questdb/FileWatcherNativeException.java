package io.questdb;

import io.questdb.std.Os;

public class FileWatcherNativeException extends Exception {
    public FileWatcherNativeException(String msg) {
        super(String.format("%s [errno: %d]", msg, Os.errno()));
    }

}
