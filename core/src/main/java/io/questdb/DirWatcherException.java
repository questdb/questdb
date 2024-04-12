package io.questdb;

public class DirWatcherException extends Exception {
    public DirWatcherException(String method, int code) {
        super(String.format("%s returned %d", method, code));
    }
}
