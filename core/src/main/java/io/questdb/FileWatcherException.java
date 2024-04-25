package io.questdb;

public class FileWatcherException extends Exception {
    public FileWatcherException(String method, int code) {
        super(String.format("%s returned %d", method, code));
    }
}
