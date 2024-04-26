package io.questdb;

public class FileEventNotifierException extends Exception {
    public FileEventNotifierException(String method, int code) {
        super(String.format("%s returned %d", method, code));
    }
}
