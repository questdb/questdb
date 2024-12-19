package io.questdb;

@FunctionalInterface
public interface FileEventCallback {

    void onFileEvent();
}
