package io.questdb;

/**
 * Implementations of this interface must be thread-safe as the callback
 * may be called by file watcher thread and reload_config() SQL.
 */
@FunctionalInterface
public interface FileEventCallback {
    // returns true if the config was reloaded
    boolean reload();
}
