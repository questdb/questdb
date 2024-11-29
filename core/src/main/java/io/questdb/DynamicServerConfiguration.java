package io.questdb;

public interface DynamicServerConfiguration extends ServerConfiguration, FileEventCallback {

    boolean isConfigReloadEnabled();
}
