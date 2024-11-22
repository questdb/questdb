package io.questdb;

import java.util.Properties;

public interface DynamicServerConfiguration extends ServerConfiguration, FileEventCallback {

    boolean isConfigReloadEnabled();

    void reload(Properties properties);
}
