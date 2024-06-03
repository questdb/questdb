package io.questdb;

import java.util.Properties;

public interface DynamicServerConfiguration extends ServerConfiguration, FileEventCallback {
    void reload(Properties properties);

}
