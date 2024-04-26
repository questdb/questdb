package io.questdb;

import java.util.Properties;

public interface DynamicServerConfiguration extends ServerConfiguration {
    FileEventCallback getFileEventCallback();

    void reload(Properties properties);

}
