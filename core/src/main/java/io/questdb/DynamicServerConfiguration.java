package io.questdb;

import java.util.Properties;

public interface DynamicServerConfiguration extends ServerConfiguration {
    ConfigReloader getConfigReloader();

    void reload(Properties properties);

}
