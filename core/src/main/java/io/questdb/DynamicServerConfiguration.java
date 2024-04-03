package io.questdb;

import java.util.Properties;

public interface DynamicServerConfiguration extends ServerConfiguration {
    void reload(Properties properties);

    CharSequence getConfRoot();
}
