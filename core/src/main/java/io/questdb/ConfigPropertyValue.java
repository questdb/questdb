package io.questdb;

public interface ConfigPropertyValue {
    int VALUE_SOURCE_CONF = 1;
    int VALUE_SOURCE_DEFAULT = 0;
    int VALUE_SOURCE_ENV = 2;

    String getValue();

    /**
     * Source of configuration value.
     *
     * @return one of VALUE_SOURCE_* values
     */
    int getValueSource();

    boolean isDynamic();
}
