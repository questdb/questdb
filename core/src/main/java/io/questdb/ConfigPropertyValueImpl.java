package io.questdb;

public class ConfigPropertyValueImpl implements ConfigPropertyValue {
    private final boolean dynamic;
    private final String value;
    private final int valueSource;

    public ConfigPropertyValueImpl(String value, int valueSource, boolean dynamic) {
        this.dynamic = dynamic;
        this.value = value;
        this.valueSource = valueSource;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public int getValueSource() {
        return valueSource;
    }

    @Override
    public boolean isDynamic() {
        return dynamic;
    }
}
