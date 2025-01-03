package io.questdb;

@FunctionalInterface
public interface ConfigReloader {

    /**
     * @return true if the config was reloaded
     */
    boolean reload();
}
