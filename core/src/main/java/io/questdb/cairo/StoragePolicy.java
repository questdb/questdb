package io.questdb.cairo;

public interface StoragePolicy {
    default void convertPartitionsToParquet() {
    }

    default boolean dropLocalPartitions() {
        return false;
    }

    default boolean dropNativePartitions() {
        return false;
    }

    default void dropRemotePartitions() {
    }

    boolean enforceTtl();
}
