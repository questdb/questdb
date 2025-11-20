package io.questdb.cairo;

public interface StoragePolicyFactory {
    StoragePolicy createStoragePolicy(TableWriter tableWriter);
}
