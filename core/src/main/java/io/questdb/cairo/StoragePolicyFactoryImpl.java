package io.questdb.cairo;

public class StoragePolicyFactoryImpl implements StoragePolicyFactory {
    public static final StoragePolicyFactory INSTANCE = new StoragePolicyFactoryImpl();

    private StoragePolicyFactoryImpl() {
    }

    @Override
    public StoragePolicy createStoragePolicy(TableWriter tableWriter) {
        return new StoragePolicyImpl(tableWriter);
    }
}
