package io.questdb.cairo;

public class DdlListenerFactoryImpl implements DdlListenerFactory {
    public static final DdlListenerFactory INSTANCE = new DdlListenerFactoryImpl();

    @Override
    public DdlListener getInstance() {
        return DdlListenerImpl.INSTANCE;
    }
}
