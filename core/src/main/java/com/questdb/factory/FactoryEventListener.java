package com.questdb.factory;

public interface FactoryEventListener {
    int SRC_WRITER = 1;
    int SRC_READER = 2;

    int EV_RETURN = 1;
    int EV_OUT_OF_POOL_CLOSE = 2;
    int EV_UNEXPECTED_CLOSE = 3;
    int EV_COMMIT_EX = 4;
    int EV_NOT_IN_POOL = 5;
    int EV_LOCK_SUCCESS = 6;
    int EV_LOCK_BUSY = 7;
    int EV_UNLOCKED = 8;
    int EV_NOT_LOCKED = 9;
    int EV_CREATE = 10;
    int EV_GET = 11;
    int EV_CLOSE = 12;
    int EV_INCOMPATIBLE = 13;
    int EV_CREATE_EX = 14;
    int EV_CLOSE_EX = 15;
    int EV_EXPIRE = 17;
    int EV_EXPIRE_EX = 18;
    int EV_LOCK_CLOSE = 19;
    int EV_LOCK_CLOSE_EX = 20;
    int EV_EX_RESEND = 21;
    int EV_RELEASE_ALL = 22;
    int EV_POOL_OPEN = 23;
    int EV_POOL_CLOSED = 24;

    void onEvent(int factoryType, long thread, String name, int event);
}
