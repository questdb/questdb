package com.questdb.factory;

public class LastError {
    public static final int E_POOL_CLOSED = 1;
    public static final int E_NAME_LOCKED = 2;
    public static final int E_POOL_FULL = 3;
    public static final int E_NOT_AN_OWNER = 4;
    public static final int E_AGAIN = 5;
    public static final int E_JOURNAL_ERROR = 6;
    public static final int E_INTERNAL = 7;
    public static final int E_OK = 0;

    static final ThreadLocal<Error> error = new ThreadLocal<Error>() {
        @Override
        protected Error initialValue() {
            return new Error();
        }
    };

    public static int getError() {
        return error.get().code;
    }


    static void error(int code) {
        error.get().code = code;
    }

    private static class Error {
        int code = E_OK;
    }
}
