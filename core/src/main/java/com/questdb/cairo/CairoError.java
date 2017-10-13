package com.questdb.cairo;

public class CairoError extends Error {
    public CairoError(Throwable cause) {
        super(cause);
    }

    public CairoError(String message, Throwable cause) {
        super(message, cause);
    }
}
