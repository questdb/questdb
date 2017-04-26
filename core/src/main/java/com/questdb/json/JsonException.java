package com.questdb.json;

public class JsonException extends Exception {
    private static final JsonException INSTANCE = new JsonException();

    private static final ThreadLocal<ExceptionInfo> tlInfo = ThreadLocal.withInitial(ExceptionInfo::new);

    private JsonException() {
    }

    public static JsonException with(String message, int position) {
        final ExceptionInfo info = tlInfo.get();
        info.message = message;
        info.position = position;
        return INSTANCE;
    }

    public String getMessage() {
        return tlInfo.get().message;
    }

    public int getPosition() {
        return tlInfo.get().position;
    }

    private static class ExceptionInfo {
        private String message;
        private int position;
    }
}
