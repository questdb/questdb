package com.questdb.parser.json;

@FunctionalInterface
public interface JsonParser {
    void onEvent(int code, CharSequence tag, int position) throws JsonException;
}
