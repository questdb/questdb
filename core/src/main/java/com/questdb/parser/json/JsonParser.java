package com.questdb.parser.json;

import com.questdb.std.str.ByteSequence;

@FunctionalInterface
public interface JsonParser {
    void onEvent(int code, ByteSequence tag, int position) throws JsonException;
}
