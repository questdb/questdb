package com.questdb.json;

import com.questdb.std.str.ByteSequence;

public interface JsonListener {
    void onEvent(int code, ByteSequence tag, int position) throws JsonException;
}
