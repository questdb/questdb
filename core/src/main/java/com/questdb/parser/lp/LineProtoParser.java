package com.questdb.parser.lp;

import com.questdb.std.str.ByteSequence;

@FunctionalInterface
public interface LineProtoParser {
    int EVT_MEASUREMENT = 1;
    int EVT_TAG_VALUE = 2;
    int EVT_FIELD_VALUE = 3;
    int EVT_TAG_NAME = 4;
    int EVT_FIELD_NAME = 5;
    int EVT_TIMESTAMP = 6;
    int EVT_END = 7;

    void onEvent(ByteSequence token, int type);
}
