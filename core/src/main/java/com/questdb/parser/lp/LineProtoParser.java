package com.questdb.parser.lp;

import com.questdb.std.str.ByteSequence;

@FunctionalInterface
public interface LineProtoParser {
    void onEvent(ByteSequence token, int type);
}
