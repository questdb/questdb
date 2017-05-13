package com.questdb.net.lp;

import com.questdb.std.str.ByteSequence;

public interface LineProtoListener {
    void onEvent(ByteSequence token, int type);
}
