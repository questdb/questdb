package com.questdb.parser.lp;

public interface LineProtoParser {
    int EVT_MEASUREMENT = 1;
    int EVT_TAG_VALUE = 2;
    int EVT_FIELD_VALUE = 3;
    int EVT_TAG_NAME = 4;
    int EVT_FIELD_NAME = 5;
    int EVT_TIMESTAMP = 6;

    int ERROR_EXPECTED = 1;
    int ERROR_ENCODING = 2;
    int ERROR_EMPTY = 3;

    void onError(int position, int state, int code);

    void onEvent(CachedCharSequence token, int type, CharSequenceCache cache);

    void onLineEnd(CharSequenceCache cache);
}
