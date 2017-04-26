package com.questdb.json;

public interface JsonListener {
    void onEvent(int code, CharSequence tag);
}
