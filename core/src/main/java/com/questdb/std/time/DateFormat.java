package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.std.str.CharSink;

public interface DateFormat {

    void append(long datetime, DateLocale locale, CharSink sink) throws NumericException;

    long parse(CharSequence in, DateLocale locale) throws NumericException;

    long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException;
}
