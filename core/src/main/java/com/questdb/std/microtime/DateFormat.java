package com.questdb.std.microtime;

import com.questdb.ex.NumericException;
import com.questdb.std.str.CharSink;

public interface DateFormat {

    void format(long micros, DateLocale locale, CharSequence timeZoneName, CharSink sink);

    long parse(CharSequence in, DateLocale locale) throws NumericException;

    long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException;
}
