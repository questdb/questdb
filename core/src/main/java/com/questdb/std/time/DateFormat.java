package com.questdb.std.time;

import com.questdb.common.NumericException;
import com.questdb.std.str.CharSink;

public interface DateFormat {

    void format(long datetime, DateLocale locale, CharSequence timeZoneName, CharSink sink);

    long parse(CharSequence in, DateLocale locale) throws NumericException;

    long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException;
}
