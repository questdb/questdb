package com.questdb.std.time;

import com.questdb.ex.NumericException;

public abstract class AbstractDateFormat implements DateFormat {
    @Override
    public long parse(CharSequence in, DateLocale locale) throws NumericException {
        return parse(in, 0, in.length(), locale);
    }
}
