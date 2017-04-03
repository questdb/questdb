package com.questdb.std;

import com.questdb.ex.NumericException;

public interface DateFormat {

    long parse(CharSequence in, DateLocale locale) throws NumericException;

    long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException;
}
