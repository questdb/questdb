package com.questdb.parser.typeprobe;

import com.questdb.common.ColumnType;
import com.questdb.common.NumericException;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocale;

public class DateProbe implements TypeProbe {
    private final String pattern;
    private final DateLocale dateLocale;
    private final DateFormat format;

    public DateProbe(DateFormatFactory dateFormatFactory, DateLocale dateLocale, String pattern) {
        this.dateLocale = dateLocale;
        this.pattern = pattern;
        this.format = dateFormatFactory.get(pattern);
    }

    @Override
    public DateFormat getDateFormat() {
        return format;
    }

    @Override
    public DateLocale getDateLocale() {
        return dateLocale;
    }

    @Override
    public String getFormat() {
        return pattern;
    }

    @Override
    public int getType() {
        return ColumnType.DATE;
    }

    @Override
    public boolean probe(CharSequence text) {
        try {
            format.parse(text, dateLocale);
            return true;
        } catch (NumericException e) {
            return false;
        }
    }
}
