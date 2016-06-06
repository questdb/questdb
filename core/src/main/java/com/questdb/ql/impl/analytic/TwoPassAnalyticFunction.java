package com.questdb.ql.impl.analytic;

import com.questdb.ql.Record;
import com.questdb.ql.impl.RecordList;

public interface TwoPassAnalyticFunction extends AnalyticFunction {
    void addRecord(Record record, long rowid);

    void prepare(RecordList base);
}
