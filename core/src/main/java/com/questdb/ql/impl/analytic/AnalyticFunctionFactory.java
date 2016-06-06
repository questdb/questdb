package com.questdb.ql.impl.analytic;

import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.model.AnalyticColumn;

public interface AnalyticFunctionFactory {
    AnalyticFunction newInstance(ServerConfiguration configuration, RecordMetadata metadata, AnalyticColumn column) throws ParserException;
}
