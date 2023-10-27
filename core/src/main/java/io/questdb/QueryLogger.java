package io.questdb;

import io.questdb.cairo.SecurityContext;
import io.questdb.log.Log;

public interface QueryLogger {
    // called when an empty query received
    default void logEmptyQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext) {
        logQuery(logger, doLog, fd, query, securityContext, "empty query");
    }

    // called when a cached query executed
    default void logExecQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext) {
        logQuery(logger, doLog, fd, query, securityContext, "exec");
    }

    // called when a new query parsed
    default void logParseQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext) {
        logQuery(logger, doLog, fd, query, securityContext, "parse");
    }

    void logQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext, String logText);
}
