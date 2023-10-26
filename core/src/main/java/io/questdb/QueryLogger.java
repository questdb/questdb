package io.questdb;

import io.questdb.cairo.SecurityContext;
import io.questdb.log.Log;

public interface QueryLogger {
    default void logEmptyQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext) {
        logQuery(logger, doLog, fd, query, securityContext, "empty query");
    }

    default void logExecQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext) {
        logQuery(logger, doLog, fd, query, securityContext, "exec");
    }

    default void logParseQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext) {
        logQuery(logger, doLog, fd, query, securityContext, "parse");
    }

    void logQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext, String logText);
}
