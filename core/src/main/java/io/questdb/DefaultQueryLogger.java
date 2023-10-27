package io.questdb;

import io.questdb.cairo.SecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;

public final class DefaultQueryLogger implements QueryLogger {
    public static final DefaultQueryLogger INSTANCE = new DefaultQueryLogger();

    private DefaultQueryLogger() {
    }

    @Override
    public LogRecord logQuery(Log logger, int fd, CharSequence query, SecurityContext securityContext, String logText) {
        return logger.info().$(logText)
                .$(" [fd=").$(fd)
                .$(", q=").utf8(query);
    }
}
