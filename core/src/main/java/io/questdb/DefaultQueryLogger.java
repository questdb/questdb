package io.questdb;

import io.questdb.cairo.SecurityContext;
import io.questdb.log.Log;

public final class DefaultQueryLogger implements QueryLogger {
    public static final DefaultQueryLogger INSTANCE = new DefaultQueryLogger();

    private DefaultQueryLogger() {
    }

    @Override
    public void logQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext, String logText) {
        if (doLog) {
            logger.info().$(logText)
                    .$(" [fd=").$(fd)
                    .$(", q=").utf8(query)
                    .I$();
        }
    }
}
