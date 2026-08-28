package io.questdb.lifecycle;

import io.questdb.Bootstrap;

/**
 * Thrown by {@link LifecycleOrchestrator#run()} when a boot-essential
 * component fails. Extends {@link Bootstrap.BootstrapException} so the
 * existing catch path in {@code ServerMain.main} (exit code 55) handles
 * it without modification.
 */
public class LifecycleStartupException extends Bootstrap.BootstrapException {

    public LifecycleStartupException(String message) {
        super(message);
    }

    public LifecycleStartupException(String message, boolean silentStacktrace) {
        super(message, silentStacktrace);
    }

    public LifecycleStartupException(String message, Throwable cause) {
        super(message);
        initCause(cause);
    }

    public LifecycleStartupException(Throwable thr) {
        super(thr);
    }
}
