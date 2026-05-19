package io.questdb.test.lifecycle.fakes;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.std.ObjList;

import java.util.function.Supplier;

/** Test fake: start() throws a RuntimeException supplied at construction. */
public final class ThrowingComponent implements Component {

    private final Supplier<RuntimeException> exceptionFactory;
    private final ObjList<String> hardDeps;
    private final String name;
    private final ObjList<String> softDeps;
    private volatile boolean stopped;

    public ThrowingComponent(String name, Supplier<RuntimeException> exceptionFactory) {
        this(name, exceptionFactory, new ObjList<>(), new ObjList<>());
    }

    public ThrowingComponent(
            String name,
            Supplier<RuntimeException> exceptionFactory,
            ObjList<String> hardDeps,
            ObjList<String> softDeps
    ) {
        this.name = name;
        this.exceptionFactory = exceptionFactory;
        this.hardDeps = hardDeps;
        this.softDeps = softDeps;
    }

    @Override
    public ObjList<String> hardRequiredDependencies() {
        return hardDeps;
    }

    public boolean isStopped() {
        return stopped;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ObjList<String> softDependencies() {
        return softDeps;
    }

    @Override
    public void start(LifecycleContext ctx) {
        throw exceptionFactory.get();
    }

    @Override
    public void stop() {
        stopped = true;
    }
}
