package io.questdb.cutlass.line.tcp;

import java.io.Closeable;
import java.io.IOException;

import org.jetbrains.annotations.Nullable;

import io.questdb.MessageBus;
import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.WorkerPoolAwareConfiguration.ServerFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.EagerThreadSetup;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.network.IOContextFactory;
import io.questdb.network.IODispatcher;
import io.questdb.network.IODispatchers;
import io.questdb.network.IORequestProcessor;
import io.questdb.std.Misc;
import io.questdb.std.ThreadLocal;
import io.questdb.std.WeakObjectPool;

public class LineTcpServer implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpServer.class);

    @Nullable
    public static LineTcpServer create(
            CairoConfiguration cairoConfiguration,
            LineTcpReceiverConfiguration lineConfiguration,
            WorkerPool sharedWorkerPool,
            Log log,
            CairoEngine cairoEngine,
            MessageBus messageBus
    ) {
        if (!lineConfiguration.isEnabled()) {
            return null;
        }

        ServerFactory<LineTcpServer, LineTcpReceiverConfiguration> factory = (conf, engine, workerPool, local,
                bus) -> new LineTcpServer(
                        cairoConfiguration,
                        lineConfiguration,
                        cairoEngine,
                        workerPool,
                        bus);
        return WorkerPoolAwareConfiguration.create(lineConfiguration, sharedWorkerPool, log, cairoEngine, factory, messageBus);
    }

    private final IODispatcher<LineTcpConnectionContext> dispatcher;
    private final LineTcpConnectionContextFactory contextFactory;
    private final LineTcpMeasurementScheduler scheduler;

    public LineTcpServer(
            CairoConfiguration cairoConfiguration,
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool workerPool,
            MessageBus messageBus
    ) {
        this.contextFactory = new LineTcpConnectionContextFactory(engine, lineConfiguration, messageBus, workerPool.getWorkerCount());
        this.dispatcher = IODispatchers.create(
                lineConfiguration
                        .getDispatcherConfiguration(),
                contextFactory);
        workerPool.assign(dispatcher);
        // TODO allow seperate worker pool for writers
        scheduler = new LineTcpMeasurementScheduler(cairoConfiguration, lineConfiguration, engine, workerPool);
        final IORequestProcessor<LineTcpConnectionContext> processor = (operation, context) -> {
            context.handleIO();
        };
        workerPool.assign(new SynchronizedJob() {
            @Override
            protected boolean runSerially() {
                return dispatcher.processIOQueue(processor);
            }
        });

        for (int i = 0, n = workerPool.getWorkerCount(); i < n; i++) {
            // http context factory has thread local pools
            // therefore we need each thread to clean their thread locals individually
            workerPool.assign(i, () -> {
                contextFactory.closeContextPool();
            });
        }
    }

    @Override
    public void close() {
        Misc.free(scheduler);
        Misc.free(contextFactory);
        Misc.free(dispatcher);
    }

    private class LineTcpConnectionContextFactory implements IOContextFactory<LineTcpConnectionContext>, Closeable, EagerThreadSetup {
        private final ThreadLocal<WeakObjectPool<LineTcpConnectionContext>> contextPool;
        private boolean closed = false;

        public LineTcpConnectionContextFactory(CairoEngine engine, LineTcpReceiverConfiguration configuration, @Nullable MessageBus messageBus, int workerCount) {
            this.contextPool = new ThreadLocal<>(
                    () -> new WeakObjectPool<>(() -> new LineTcpConnectionContext(configuration, scheduler), configuration.getConnectionPoolInitialCapacity()));
        }

        @Override
        public void setup() {
            contextPool.get();
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

        @Override
        public LineTcpConnectionContext newInstance(long fd, IODispatcher<LineTcpConnectionContext> dispatcher) {
            return contextPool.get().pop().of(fd, dispatcher);
        }

        @Override
        public void done(LineTcpConnectionContext context) {
            if (closed) {
                Misc.free(context);
            } else {
                context.of(-1, null);
                contextPool.get().push(context);
                LOG.info().$("pushed").$();
            }
        }

        private void closeContextPool() {
            Misc.free(this.contextPool.get());
            LOG.info().$("closed").$();
        }

    }
}
