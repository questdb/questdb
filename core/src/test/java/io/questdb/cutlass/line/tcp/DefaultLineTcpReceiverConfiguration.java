package io.questdb.cutlass.line.tcp;

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoNanoTimestampAdapter;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.std.microtime.MicrosecondClockImpl;

public class DefaultLineTcpReceiverConfiguration implements LineTcpReceiverConfiguration {
    private final IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration();

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public CairoSecurityContext getCairoSecurityContext() {
        return AllowAllCairoSecurityContext.INSTANCE;
    }

    @Override
    public LineProtoTimestampAdapter getTimestampAdapter() {
        return LineProtoNanoTimestampAdapter.INSTANCE;
    }

    @Override
    public int getConnectionPoolInitialCapacity() {
        return 64;
    }

    @Override
    public IODispatcherConfiguration getNetDispatcherConfiguration() {
        return ioDispatcherConfiguration;
    }

    @Override
    public int getNetMsgBufferSize() {
        return 2048;
    }

    @Override
    public int getMaxMeasurementSize() {
        return 512;
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public int getWriterQueueSize() {
        return 64;
    }

    @Override
    public MicrosecondClock getMicrosecondClock() {
        return MicrosecondClockImpl.INSTANCE;
    }

    @Override
    public WorkerPoolAwareConfiguration getNetWorkerPoolConfiguration() {
        return WorkerPoolAwareConfiguration.USE_SHARED_CONFIGURATION;
    }

    @Override
    public WorkerPoolAwareConfiguration getWriterWorkerPoolConfiguration() {
        return WorkerPoolAwareConfiguration.USE_SHARED_CONFIGURATION;
    }
}
