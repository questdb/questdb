package io.questdb.cutlass.line.tcp;

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
    private int[] workerAffinity = { -1, -1 };

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public int[] getWorkerAffinity() {
        return workerAffinity;
    }

    @Override
    public int getWorkerCount() {
        return 2;
    }

    @Override
    public boolean haltOnError() {
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
    public IODispatcherConfiguration getDispatcherConfiguration() {
        return ioDispatcherConfiguration;
    }

    @Override
    public int getMsgBufferSize() {
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
    public int getNWriterThreads() {
        return 2;
    }

    @Override
    public int getWriterQueueSize() {
        return 64;
    }

    @Override
    public MicrosecondClock getMicrosecondClock() {
        return MicrosecondClockImpl.INSTANCE;
    }
}
