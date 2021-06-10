package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.tcp.LineTcpMeasurementScheduler.NetworkIOJob;

public class AggressiveRecvLineTcpConnectionContext extends LineTcpConnectionContext {

    private final int aggressiveReadRetryCount;

    AggressiveRecvLineTcpConnectionContext(LineTcpReceiverConfiguration configuration, LineTcpMeasurementScheduler scheduler) {
        super(configuration, scheduler);
        this.aggressiveReadRetryCount = configuration.getAggressiveReadRetryCount();
    }

    @Override
    IOContextResult handleIO(NetworkIOJob netIoJob) {
        IOContextResult rc;
        read();
        do {
            rc = parseMeasurements(netIoJob);
        } while (rc == IOContextResult.NEEDS_READ && read());
        return rc;
    }

    @Override
    protected boolean read() {
        if (super.read()) {
            return true;
        }

        int remaining = aggressiveReadRetryCount;

        while (remaining > 0) {
            if (super.read()) {
                return true;
            }
            remaining--;
        }

        return false;
    }
}
