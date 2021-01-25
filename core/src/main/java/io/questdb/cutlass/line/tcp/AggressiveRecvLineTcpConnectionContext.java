package io.questdb.cutlass.line.tcp;

public class AggressiveRecvLineTcpConnectionContext extends LineTcpConnectionContext {
    AggressiveRecvLineTcpConnectionContext(LineTcpReceiverConfiguration configuration, LineTcpMeasurementScheduler scheduler) {
        super(configuration, scheduler);
    }

    @Override
    IOContextResult handleIO() {
        IOContextResult rc;
        read();
        do {
            rc = parseMeasurements();
        } while (rc == IOContextResult.NEEDS_READ && read());
        return rc;
    }
}
