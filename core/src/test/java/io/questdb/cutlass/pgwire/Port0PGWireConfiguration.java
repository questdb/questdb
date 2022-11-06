package io.questdb.cutlass.pgwire;

import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;

public class Port0PGWireConfiguration extends DefaultPGWireConfiguration {
    private final DefaultIODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration() {
        @Override
        public int getBindPort() {
            return 0;  // Bind to ANY port.
        }

        @Override
        public String getDispatcherLogName() {
            return "pg-server";
        }
    };

    @Override
    public IODispatcherConfiguration getDispatcherConfiguration() {
        return ioDispatcherConfiguration;
    }
}
