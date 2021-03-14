package io.questdb.cutlass.pgwire;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.TimeZone;

abstract class AbstractPGContextTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(AbstractPGContextTest.class);

    PGWireServer createPGServer(PGWireConfiguration configuration) {
        return PGWireServer.create(
                configuration,
                null,
                LOG,
                engine,
                compiler.getFunctionFactoryCache()
        );
    }

    PGWireServer createPGServer(int workerCount) {
        final int[] affinity = new int[workerCount];
        Arrays.fill(affinity, -1);

        final PGWireConfiguration conf = new DefaultPGWireConfiguration() {
            @Override
            public Rnd getRandom() {
                return new Rnd();
            }

            @Override
            public int[] getWorkerAffinity() {
                return affinity;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }
        };

        return createPGServer(conf);
    }

    Connection getConnection(boolean simple, boolean binary) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", Boolean.toString(binary));
        if (simple) {
            properties.setProperty("preferQueryMode", "simple");
        }

        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:8812/qdb", properties);
    }
}
