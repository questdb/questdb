package io.questdb.cutlass.pgwire;

import io.questdb.mp.WorkerPool;
import io.questdb.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PGFunctionsTest extends BasePGTest {

    @Test
    public void testListTablesDoesntLeakMetaFds() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGWireServer server = createPGServer(2);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(server.getPort(), true, true)) {
                    try (CallableStatement st1 = connection.prepareCall("create table a (i int)")) {
                        st1.execute();
                    }
                    sink.clear();
                    long openFilesBefore = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();
                    try (PreparedStatement ps = connection.prepareStatement("select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()")) {
                        try (ResultSet rs = ps.executeQuery()) {
                            assertResultSet(
                                    "id[INTEGER],name[VARCHAR],designatedTimestamp[VARCHAR],partitionBy[VARCHAR],maxUncommittedRows[INTEGER],o3MaxLag[BIGINT]\n" +
                                            "1,a,null,NONE,1000,300000000\n",
                                    sink,
                                    rs
                            );
                        }
                    }
                    long openFilesAfter = TestFilesFacadeImpl.INSTANCE.getOpenFileCount();

                    Assert.assertEquals(openFilesBefore, openFilesAfter);
                }
            }
        });
    }
}
