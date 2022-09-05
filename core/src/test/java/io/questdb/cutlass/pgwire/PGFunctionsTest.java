package io.questdb.cutlass.pgwire;

import io.questdb.mp.WorkerPoolManager;
import io.questdb.std.FilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *
 */
public class PGFunctionsTest extends BasePGTest {

    @Test
    public void testListTablesDoesntLeakMetaFds() throws Exception {
        assertMemoryLeak(() -> {
            try(final PGWireServer server = createPGServer(2)) {
                workerPoolManager.startAll();
                try (
                        final Connection connection = getConnection(server.getPort(), true, true)
                ) {
                    try (CallableStatement st1 = connection.prepareCall("create table a (i int)")) {
                        st1.execute();
                    }
                    sink.clear();
                    long openFilesBefore = FilesFacadeImpl.INSTANCE.getOpenFileCount();
                    try (PreparedStatement ps = connection.prepareStatement("select id,name,designatedTimestamp,partitionBy,maxUncommittedRows,commitLag from tables()")) {
                        try (ResultSet rs = ps.executeQuery()) {
                            assertResultSet("id[INTEGER],name[VARCHAR],designatedTimestamp[VARCHAR],partitionBy[VARCHAR],maxUncommittedRows[INTEGER],commitLag[BIGINT]\n" +
                                    "1,a,null,NONE,1000,0\n", sink, rs);
                        }
                    }
                    long openFilesAfter = FilesFacadeImpl.INSTANCE.getOpenFileCount();

                    Assert.assertEquals(openFilesBefore, openFilesAfter);
                }
            } finally {
                workerPoolManager.closeAll();
            }
        });
    }
}
