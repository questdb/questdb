package io.questdb.cutlass.text;

import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public class FileSplitterTest extends AbstractGriffinTest {

    //test csv with large string field 
    //test csv with timestamp over buffer boundaries 
    //test csv with timestamp over buffer boundaries that's too long 
    //test csv with bad timestamp value 
    //test csv with quoted field that is too long and doesn't end before newline (should make a mess also with TextLexer/TextLoader)

    @Before
    public void before() throws IOException {
        inputRoot = new File(".").getAbsolutePath();
        inputWorkRoot = temp.newFolder("imports").getAbsolutePath();
    }

    @Test
    public void testSimpleCsv() throws Exception {
        assertMemoryLeak(() -> {

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            //String inputDir = new File(".").getAbsolutePath();
            String inputDir = new File("E:/dev/tmp").getAbsolutePath();

            //try (Path path = new Path().of(inputDir).slash().concat("src/test/resources/csv/test-import.csv").$();
            try (Path path = new Path().of(inputDir).slash().concat("trips300mil.csv").$();
                 FileSplitter splitter = new FileSplitter(engine)) {

                DateFormat format = new TimestampFormatCompiler().compile("yyyy-MM-ddTHH:mm:ss.SSSUUUZ");

                long fd = ff.openRO(path);
                Assert.assertTrue(fd > -1);

                try {
                    //splitter.split("test-import-csv", fd, PartitionBy.MONTH, (byte) ',', 4, format, true);
                    splitter.split("test-import-csv", fd, PartitionBy.MONTH, (byte) ',', 2, format, true);
                } finally {
                    ff.close(fd);
                }
            }

            //Thread.sleep(180000);
        });
    }

}
