package io.questdb.test.cutlass.text;

import io.questdb.cutlass.text.CsvTextLexer;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CsvTextLexerNullByteTest extends AbstractCairoTest {

    @Test
    public void testNullByteContaminationInCsv() throws Exception {
        String csv = "id,name,value\n" +
                "1,test\0data,100\n";

        byte[] bytes = csv.getBytes(StandardCharsets.UTF_8);
        long len = bytes.length;
        long ptr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);

        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
            }

            try (CsvTextLexer lexer = new CsvTextLexer(new DefaultTextConfiguration())) {
                ObjList<String> parsedNames = new ObjList<>();

                lexer.setupBeforeExactLines((line, fields, hi) -> {
                    if (line == 1) {
                        parsedNames.add(fields.getQuick(1).toString());
                    }
                });
                lexer.parse(ptr, ptr + len);

                Assert.assertEquals("Row containing null byte should be dropped", 0, parsedNames.size());
                Assert.assertEquals("Lexer error count should increment by 1", 1, lexer.getErrorCount());
            }
        } finally {
            Unsafe.free(ptr, len, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
