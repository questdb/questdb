package io.questdb.cutlass.text;

import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class TextUtilTest {

    @Test
    public void testQuotedTextParsing() throws Utf8Exception {
        StringSink query = new StringSink();

        String text = "select count(*) from \"file.csv\" abcd";
        copyToSinkWithTextUtil(query, text);

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testDoubleQuotedTextParsing() throws Utf8Exception {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        copyToSinkWithTextUtil(query, text);

        Assert.assertEquals(text.replace("\"\"", "\""), query.toString());
    }

    private void copyToSinkWithTextUtil(StringSink query, String text) throws Utf8Exception {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        long ptr = Unsafe.malloc(bytes.length);
        for(int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
        }

        TextUtil.utf8Decode(ptr, ptr + bytes.length, query);
        Unsafe.free(ptr, bytes.length);
    }
}
