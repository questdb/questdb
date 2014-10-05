package com.nfsdb.journal;

import com.nfsdb.journal.factory.configuration.JournalMetadata;
import com.nfsdb.journal.factory.configuration.JournalMetadataBuilder;
import com.nfsdb.journal.lang.parser.TokenStream;
import org.junit.Test;

public class JournalMetadataTest {

    @Test
    public void testMetadataParse() throws Exception {
        JournalMetadataBuilder<TestDate> builder = new JournalMetadataBuilder<>(TestDate.class)
                .$sym("name")
                .$date("birthDate")
                .$ts();

        JournalMetadata meta = builder.build();

        TokenStream ts = new TokenStream();
        ts.setContent(meta.toString());
        ts.defineSymbol(" ");
        ts.defineSymbol(",");
        ts.defineSymbol("{");
        ts.defineSymbol("}");
        ts.defineSymbol("=");
        ts.defineSymbol("[");
        ts.defineSymbol("]");


        boolean startMetadata = false;
        boolean collectName = false;
        boolean collectNameValue = false;
        boolean collectType = false;
        boolean collectTypeValue = false;

        String columnName = "";

        for (String s : ts) {
            switch (s) {
                case "ColumnMetadata":
                    if (startMetadata) {
                        collectName = true;
                        collectType = true;
                    }
                    break;
                case "[":
                    startMetadata = true;
                    break;
                case "]":
                    collectName = false;
                    break;
                case "name*":
                    if (collectName) {
                        collectNameValue = true;
                        collectName = false;
                    }
                    break;
                case "=":
                    break;
                case "type*":
                    if (collectType) {
                        collectTypeValue = true;
                        collectType = false;
                    }
                    break;
                default:
                    if (collectNameValue) {
                        columnName = s;
                        collectNameValue = false;
                    }
                    if (collectTypeValue) {
                        System.out.println(columnName + "=" + s);
                        collectTypeValue = false;
                    }
            }
        }
    }


    public static class TestDate {
        private String name;
        private long birthDate;
        private long timestamp;

    }
}
