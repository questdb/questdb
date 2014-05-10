package org.nfsdb.examples.support;

import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import org.nfsdb.examples.model.Quote;

import java.util.Random;

public class QuoteGenerator {
    public static void generateQuoteData(JournalWriter<Quote> w, int count) throws JournalException {
        generateQuoteData(w, count, 30);
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count, int days) throws JournalException {
        long lo = System.currentTimeMillis();
        long hi = lo + days * 24 * 60 * 60 * 1000L;
        long delta = (hi - lo) / count;

        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        Quote q = new Quote();
        Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < count; i++) {
            q.clear();
            q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length))]);
            q.setAsk(Math.abs(r.nextDouble()));
            q.setBid(Math.abs(r.nextDouble()));
            q.setAskSize(Math.abs(r.nextInt() % 10000));
            q.setBidSize(Math.abs(r.nextInt() % 10000));
            q.setEx("LXE");
            q.setMode("Fast trading");
            q.setTimestamp(lo);
            w.append(q);
            lo += delta;
        }
        w.commit();
    }
}
