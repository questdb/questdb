package com.nfsdb.journal;

import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.test.model.Quote;

import java.lang.reflect.InvocationTargetException;

public class StupidTest {
    public static void main(String[] args) throws JournalException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Quote q = new Quote();


        JournalFactory f = new JournalFactory("c:/");

        JournalWriter<Quote> w = f.writer(Quote.class);

        for (int k = 0; k < 10; k++) {
            long t = System.nanoTime();
            for (int i = 0; i < 10000000; i++) {
                w.clearObject(q);
            }
            System.out.println(System.nanoTime() - t);
        }

    }
}
