package io.questdb.misc;


import io.questdb.cairo.ColumnType;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.Constants;

import java.lang.reflect.Field;
import java.util.*;

public class SqlGrammarUtil {
    private static void print(String header, Set<String> names) {
        System.out.printf("%s:%n", header);
        for (String name : names) {
            System.out.printf("\"%s\",%n", name);
        }
        System.out.print("\n=====\n");
    }

    public static void main(String... args) {
        // static
        final Set<String> staticSet = new TreeSet<>();
        Collections.addAll(
                staticSet,
                "&", "|", "^", "~", "[]",
                "!=", "!~", "%", "*", "+",
                "-", ".", "/", "<", "<=",
                "<>", "<>all", "=", ">", ">="
        );

        // function names
        final Set<String> names = new TreeSet<>();
        for (FunctionFactory factory : ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())) {
            if (factory.getClass().getName().contains("test")) {
                continue;
            }
            String signature = factory.getSignature();
            String name = signature.substring(0, signature.indexOf('('));
            if (staticSet.contains(name)) {
                continue;
            }
            names.add(name);
            // add != counterparts to equality function factories
            if (factory.isBoolean()) {
                if (name.equals("=")) {
                    names.add("!=");
                    names.add("<>");
                } else if (name.equals("<")) {
                    names.add("<=");
                    names.add(">=");
                    names.add(">");
                }
            }
        }
        print("FUNCTIONS", names);

        // keywords
        names.clear();
        try {
            Field field = Constants.class.getDeclaredField("KEYWORDS");
            field.setAccessible(true);
            for (CharSequence keyword : (CharSequence[]) field.get(null)) {
                names.add((String) keyword);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        print("KEYWORDS", names);

        // types
        names.clear();
        final Set<String> skipSet = new HashSet<>();
        Collections.addAll(skipSet, "unknown", "regclass", "regprocedure", "VARARG", "text[]", "CURSOR", "RECORD", "PARAMETER");
        for (int type = 1; type < ColumnType.NULL; type++) {
            String name = ColumnType.nameOf(type);
            if (!skipSet.contains(name)) {
                names.add(name.toLowerCase());
            }
        }
        print("TYPES", names);
    }
}