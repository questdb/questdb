package com.questdb.txt.parser.listener.probe;

import com.questdb.std.ObjList;

public class TypeProbeCollection {
    private static final ObjList<String> DEFAULT_DATE_FORMATS = new ObjList<>();
    private final ObjList<TypeProbe> probes = new ObjList<>();
    private final int probeCount;

    public TypeProbeCollection() {
        probes.add(new IntProbe());
        probes.add(new LongProbe());
        probes.add(new DoubleProbe());
        probes.add(new BooleanProbe());
        probes.add(new DateIsoProbe());
        probes.add(new DateFmt1Probe());
        probes.add(new DateFmt2Probe());
        probes.add(new DateFmt3Probe());
        this.probeCount = probes.size();
    }

    public int getProbeCount() {
        return probeCount;
    }

    public int getType(int index) {
        return probes.getQuick(index).getType();
    }

    public boolean probe(int index, CharSequence text) {
        return probes.getQuick(index).probe(text);
    }

    static {
        DEFAULT_DATE_FORMATS.add("yyyy-MM-dd HH:mm:ss");
        DEFAULT_DATE_FORMATS.add("dd/MM/y");
        DEFAULT_DATE_FORMATS.add("MM/dd/y");
    }
}
