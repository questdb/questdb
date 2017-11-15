package com.questdb.std.time;

import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;

import java.time.ZoneId;
import java.util.Map;

public class TimeZoneRuleFactory {

    public static final TimeZoneRuleFactory INSTANCE = new TimeZoneRuleFactory();

    private final CharSequenceIntHashMap ruleMap = new CharSequenceIntHashMap();
    private final ObjList<TimeZoneRules> ruleList = new ObjList<>();

    public TimeZoneRuleFactory() {
        int index = 0;
        for (String z : ZoneId.getAvailableZoneIds()) {
            ruleList.add(new TimeZoneRulesImpl(z, ZoneId.of(z).getRules()));
            ruleMap.put(z, index++);
        }

        for (Map.Entry<String, String> e : ZoneId.SHORT_IDS.entrySet()) {
            String key = e.getKey();
            String alias = e.getValue();

            // key already added somehow?
            int i = ruleMap.get(key);
            if (i == -1) {
                // no, good, add
                i = ruleMap.get(alias);
                if (i == -1) {
                    // this could be fixed offset, try parsing value as one
                    long offset = Dates.parseOffset(alias, 0, alias.length());
                    if (offset != Long.MIN_VALUE) {
                        ruleList.add(new FixedTimeZoneRule(alias, Numbers.decodeInt(offset) * Dates.MINUTE_MILLIS));
                        ruleMap.put(key, index++);
                    }
                } else {
                    ruleMap.put(key, i);
                }
            }
        }
    }

    public TimeZoneRules getTimeZoneRules(CharSequence id) {
        int index = ruleMap.get(id);
        if (index == -1) {
            return null;
        }
        return ruleList.getQuick(index);
    }

    public int getTimeZoneRulesIndex(CharSequence id) {
        return ruleMap.get(id);
    }

    public TimeZoneRules getTimeZoneRulesQuick(int index) {
        return ruleList.getQuick(index);
    }
}
