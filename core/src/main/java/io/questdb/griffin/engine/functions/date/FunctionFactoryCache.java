package io.questdb.griffin.engine.functions.date;

import java.util.ArrayList;
import java.util.List;

public class FunctionFactoryCache {
    
    private List<FunctionFactory> factories;

    public FunctionFactoryCache() {
        factories = new ArrayList<>();
        factories.add(new IntervalTimezoneFunction.Factory());
    }

    // Other methods can be added here
}
