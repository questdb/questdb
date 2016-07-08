package com.questdb.mp;

import com.questdb.std.ObjectFactory;

class Event {
    static final ObjectFactory<Event> FACTORY = new ObjectFactory<Event>() {
        @Override
        public Event newInstance() {
            return new Event();
        }
    };
    int value;
}
