package com.fde.txnprocessor.observability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Keeps events in memory so tests can assert on them. */
public final class CollectingEventLog implements EventLog {
    public record Event(String name, Map<String, Object> fields) {}

    private final List<Event> events = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void event(String name, Map<String, Object> fields) {
        events.add(new Event(name, fields));
    }

    public List<Event> events() {
        synchronized (events) {
            return new ArrayList<>(events);
        }
    }

    public long count(String name) {
        return events().stream().filter(e -> e.name().equals(name)).count();
    }
}
