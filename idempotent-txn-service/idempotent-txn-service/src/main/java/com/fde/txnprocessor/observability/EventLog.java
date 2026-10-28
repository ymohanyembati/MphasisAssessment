package com.fde.txnprocessor.observability;

import java.util.Map;

/** Sink for structured processing events. */
public interface EventLog {
    void event(String name, Map<String, Object> fields);
}
