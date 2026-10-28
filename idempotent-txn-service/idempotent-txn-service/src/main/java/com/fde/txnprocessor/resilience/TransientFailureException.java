package com.fde.txnprocessor.resilience;

/** A failure that may succeed if tried again (timeout, lock contention, downstream blip). */
public class TransientFailureException extends Exception {
    public TransientFailureException(String message) {
        super(message);
    }
}
