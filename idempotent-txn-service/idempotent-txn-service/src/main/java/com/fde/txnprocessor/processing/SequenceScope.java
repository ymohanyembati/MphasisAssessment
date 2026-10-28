package com.fde.txnprocessor.processing;

/** What a sequence number is unique/contiguous over. */
public enum SequenceScope {
    /** One contiguous sequence 1..N across the whole feed (matches the suggested test data). */
    GLOBAL,
    /** An independent contiguous sequence 1..N per account. */
    PER_ACCOUNT
}
