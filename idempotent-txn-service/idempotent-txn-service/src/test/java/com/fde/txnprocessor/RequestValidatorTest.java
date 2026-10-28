package com.fde.txnprocessor;

import com.fde.txnprocessor.model.RawRecord;
import com.fde.txnprocessor.validation.RequestValidator;
import com.fde.txnprocessor.validation.ValidationResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestValidatorTest {
    private final RequestValidator validator = new RequestValidator();
    private final TestSupport t = new TestSupport();

    @Test
    void acceptsAWellFormedRecord() {
        ValidationResult r = validator.validate(t.row("TXN-1", "REQ-1", "1", "ACC-1", "credit", "10.50", "USD"));
        assertTrue(r.isValid());
        assertEquals(1L, r.request().sequenceNumber());
    }

    @Test
    void reportsAllProblemsAtOnceWithoutEchoingValues() {
        ValidationResult r = validator.validate(t.row("", "REQ-1", "0", "ACC-1", "CREDIT", "secret-not-a-number", "USD"));
        assertFalse(r.isValid());
        String joined = String.join(";", r.errors());
        assertTrue(joined.contains("transactionId is required"));
        assertTrue(joined.contains("sequenceNumber must be >= 1"));
        assertTrue(joined.contains("amount must be numeric"));
        assertFalse(joined.contains("secret-not-a-number"));
    }

    @Test
    void keepsAValidSequenceAvailableEvenWhenOtherFieldsAreBad() {
        ValidationResult r = validator.validate(t.row("TXN-1", "REQ-1", "4", "ACC-1", "CREDIT", "-1", "USD"));
        assertFalse(r.isValid());
        assertEquals(4L, (long) r.sequenceNumber());
    }

    @Test
    void rejectsUnparseableSequence() {
        ValidationResult r = validator.validate(t.row("TXN-1", "REQ-1", "four", "ACC-1", "CREDIT", "1", "USD"));
        assertFalse(r.isValid());
        assertNull(r.sequenceNumber());
    }

    @Test
    void rejectsParseErrorRows() {
        ValidationResult r = validator.validate(new RawRecord(9, Map.of(), "row has 3 columns, expected 7"));
        assertFalse(r.isValid());
    }
}
