package com.fde.txnprocessor.validation;

import com.fde.txnprocessor.model.RawRecord;
import com.fde.txnprocessor.model.TransactionRequest;
import com.fde.txnprocessor.model.TransactionType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;
import java.util.regex.Pattern;

/** Pure business validation: no I/O, no state. Error messages name fields, never echo values. */
public final class RequestValidator {
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");
    private static final int MAX_PRECISION = 15;
    private static final int MAX_SCALE = 4;

    public ValidationResult validate(RawRecord raw) {
        List<String> errors = new ArrayList<>();
        if (raw.parseError() != null) {
            errors.add(raw.parseError());
        }

        String transactionId = id(raw, "transactionId", errors);
        String requestId = id(raw, "requestId", errors);
        String accountId = id(raw, "accountId", errors);
        Long sequence = sequence(raw, errors);
        TransactionType type = type(raw, errors);
        BigDecimal amount = amount(raw, errors);
        String currency = currency(raw, errors);

        if (!errors.isEmpty()) {
            return new ValidationResult(null, List.copyOf(errors), sequence, accountId);
        }
        TransactionRequest request = new TransactionRequest(transactionId, requestId, sequence, accountId, type, amount, currency);
        return new ValidationResult(request, List.of(), sequence, accountId);
    }

    private static String id(RawRecord raw, String field, List<String> errors) {
        String v = raw.get(field);
        if (v == null || v.isEmpty()) {
            errors.add(field + " is required");
            return null;
        }
        if (!ID.matcher(v).matches()) {
            errors.add(field + " has an invalid format");
            return null;
        }
        return v;
    }

    private static Long sequence(RawRecord raw, List<String> errors) {
        String v = raw.get("sequenceNumber");
        if (v == null || v.isEmpty()) {
            errors.add("sequenceNumber is required");
            return null;
        }
        try {
            long seq = Long.parseLong(v);
            if (seq < 1) {
                errors.add("sequenceNumber must be >= 1");
                return null;
            }
            return seq;
        } catch (NumberFormatException e) {
            errors.add("sequenceNumber must be a whole number");
            return null;
        }
    }

    private static TransactionType type(RawRecord raw, List<String> errors) {
        String v = raw.get("transactionType");
        if (v == null || v.isEmpty()) {
            errors.add("transactionType is required");
            return null;
        }
        try {
            return TransactionType.valueOf(v.toUpperCase());
        } catch (IllegalArgumentException e) {
            errors.add("transactionType is not supported");
            return null;
        }
    }

    private static BigDecimal amount(RawRecord raw, List<String> errors) {
        String v = raw.get("amount");
        if (v == null || v.isEmpty()) {
            errors.add("amount is required");
            return null;
        }
        try {
            BigDecimal amount = new BigDecimal(v);
            if (amount.signum() <= 0) {
                errors.add("amount must be greater than zero");
                return null;
            }
            BigDecimal normalized = amount.stripTrailingZeros();
            if (normalized.scale() > MAX_SCALE || normalized.precision() - normalized.scale() > MAX_PRECISION) {
                errors.add("amount exceeds supported precision");
                return null;
            }
            return amount;
        } catch (NumberFormatException e) {
            errors.add("amount must be numeric");
            return null;
        }
    }

    private static String currency(RawRecord raw, List<String> errors) {
        String v = raw.get("currency");
        if (v == null || v.isEmpty()) {
            errors.add("currency is required");
            return null;
        }
        if (!CURRENCY.matcher(v).matches()) {
            errors.add("currency must be a 3-letter ISO 4217 code");
            return null;
        }
        try {
            Currency.getInstance(v);
            return v;
        } catch (IllegalArgumentException e) {
            errors.add("currency is not a known ISO 4217 code");
            return null;
        }
    }
}
