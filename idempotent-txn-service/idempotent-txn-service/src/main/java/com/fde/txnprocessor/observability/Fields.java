package com.fde.txnprocessor.observability;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Fields {
    private Fields() {}

    /** Ordered key/value builder: Fields.of("k1", v1, "k2", v2). Null values are skipped. */
    public static Map<String, Object> of(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            if (kv[i + 1] != null) {
                m.put(String.valueOf(kv[i]), kv[i + 1]);
            }
        }
        return m;
    }

    /** Masks an identifier for logs: keeps at most the last 3 characters. */
    public static String mask(String id) {
        if (id == null || id.length() <= 4) {
            return "***";
        }
        return "***" + id.substring(id.length() - 3);
    }
}
