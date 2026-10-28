package com.fde.txnprocessor.observability;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Map;

/** Writes one JSON object per line (JSON Lines), ready for any log shipper. */
public final class StructuredEventLog implements EventLog {
    private final PrintStream out;

    public StructuredEventLog(PrintStream out) {
        this.out = out;
    }

    @Override
    public void event(String name, Map<String, Object> fields) {
        StringBuilder sb = new StringBuilder(160);
        sb.append("{\"ts\":\"").append(Instant.now()).append("\",\"event\":\"").append(escape(name)).append('"');
        for (Map.Entry<String, Object> e : fields.entrySet()) {
            sb.append(",\"").append(escape(e.getKey())).append("\":");
            Object v = e.getValue();
            if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
            } else {
                sb.append('"').append(escape(String.valueOf(v))).append('"');
            }
        }
        sb.append('}');
        out.println(sb);
    }

    static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
