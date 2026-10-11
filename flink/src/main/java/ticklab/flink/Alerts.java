package ticklab.flink;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import ticklab.v1.Alert;
import ticklab.v1.AlertStatus;
import ticklab.v1.AlertType;

/** Building alert records. */
final class Alerts {
    static final DateTimeFormatter MINUTE =
            DateTimeFormatter.ofPattern("HH:mm").withZone(ZoneOffset.UTC);

    private Alerts() {}

    static Alert of(AlertType type, String key, long openedAt, long at, AlertStatus status,
            double value, double peak, String explanation) {
        return Alert.newBuilder()
                .setType(type)
                .setKey(key)
                .setOpenedAt(Instant.ofEpochMilli(openedAt))
                .setStatus(status)
                .setUpdateSeq(status == AlertStatus.OPEN ? 0 : 1)
                .setAt(Instant.ofEpochMilli(at))
                .setValue(value)
                .setPeak(peak)
                .setExplanation(explanation)
                .build();
    }

    /** A new array: {@code values} plus {@code value} at the end, keeping the last {@code size}. */
    static double[] append(double[] values, double value, int size) {
        int keep = Math.min(values.length, size - 1);
        double[] result = new double[keep + 1];
        System.arraycopy(values, values.length - keep, result, 0, keep);
        result[keep] = value;
        return result;
    }
}
