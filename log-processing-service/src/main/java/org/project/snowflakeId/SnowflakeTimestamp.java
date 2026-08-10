package org.project.snowflakeId;

public class SnowflakeTimestamp {
    // Custom epoch: 1 January 2026, 00:00:00 UTC
    private static final long CUSTOM_EPOCH = 1767225600000L;
    //OR --> long time = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli(); == 1767225600000

    private static final int TIMESTAMP_BITS = 41;

    // Maximum value that can fit inside 41 bits, basically this code gives us "(2 power 41) - 1" value
    private static final long MAX_TIMESTAMP = (1L << TIMESTAMP_BITS) - 1;

    public static long get41BitTimestamp() {
        long currentTimestamp = System.currentTimeMillis();

        // Store milliseconds elapsed since our custom epoch
        // because if we save the difference we can save 18 billion values instead of 1.7 trillion
        long timestamp = currentTimestamp - CUSTOM_EPOCH;

        if (timestamp < 0) {
            throw new IllegalStateException(
                    "Current time is before the custom epoch"
            );
        }

        if (timestamp > MAX_TIMESTAMP) {
            throw new IllegalStateException(
                    "Timestamp cannot fit inside 41 bits"
            );
        }

        return timestamp;
    }
}
