package software.spool.ingester.internal.utils;

import java.time.Duration;

public class FlushPolicy {
    private final int limit;
    private final Duration interval;

    private FlushPolicy(int limit, Duration interval) {
        this.limit = limit;
        this.interval = interval;
    }

    public static FlushPolicy immediate() {
        return new FlushPolicy(1, Duration.ZERO);
    }

    public static FlushPolicy whenReaches(int limit) {
        return new FlushPolicy(limit, null);
    }

    public static FlushPolicy every(Duration interval) {
        return new FlushPolicy(-1, interval);
    }

    public FlushPolicy orEvery(Duration interval) {
        return new FlushPolicy(limit, interval);
    }

    public boolean shouldFlush(int bufferSize, Duration elapsed) {
        if (bufferSize == 0) return false;
        if (limit > 0 && bufferSize >= limit) return true;
        return interval != null && !elapsed.minus(interval).isNegative();
    }
}
