package software.spool.ingester.internal.utils;

import java.time.Duration;

/**
 * Defines the rules for when a buffer should be flushed to the data lake.
 *
 * <p>
 * A policy evaluates two conditions: the current buffer size and the elapsed
 * time since the last flush. A flush is triggered when either condition is met.
 * </p>
 *
 * <p>
 * Three built-in presets are provided:
 * </p>
 * <ul>
 * <li>{@link #immediate()} — flush on every single item (no buffering).</li>
 * <li>{@link #whenReaches(int)} — flush when the buffer reaches the given
 * size.</li>
 * <li>{@link #every(Duration)} — flush at a fixed time interval regardless of
 * buffer size.</li>
 * </ul>
 *
 * <p>
 * Policies can be combined using {@link #orEvery(Duration)}:
 * </p>
 * 
 * <pre>{@code
 * FlushPolicy.whenReaches(100).orEvery(Duration.ofSeconds(30))
 * }</pre>
 */
public class FlushPolicy {
    private final int limit;
    private final Duration interval;

    private FlushPolicy(int limit, Duration interval) {
        this.limit = limit;
        this.interval = interval;
    }

    /**
     * Creates a policy that flushes immediately on every item.
     *
     * @return an immediate flush policy
     */
    public static FlushPolicy immediate() {
        return new FlushPolicy(1, Duration.ZERO);
    }

    /**
     * Creates a policy that flushes when the buffer reaches the given size.
     *
     * @param limit the number of items that triggers a flush; must be positive
     * @return a size-based flush policy
     */
    public static FlushPolicy whenReaches(int limit) {
        return new FlushPolicy(limit, null);
    }

    /**
     * Creates a policy that flushes at a fixed time interval.
     *
     * @param interval the time interval between flushes; must not be {@code null}
     * @return a time-based flush policy
     */
    public static FlushPolicy every(Duration interval) {
        return new FlushPolicy(-1, interval);
    }

    /**
     * Combines this policy with a time interval, creating a composite policy that
     * flushes when either the original condition or the time interval is met.
     *
     * @param interval the additional time interval; must not be {@code null}
     * @return a composite flush policy
     */
    public FlushPolicy orEvery(Duration interval) {
        return new FlushPolicy(limit, interval);
    }

    /**
     * Determines whether a flush should occur given the current buffer size
     * and elapsed time since the last flush.
     *
     * @param bufferSize the current number of buffered items
     * @param elapsed    the time elapsed since the last flush
     * @return {@code true} if the buffer should be flushed
     */
    public boolean shouldFlush(int bufferSize, Duration elapsed) {
        if (bufferSize == 0)
            return false;
        if (limit > 0 && bufferSize >= limit)
            return true;
        return interval != null && !elapsed.minus(interval).isNegative();
    }
}
