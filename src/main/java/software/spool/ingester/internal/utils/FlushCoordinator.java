package software.spool.ingester.internal.utils;

import software.spool.core.model.event.ItemPublished;
import software.spool.core.port.bus.Handler;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

/**
 * Coordinates the submission and flushing of buffered {@link ItemPublished}
 * events.
 *
 * <p>
 * Items are submitted via {@link #submit(ItemPublished)}, which adds them to
 * the internal buffer and checks whether a flush is needed. A background
 * scheduler should also call {@link #flushIfNeeded()} periodically to handle
 * time-based flush policies.
 * </p>
 *
 * <p>
 * When a flush is triggered, all buffered items are drained atomically and
 * passed to the configured handler for processing.
 * </p>
 */
public class FlushCoordinator {
    private final Buffer buffer;
    private final FlushPolicy policy;
    private final Handler<Collection<ItemPublished>> handler;
    private Instant lastFlush = Instant.now();

    /**
     * Creates a new coordinator with the given buffer, policy, and handler.
     *
     * @param buffer  the buffer to accumulate items in; must not be {@code null}
     * @param policy  the flush policy that decides when to flush; must not be
     *                {@code null}
     * @param handler the handler that processes flushed batches; must not be
     *                {@code null}
     */
    public FlushCoordinator(Buffer buffer, FlushPolicy policy,
            Handler<Collection<ItemPublished>> handler) {
        this.buffer = buffer;
        this.policy = policy;
        this.handler = handler;
    }

    /**
     * Submits an item to the buffer and triggers a flush if the policy requires it.
     *
     * @param item the event to buffer; must not be {@code null}
     */
    public void submit(ItemPublished item) {
        buffer.add(item);
        flushIfNeeded();
    }

    /**
     * Checks whether the flush policy is satisfied and flushes the buffer if so.
     *
     * <p>
     * This method is intended to be called periodically by a scheduler
     * to handle time-based flush policies.
     * </p>
     */
    public void flushIfNeeded() {
        if (!policy.shouldFlush(buffer.size(), Duration.between(lastFlush, Instant.now()))) return;
        Collection<ItemPublished> items = buffer.drain();
        if (items.isEmpty()) return;
        lastFlush = Instant.now();
        handler.handle(items);
    }
}
