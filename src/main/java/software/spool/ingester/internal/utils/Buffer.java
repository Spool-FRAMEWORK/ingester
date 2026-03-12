package software.spool.ingester.internal.utils;

import software.spool.core.model.ItemPublished;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Thread-safe in-memory buffer for {@link ItemPublished} events.
 *
 * <p>
 * Items are added individually via {@link #add(ItemPublished)} and drained
 * atomically via {@link #drain()}. Draining returns a snapshot of all buffered
 * items and clears the buffer in a single synchronized operation.
 * </p>
 */
public class Buffer {
    private final List<ItemPublished> items = new ArrayList<>();

    /**
     * Adds an item to the buffer.
     *
     * @param item the event to buffer; must not be {@code null}
     */
    public synchronized void add(ItemPublished item) {
        items.add(item);
    }

    /**
     * Atomically drains all items from the buffer and returns them.
     *
     * @return an unmodifiable snapshot of the buffered items, or an empty
     *         collection if the buffer was empty
     */
    public synchronized Collection<ItemPublished> drain() {
        if (items.isEmpty())
            return List.of();
        List<ItemPublished> snapshot = List.copyOf(items);
        items.clear();
        return snapshot;
    }

    /**
     * Returns the current number of items in the buffer.
     *
     * @return the buffer size
     */
    public synchronized int size() {
        return items.size();
    }
}
