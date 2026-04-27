package software.spool.ingester.internal.utils;

import software.spool.core.model.event.EnvelopeStored;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Buffer {
    private final List<EnvelopeStored> items = new ArrayList<>();

    /**
     * Adds an envelopeStored to the buffer.
     *
     * @param envelopeStored the event to buffer; must not be {@code null}
     */
    public synchronized void add(EnvelopeStored envelopeStored) {
        items.add(envelopeStored);
    }

    /**
     * Atomically drains all items from the buffer and returns them.
     *
     * @return an unmodifiable snapshot of the buffered items, or an empty
     *         collection if the buffer was empty
     */
    public synchronized Collection<EnvelopeStored> drain() {
        if (items.isEmpty())
            return List.of();
        List<EnvelopeStored> snapshot = List.copyOf(items);
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
