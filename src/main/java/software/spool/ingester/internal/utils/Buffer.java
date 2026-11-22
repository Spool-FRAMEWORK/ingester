package software.spool.ingester.internal.utils;

import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.IdempotencyKey;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The envelopes announced since the last flush, one per idempotency key: an envelope announced again while it waits
 * keeps its first announcement.
 */
public class Buffer {
    private final Map<IdempotencyKey, EnvelopeStored> items = new LinkedHashMap<>();

    public synchronized void add(EnvelopeStored envelopeStored) {
        items.putIfAbsent(envelopeStored.idempotencyKey(), envelopeStored);
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
        List<EnvelopeStored> snapshot = List.copyOf(items.values());
        items.clear();
        return snapshot;
    }

    public synchronized int size() {
        return items.size();
    }
}
