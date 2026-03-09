package software.spool.ingester.internal.utils;

import software.spool.core.model.ItemPublished;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Buffer {
    private final List<ItemPublished> items = new ArrayList<>();
    private final FlushPolicy policy;
    private Instant lastFlush = Instant.now();

    public Buffer(FlushPolicy policy) {
        this.policy = policy;
    }

    public synchronized void insert(ItemPublished item) {
        items.add(item);
    }

    public synchronized boolean shouldFlush() {
        return policy.shouldFlush(items.size(), Duration.between(lastFlush, Instant.now()));
    }

    public synchronized Collection<ItemPublished> flush() {
        if (items.isEmpty()) return List.of();
        List<ItemPublished> snapshot = List.copyOf(items);
        items.clear();
        lastFlush = Instant.now();
        return snapshot;
    }
}
