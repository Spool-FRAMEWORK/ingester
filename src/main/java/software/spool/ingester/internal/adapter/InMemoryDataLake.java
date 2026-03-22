package software.spool.ingester.internal.adapter;

import software.spool.core.model.Event;
import software.spool.core.model.ItemPublished;
import software.spool.core.model.PartitionKey;
import software.spool.ingester.api.port.DataLakeWriter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory implementation of {@link DataLakeWriter} intended for local
 * testing and development purposes.
 *
 * <p>
 * Events are stored in a thread-safe {@link CopyOnWriteArrayList}. All
 * persisted events can be retrieved via {@link #findAll()}.
 * </p>
 *
 * <p>
 * <strong>Note:</strong> this implementation keeps everything in memory and
 * should not be used in production environments.
 * </p>
 */
public class InMemoryDataLake implements DataLakeWriter {

    private final Map<PartitionKey, List<Event>> store = new ConcurrentHashMap<>();

    @Override
    public void write(Collection<ItemPublished> items) {
        items.forEach(event ->
                store.computeIfAbsent(
                        PartitionKey.of(event.partitionKeySchema()).from(event.payload()),
                        k -> Collections.synchronizedList(new ArrayList<>())
                ).add(event)
        );
    }

    public List<Event> findAll() {
        return store.values().stream()
                .flatMap(List::stream)
                .toList();
    }

    public List<Event> findByPartitionKey(PartitionKey key) {
        return List.copyOf(store.getOrDefault(key, List.of()));
    }
}

