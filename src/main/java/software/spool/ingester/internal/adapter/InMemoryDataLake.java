package software.spool.ingester.internal.adapter;

import software.spool.core.model.Event;
import software.spool.ingester.api.port.DataLakeWriter;

import java.util.Collection;
import java.util.List;
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
    private final CopyOnWriteArrayList<Event> store = new CopyOnWriteArrayList<>();

    @Override
    public <E extends Event> void write(Collection<E> items) {
        store.addAll(items);
    }

    public List<Event> findAll() {
        return List.copyOf(store);
    }
}
