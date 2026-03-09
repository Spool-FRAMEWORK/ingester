package software.spool.ingester.internal.adapter;

import software.spool.core.model.Event;
import software.spool.ingester.api.port.DataLakeWriter;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
