package software.spool.ingester.api.port;

import software.spool.core.model.Event;

import java.util.Collection;

public interface DataLakeWriter {
    <E extends Event> void write(Collection<E> items);
}