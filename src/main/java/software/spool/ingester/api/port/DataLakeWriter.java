package software.spool.ingester.api.port;

import software.spool.core.model.event.ItemPublished;

import java.util.Collection;

/**
 * Port for writing batches of events into the data lake.
 *
 * <p>
 * Implement this interface to connect the ingester to your storage backend
 * (database, object store, data warehouse, etc.). The ingester calls
 * {@link #write(Collection)} once per flush cycle with all buffered events.
 * </p>
 *
 * <p>
 * A simple in-memory implementation for testing is available in
 * {@link software.spool.ingester.internal.adapter.InMemoryDataLake}.
 * </p>
 */
public interface DataLakeWriter {
    /**
     * Writes a batch of events to the data lake.
     *
     * @param items the events to persist; never {@code null} or empty
     * @throws software.spool.ingester.internal.exception.DataLakeWriteException
     *                                                                           if
     *                                                                           the
     *                                                                           batch
     *                                                                           could
     *                                                                           not
     *                                                                           be
     *                                                                           persisted
     */
    void write(Collection<ItemPublished> items);
}