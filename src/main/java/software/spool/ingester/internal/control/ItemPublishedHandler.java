package software.spool.ingester.internal.control;

import software.spool.core.control.Handler;
import software.spool.core.exception.SpoolException;
import software.spool.core.model.InboxItemStatus;
import software.spool.core.model.ItemPersisted;
import software.spool.core.model.ItemPublished;
import software.spool.core.port.EventBusEmitter;
import software.spool.core.port.InboxUpdater;
import software.spool.ingester.api.port.DataLakeWriter;

import java.util.Collection;

/**
 * Handler that processes a batch of {@link ItemPublished} events by writing
 * them to the data lake, updating their inbox status to {@code PERSISTED},
 * and emitting {@link ItemPersisted} events on the event bus.
 *
 * <p>
 * This is the core processing logic invoked by the
 * {@link software.spool.ingester.internal.utils.FlushCoordinator}
 * after a flush is triggered.
 * </p>
 */
public class ItemPublishedHandler implements Handler<Collection<ItemPublished>> {
    private final DataLakeWriter dataLakeWriter;
    private final InboxUpdater updater;
    private final EventBusEmitter emitter;

    /**
     * Creates a new handler with the given ports.
     *
     * @param dataLakeWriter the writer that persists events to the data lake
     * @param updater        the inbox updater for marking items as persisted
     * @param emitter        the event bus emitter for publishing
     *                       {@link ItemPersisted} events
     */
    public ItemPublishedHandler(DataLakeWriter dataLakeWriter, InboxUpdater updater, EventBusEmitter emitter) {
        this.dataLakeWriter = dataLakeWriter;
        this.updater = updater;
        this.emitter = emitter;
    }

    /**
     * Writes the batch to the data lake, updates each item's inbox status to
     * {@code PERSISTED}, and emits an {@link ItemPersisted} event for each item.
     *
     * @param items the batch of published items to process; must not be
     *              {@code null}
     * @throws SpoolException if the data lake write or inbox update fails
     */
    @Override
    public void handle(Collection<ItemPublished> items) throws SpoolException {
        dataLakeWriter.write(items);
        items.forEach(item -> {
            updater.update(item.idempotencyKey(), InboxItemStatus.PERSISTED);
            emitter.emit(ItemPersisted.builder().from(item).build());
        });
    }
}
