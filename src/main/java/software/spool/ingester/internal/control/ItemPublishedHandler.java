package software.spool.ingester.internal.control;

import software.spool.core.control.Handler;
import software.spool.core.exception.SpoolException;
import software.spool.core.model.*;
import software.spool.core.port.EventBusEmitter;
import software.spool.core.port.InboxUpdater;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.QuarantineStore;
import software.spool.ingester.api.port.QuarantinedRecord;
import software.spool.validator.api.ValidationResult;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
    private final ItemValidator validator;
    private final QuarantineStore quarantineStore;

    public ItemPublishedHandler(DataLakeWriter dataLakeWriter, InboxUpdater updater,
                                EventBusEmitter emitter, ItemValidator validator, QuarantineStore quarantineStore) {
        this.dataLakeWriter = dataLakeWriter;
        this.updater = updater;
        this.emitter = emitter;
        this.validator = validator;
        this.quarantineStore = quarantineStore;
    }

    @Override
    public void handle(Collection<ItemPublished> items) throws SpoolException {
        List<ItemPublished> valid = partition(items);
        dataLakeWriter.write(valid);
        valid.forEach(this::persistAndEmit);
    }

    private List<ItemPublished> partition(Collection<ItemPublished> items) {
        List<ItemPublished> valid = new ArrayList<>();
        items.forEach(item -> {
            ValidationResult result = validator.validate(item);
            if (result.isQuarantine()) quarantine(item, result);
            else valid.add(item);
        });
        return valid;
    }

    private void persistAndEmit(ItemPublished item) {
        updater.update(item.idempotencyKey(), InboxItemStatus.PERSISTED);
        emitter.emit(ItemPersisted.builder()
                .from(item)
                .partitionKey(buildPartitionKey(item))
                .build());
    }

    private void quarantine(ItemPublished item, ValidationResult result) {
        List<String> violations = result.getViolations().stream()
                .map(Throwable::getMessage)
                .toList();
        quarantineStore.send(new QuarantinedRecord(item.payload(), violations, Instant.now()));
        emitter.emit(ItemQuarantined.builder()
                .from(item)
                .violations(violations)
                .build());
    }

    private PartitionKey buildPartitionKey(ItemPublished item) {
        return PartitionKey.of(item.partitionKeySchema()).from(item.payload());
    }
}

