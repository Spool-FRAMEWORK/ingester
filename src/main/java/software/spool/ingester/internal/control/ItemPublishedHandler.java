package software.spool.ingester.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.InboxItemStatus;
import software.spool.core.model.event.ItemPersisted;
import software.spool.core.model.event.ItemPublished;
import software.spool.core.model.failure.ItemQuarantined;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventBusEmitter;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.inbox.InboxReader;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.QuarantineStore;
import software.spool.ingester.api.port.QuarantinedRecord;
import software.spool.ingester.internal.model.PublishedInboxItem;
import software.spool.validator.api.ValidationResult;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ItemPublishedHandler implements Handler<Collection<ItemPublished>> {
    private final DataLakeWriter dataLakeWriter;
    private final InboxReader reader;
    private final InboxUpdater updater;
    private final EventBusEmitter emitter;
    private final ItemValidator validator;
    private final QuarantineStore quarantineStore;
    private final ErrorRouter errorRouter;

    public ItemPublishedHandler(DataLakeWriter dataLakeWriter,
                                InboxReader reader,
                                InboxUpdater updater,
                                EventBusEmitter emitter,
                                ItemValidator validator,
                                QuarantineStore quarantineStore,
                                ErrorRouter errorRouter) {
        this.dataLakeWriter = dataLakeWriter;
        this.reader = reader;
        this.updater = updater;
        this.emitter = emitter;
        this.validator = validator;
        this.quarantineStore = quarantineStore;
        this.errorRouter = errorRouter;
    }

    @Override
    public void handle(Collection<ItemPublished> items) throws SpoolException {
        try {
            List<PublishedInboxItem> valid = partition(resolve(items));
            Set<IdempotencyKey> written = dataLakeWriter.write(
                    valid.stream()
                            .map(PublishedInboxItem::inboxItem)
                            .collect(Collectors.toList())
            ).collect(Collectors.toSet());
            valid.stream()
                    .filter(item -> written.contains(item.idempotencyKey()))
                    .forEach(this::updateAndEmit);
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }

    private List<PublishedInboxItem> resolve(Collection<ItemPublished> publishedItems) {
        return publishedItems.stream()
                .map(published -> reader.getBy(published.idempotencyKey())
                        .map(inboxItem -> new PublishedInboxItem(published, inboxItem)))
                .filter(java.util.Optional::isPresent)
                .map(java.util.Optional::get)
                .collect(Collectors.toList());
    }

    private List<PublishedInboxItem> partition(Collection<PublishedInboxItem> items) {
        List<PublishedInboxItem> valid = new ArrayList<>();
        items.forEach(item -> {
            ValidationResult result = validator.validate(item);
            if (result.isQuarantine()) quarantine(item, result);
            else valid.add(item);
        });
        return valid;
    }

    private void updateAndEmit(PublishedInboxItem item) {
        try {
            updater.update(item.inboxItem().idempotencyKey(), InboxItemStatus.PERSISTED);
            emitter.emit(ItemPersisted.builder()
                    .from(item.published())
                    .partitionKey(PartitionKey.of(item.inboxItem().partitionKeySchema()).from(item.payload()))
                    .build());
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }

    private void quarantine(PublishedInboxItem item, ValidationResult result) {
        try {
            List<String> violations = result.getViolations().stream()
                    .map(Throwable::getMessage)
                    .toList();
            quarantineStore.send(new QuarantinedRecord(
                    item.inboxItem().payload(),
                    violations,
                    Instant.now()
            ));
            emitter.emit(ItemQuarantined.builder()
                    .from(item.published())
                    .violations(violations)
                    .build());
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }
}