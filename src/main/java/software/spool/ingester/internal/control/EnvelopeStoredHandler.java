package software.spool.ingester.internal.control;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.exception.SpoolException;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.failure.ItemQuarantined;
import software.spool.core.model.vo.*;
import software.spool.core.port.bus.BrokerMessage;
import software.spool.core.port.bus.Destination;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.inbox.InboxEnvelopeResolver;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.QuarantineStore;
import software.spool.ingester.api.port.QuarantinedRecord;
import software.spool.ingester.internal.model.EnvelopeStoredContext;
import software.spool.validator.api.ValidationResult;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class EnvelopeStoredHandler implements Handler<Collection<EnvelopeStored>> {
    private final DataLakeWriter dataLakeWriter;
    private final InboxEnvelopeResolver reader;
    private final InboxUpdater updater;
    private final EventPublisher publisher;
    private final ItemValidator validator;
    private final QuarantineStore quarantineStore;
    private final ErrorRouter errorRouter;

    public EnvelopeStoredHandler(DataLakeWriter dataLakeWriter,
                                InboxEnvelopeResolver reader,
                                InboxUpdater updater,
                                EventPublisher publisher,
                                ItemValidator validator,
                                QuarantineStore quarantineStore,
                                ErrorRouter errorRouter) {
        this.dataLakeWriter = dataLakeWriter;
        this.reader = reader;
        this.updater = updater;
        this.publisher = publisher;
        this.validator = validator;
        this.quarantineStore = quarantineStore;
        this.errorRouter = errorRouter;
    }

    @Override
    public void handle(Collection<EnvelopeStored> items) throws SpoolException {
        try {
            List<EnvelopeStoredContext> valid = partition(resolve(items));
            Set<IdempotencyKey> written = dataLakeWriter.write(
                    valid.stream().map(EnvelopeStoredContext::envelope).toList()).collect(Collectors.toSet());
            valid.stream()
                    .filter(item -> written.contains(item.idempotencyKey()))
                    .forEach(this::updateAndEmit);
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }

    private Collection<EnvelopeStoredContext> resolve(Collection<EnvelopeStored> storedEnvelopes) {
        return reader.findByIds(storedEnvelopes.stream().map(EnvelopeStored::idempotencyKey).toList()).stream()
                .map(e -> new EnvelopeStoredContext(getEnvelopeStoredWith(e.idempotencyKey(), storedEnvelopes), e))
                .toList();
    }

    private EnvelopeStored getEnvelopeStoredWith(IdempotencyKey idempotencyKey, Collection<EnvelopeStored> storedEnvelopes) {
        return storedEnvelopes.stream()
                .filter(e -> e.idempotencyKey().equals(idempotencyKey))
                .findFirst()
                .orElse(null);
    }

    private List<EnvelopeStoredContext> partition(Collection<EnvelopeStoredContext> envelopes) {
        List<EnvelopeStoredContext> valid = new ArrayList<>();
        envelopes.forEach(e -> {
            ValidationResult result = validator.validate(e.envelope());
            if (result.isQuarantine()) quarantine(e, result);
            else valid.add(e);
        });
        return valid;
    }

    private void updateAndEmit(EnvelopeStoredContext envelopeStoredContext) {
        try {
            updater.update(envelopeStoredContext.idempotencyKey(), EnvelopeStatus.PERSISTED);
            EnvelopePersisted event = EnvelopePersisted.builder()
                    .from(envelopeStoredContext.event())
                    .partitionKey(PartitionKey.of(getPartitionKeySchema(envelopeStoredContext.envelope())).from(envelopeStoredContext.payload()))
                    .build();
            publisher.publish(new Destination("spool." + event.getClass().getSimpleName()),
                    new BrokerMessage<>(event, event.getClass().getSimpleName(), Map.of()));
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }

    private static PartitionKeySchema getPartitionKeySchema(Envelope envelope) {
        return PayloadDeserializerFactory.json().as(PartitionKeySchema.class)
                .deserialize(envelope.metadata().get(EventMetadataKey.PARTITION_SCHEMA));
    }

    private void quarantine(EnvelopeStoredContext envelopeStoredContext, ValidationResult result) {
        try {
            List<String> violations = result.getViolations().stream()
                    .map(Throwable::getMessage)
                    .toList();
            quarantineStore.send(new QuarantinedRecord(
                    envelopeStoredContext.payload(),
                    violations,
                    Instant.now()
            ));
            ItemQuarantined event = ItemQuarantined.builder()
                    .from(envelopeStoredContext.event())
                    .violations(violations)
                    .build();
            publisher.publish(new Destination("spool." + event.getClass().getSimpleName()),
                    new BrokerMessage<>(event, event.getClass().getSimpleName(), Map.of()));
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }
}