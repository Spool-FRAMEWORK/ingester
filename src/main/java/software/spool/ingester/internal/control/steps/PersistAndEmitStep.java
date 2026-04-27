package software.spool.ingester.internal.control.steps;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.vo.*;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.bus.BrokerMessage;
import software.spool.core.port.bus.Destination;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.internal.model.EnvelopeStoredContext;

import javax.management.AttributeNotFoundException;
import java.util.Map;
import java.util.Set;

public class PersistAndEmitStep implements Step<PipelineContext, PipelineContext> {

    private final InboxUpdater updater;
    private final EventPublisher publisher;
    private final ErrorRouter errorRouter;

    public PersistAndEmitStep(InboxUpdater updater, EventPublisher publisher, ErrorRouter errorRouter) {
        this.updater = updater;
        this.publisher = publisher;
        this.errorRouter = errorRouter;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        Set<IdempotencyKey> written = ctx.require(EnvelopePipelineKeys.WRITTEN_KEYS);
        ctx.require(EnvelopePipelineKeys.VALID_CONTEXTS).stream()
                .filter(e -> written.contains(e.idempotencyKey()))
                .forEach(this::updateAndEmit);
        return ctx;
    }

    private void updateAndEmit(EnvelopeStoredContext e) {
        try {
            updater.update(e.idempotencyKey(), EnvelopeStatus.PERSISTED);
            EnvelopePersisted event = EnvelopePersisted.builder()
                    .from(e.event())
                    .partitionKey(PartitionKey.of(partitionKeySchema(e.envelope())).from(e.payload()))
                    .build();
            publisher.publish(
                    new Destination("spool." + event.getClass().getSimpleName()),
                    new BrokerMessage<>(event, event.getClass().getSimpleName(), Map.of())
            );
        } catch (Exception ex) {
            errorRouter.dispatch(ex);
        }
    }

    private static PartitionKeySchema partitionKeySchema(Envelope envelope) {
        return PayloadDeserializerFactory.json().as(PartitionKeySchema.class)
                .deserialize(envelope.metadata().get(EventMetadataKey.PARTITION_SCHEMA));
    }
}