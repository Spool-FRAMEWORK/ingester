package software.spool.ingester.internal.control.steps;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.vo.*;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.internal.model.EnvelopeStoredContext;

import javax.management.AttributeNotFoundException;
import java.util.Set;
import java.util.stream.Collectors;

public class PersistAndEmitStep implements Step<PipelineContext, PipelineContext> {
    private final DataLakeWriter dataLakeWriter;
    private final EventPublisher publisher;
    private final ErrorRouter errorRouter;

    public PersistAndEmitStep(DataLakeWriter dataLakeWriter, EventPublisher publisher, ErrorRouter errorRouter) {
        this.dataLakeWriter = dataLakeWriter;
        this.publisher = publisher;
        this.errorRouter = errorRouter;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        Set<IdempotencyKey> written = dataLakeWriter
                .write(ctx.require(EnvelopePipelineKeys.VALID_CONTEXTS).stream().map(EnvelopeStoredContext::envelope).toList())
                .collect(Collectors.toSet());
        ctx.require(EnvelopePipelineKeys.VALID_CONTEXTS).stream()
                .filter(e -> written.contains(e.idempotencyKey()))
                .forEach(this::updateAndEmit);
        return ctx.with(EnvelopePipelineKeys.WRITTEN_KEYS, written);
    }

    private void updateAndEmit(EnvelopeStoredContext e) {
        try {
            EnvelopePersisted event = EnvelopePersisted.builder()
                    .from(e.event())
                    .partitionKey(PartitionKey.of(partitionKeySchema(e.envelope())).from(e.payload()))
                    .build();
            publisher.publish(event);
        } catch (Exception ex) {
            errorRouter.dispatch(ex);
        }
    }

    private static PartitionKeySchema partitionKeySchema(Envelope envelope) {
        return PayloadDeserializerFactory.json().as(PartitionKeySchema.class)
                .deserialize(envelope.metadata().get(EventMetadataKey.PARTITION_SCHEMA));
    }
}