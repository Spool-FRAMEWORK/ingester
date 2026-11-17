package software.spool.ingester.internal.control.steps;

import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.vo.*;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.PartitionedEnvelope;
import software.spool.ingester.internal.model.EnvelopeStoredContext;
import software.spool.ingester.internal.utils.EnvelopePartitioner;

import javax.management.AttributeNotFoundException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PersistAndEmitStep implements Step<PipelineContext, PipelineContext> {
    private final DataLakeWriter dataLakeWriter;
    private final EventPublisher publisher;
    private final ErrorRouter errorRouter;
    private final EnvelopePartitioner partitioner;

    public PersistAndEmitStep(DataLakeWriter dataLakeWriter, EventPublisher publisher, ErrorRouter errorRouter,
                              EnvelopePartitioner partitioner) {
        this.dataLakeWriter = dataLakeWriter;
        this.publisher = publisher;
        this.errorRouter = errorRouter;
        this.partitioner = partitioner;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        List<PartitionedContext> partitioned = ctx.require(EnvelopePipelineKeys.VALID_CONTEXTS).stream()
                .flatMap(this::partition)
                .toList();
        Set<IdempotencyKey> written = dataLakeWriter
                .write(partitioned.stream().map(PartitionedContext::toPartitionedEnvelope).toList())
                .collect(Collectors.toSet());
        partitioned.stream()
                .filter(e -> written.contains(e.context().idempotencyKey()))
                .forEach(this::updateAndEmit);
        return ctx.with(EnvelopePipelineKeys.WRITTEN_KEYS, written);
    }

    private Stream<PartitionedContext> partition(EnvelopeStoredContext context) {
        try {
            return Stream.of(new PartitionedContext(context, partitioner.partitionOf(context.envelope())));
        } catch (Exception ex) {
            errorRouter.dispatch(ex);
            return Stream.empty();
        }
    }

    private void updateAndEmit(PartitionedContext e) {
        try {
            EnvelopePersisted event = EnvelopePersisted.builder()
                    .from(e.context().event())
                    .idempotencyKey(e.context().idempotencyKey())
                    .partitionKey(e.partition())
                    .build();
            publisher.publish(event);
        } catch (Exception ex) {
            errorRouter.dispatch(ex);
        }
    }

    private record PartitionedContext(EnvelopeStoredContext context, PartitionKey partition) {
        PartitionedEnvelope toPartitionedEnvelope() {
            return new PartitionedEnvelope(partition, context.envelope());
        }
    }
}
