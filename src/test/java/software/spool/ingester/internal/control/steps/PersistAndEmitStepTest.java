package software.spool.ingester.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.Event;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.*;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.ingester.internal.model.EnvelopeStoredContext;
import software.spool.ingester.internal.utils.EnvelopePartitioner;

import javax.management.AttributeNotFoundException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PersistAndEmitStepTest {

    private static final PartitionKey BEFORE_HOUR_CHANGE = new PartitionKey("year=2026/month=11/day=17/hour=10");
    private static final PartitionKey AFTER_HOUR_CHANGE = new PartitionKey("year=2026/month=11/day=17/hour=11");

    private final List<PartitionedEnvelope> writtenToBronze = new ArrayList<>();
    private final List<Event> published = new ArrayList<>();
    private final List<Exception> routedErrors = new ArrayList<>();

    @Test
    void apply_hourChangesBetweenWritingAndEmitting_emitsThePartitionTheEnvelopeWasWrittenTo() throws AttributeNotFoundException {
        PersistAndEmitStep step = stepWith(partitionerWhoseClockMovesTo(BEFORE_HOUR_CHANGE, AFTER_HOUR_CHANGE));

        step.apply(contextWith(storedContext("order-1")));

        assertThat(writtenToBronze).singleElement()
                .extracting(PartitionedEnvelope::partition).isEqualTo(BEFORE_HOUR_CHANGE);
        assertThat(published).singleElement().isInstanceOfSatisfying(EnvelopePersisted.class,
                event -> assertThat(event.partitionKey()).isEqualTo(BEFORE_HOUR_CHANGE));
    }

    @Test
    void apply_partitionCannotBeWorkedOut_routesTheErrorAndWritesTheOtherEnvelopes() throws AttributeNotFoundException {
        IllegalStateException partitionFailure = new IllegalStateException("missing partition attribute");
        EnvelopePartitioner partitioner = envelope -> {
            if (envelope.idempotencyKey().equals(IdempotencyKey.of("broken"))) throw partitionFailure;
            return BEFORE_HOUR_CHANGE;
        };

        stepWith(partitioner).apply(contextWith(storedContext("broken"), storedContext("order-1")));

        assertThat(writtenToBronze).extracting(written -> written.envelope().idempotencyKey())
                .containsExactly(IdempotencyKey.of("order-1"));
        assertThat(routedErrors).containsExactly(partitionFailure);
    }

    private PersistAndEmitStep stepWith(EnvelopePartitioner partitioner) {
        EventPublisher publisher = new EventPublisher() {
            @Override
            public <E extends Event> void publish(E event) {
                published.add(event);
            }
        };
        ErrorRouter errorRouter = new ErrorRouter().orElse((exception, cause) -> routedErrors.add(exception));
        return new PersistAndEmitStep(items -> {
            writtenToBronze.addAll(items);
            return items.stream().map(item -> item.envelope().idempotencyKey());
        }, publisher, errorRouter, partitioner);
    }

    private static EnvelopePartitioner partitionerWhoseClockMovesTo(PartitionKey... successivePartitions) {
        Deque<PartitionKey> remaining = new ArrayDeque<>(List.of(successivePartitions));
        return envelope -> remaining.size() > 1 ? remaining.poll() : remaining.peek();
    }

    private static PipelineContext contextWith(EnvelopeStoredContext... contexts) {
        return PipelineContext.empty().with(EnvelopePipelineKeys.VALID_CONTEXTS, List.of(contexts));
    }

    private static EnvelopeStoredContext storedContext(String key) {
        IdempotencyKey idempotencyKey = IdempotencyKey.of(key);
        EnvelopeStored event = EnvelopeStored.builder().idempotencyKey(idempotencyKey).build();
        Envelope envelope = new Envelope(idempotencyKey, new EventMetadata(), MediaType.of("application/json"),
                "{}".getBytes(), EnvelopeStatus.CAPTURED, 0, Instant.now(), null);
        return new EnvelopeStoredContext(event, envelope);
    }
}
