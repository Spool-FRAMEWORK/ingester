package software.spool.ingester.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.Event;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.*;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.api.port.QuarantinedRecord;
import software.spool.ingester.internal.model.EnvelopeStoredContext;

import javax.management.AttributeNotFoundException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QuarantineStepTest {

    @Test
    void apply_candidates_sendsToQuarantineStore() throws AttributeNotFoundException {
        List<QuarantinedRecord> received = new ArrayList<>();
        QuarantineStep step = new QuarantineStep(received::add, new EventPublisher() {
            @Override public <E extends Event> void publish(E event) {}
        }, new ErrorRouter());
        QuarantineCandidate candidate = new QuarantineCandidate(anyContext(), List.of("violation"));
        PipelineContext ctx = PipelineContext.empty()
            .with(EnvelopePipelineKeys.QUARANTINE_CANDIDATES, List.of(candidate));

        step.apply(ctx);

        assertThat(received).hasSize(1);
    }

    @Test
    void apply_emptyCandidates_storeNeverCalled() throws AttributeNotFoundException {
        List<QuarantinedRecord> received = new ArrayList<>();
        QuarantineStep step = new QuarantineStep(received::add, new EventPublisher() {
            @Override public <E extends Event> void publish(E event) {}
        }, new ErrorRouter());
        PipelineContext ctx = PipelineContext.empty()
            .with(EnvelopePipelineKeys.QUARANTINE_CANDIDATES, List.of());

        step.apply(ctx);

        assertThat(received).isEmpty();
    }

    private static EnvelopeStoredContext anyContext() {
        IdempotencyKey key = IdempotencyKey.of("test-key");
        EnvelopeStored event = EnvelopeStored.builder().idempotencyKey(key).build();
        Envelope envelope = new Envelope(key, new EventMetadata(), MediaType.of("application/json"), "{}".getBytes(), EnvelopeStatus.CAPTURED, 0, Instant.now(), null);
        return new EnvelopeStoredContext(event, envelope);
    }
}
