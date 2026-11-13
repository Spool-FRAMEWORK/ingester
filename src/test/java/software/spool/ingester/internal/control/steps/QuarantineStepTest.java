package software.spool.ingester.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.Event;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.vo.*;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.internal.model.EnvelopeStoredContext;

import javax.management.AttributeNotFoundException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QuarantineStepTest {

    private final List<Event> published = new ArrayList<>();
    private final QuarantineStep step = new QuarantineStep(new EventPublisher() {
        @Override public <E extends Event> void publish(E event) {
            published.add(event);
        }
    }, new ErrorRouter());

    @Test
    void apply_candidateThatFailedAValidation_publishesEnvelopeQuarantinedWithItsKeyReasonAndViolations() throws AttributeNotFoundException {
        QuarantineCandidate candidate = new QuarantineCandidate(anyContext(), List.of("price must be positive"));

        step.apply(PipelineContext.empty().with(EnvelopePipelineKeys.QUARANTINE_CANDIDATES, List.of(candidate)));

        assertThat(published).singleElement().isInstanceOfSatisfying(EnvelopeQuarantined.class, event -> {
            assertThat(event.idempotencyKey()).isEqualTo(IdempotencyKey.of("test-key"));
            assertThat(event.reason()).isEqualTo(QuarantineReason.VALIDATION_FAILED);
            assertThat(event.violations()).containsExactly("price must be positive");
        });
    }

    @Test
    void apply_emptyCandidates_publishesNothing() throws AttributeNotFoundException {
        step.apply(PipelineContext.empty().with(EnvelopePipelineKeys.QUARANTINE_CANDIDATES, List.of()));

        assertThat(published).isEmpty();
    }

    private static EnvelopeStoredContext anyContext() {
        IdempotencyKey key = IdempotencyKey.of("test-key");
        EnvelopeStored event = EnvelopeStored.builder().idempotencyKey(key).build();
        Envelope envelope = new Envelope(key, new EventMetadata(), MediaType.of("application/json"), "{}".getBytes(), EnvelopeStatus.CAPTURED, 0, Instant.now(), null);
        return new EnvelopeStoredContext(event, envelope);
    }
}
