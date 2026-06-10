package software.spool.ingester.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.*;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.port.inbox.InboxEnvelopeResolver;
import software.spool.ingester.internal.model.EnvelopeStoredContext;

import javax.management.AttributeNotFoundException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ResolveEnvelopesStepTest {

    @Test
    void apply_storedEvents_resolvedWithMatchingEnvelopes() throws AttributeNotFoundException {
        IdempotencyKey key = IdempotencyKey.of("k1");
        EnvelopeStored stored = EnvelopeStored.builder().idempotencyKey(key).build();
        Envelope envelope = anyEnvelope(key);
        InboxEnvelopeResolver resolver = new InboxEnvelopeResolver() {
            @Override public Optional<Envelope> findById(IdempotencyKey k) { return Optional.empty(); }
            @Override public Collection<Envelope> findByIds(Collection<IdempotencyKey> keys) { return List.of(envelope); }
        };

        PipelineContext ctx = PipelineContext.empty().with(EnvelopePipelineKeys.STORED_EVENTS, List.of(stored));
        PipelineContext result = new ResolveEnvelopesStep(resolver).apply(ctx);

        Collection<EnvelopeStoredContext> resolved = result.require(EnvelopePipelineKeys.RESOLVED_CONTEXTS);
        assertThat(resolved).hasSize(1);
    }

    @Test
    void apply_resolverReturnsEmpty_resolvedContextsEmpty() throws AttributeNotFoundException {
        EnvelopeStored stored = EnvelopeStored.builder().idempotencyKey(IdempotencyKey.of("k")).build();
        InboxEnvelopeResolver resolver = new InboxEnvelopeResolver() {
            @Override public Optional<Envelope> findById(IdempotencyKey k) { return Optional.empty(); }
            @Override public Collection<Envelope> findByIds(Collection<IdempotencyKey> keys) { return List.of(); }
        };

        PipelineContext ctx = PipelineContext.empty().with(EnvelopePipelineKeys.STORED_EVENTS, List.of(stored));
        PipelineContext result = new ResolveEnvelopesStep(resolver).apply(ctx);

        Collection<EnvelopeStoredContext> resolved = result.require(EnvelopePipelineKeys.RESOLVED_CONTEXTS);
        assertThat(resolved).isEmpty();
    }

    private static Envelope anyEnvelope(IdempotencyKey key) {
        return new Envelope(key, new EventMetadata(), MediaType.of("application/json"), "{}".getBytes(), EnvelopeStatus.CAPTURED, 0, Instant.now(), null);
    }
}
