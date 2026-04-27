package software.spool.ingester.internal.control.steps;

import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.pipeline.ContextKey;
import software.spool.ingester.internal.model.EnvelopeStoredContext;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public final class EnvelopePipelineKeys {
    public static final ContextKey<Collection<EnvelopeStored>> STORED_EVENTS = ContextKey.of("storedEvents");
    static final ContextKey<Collection<EnvelopeStoredContext>> RESOLVED_CONTEXTS = ContextKey.of("resolvedContexts");
    static final ContextKey<List<EnvelopeStoredContext>> VALID_CONTEXTS = ContextKey.of("validContexts");
    static final ContextKey<List<QuarantineCandidate>> QUARANTINE_CANDIDATES = ContextKey.of("quarantineCandidates");
    static final ContextKey<Set<IdempotencyKey>> WRITTEN_KEYS = ContextKey.of("writtenKeys");

    private EnvelopePipelineKeys() {}
}