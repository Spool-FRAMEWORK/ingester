package software.spool.ingester.internal.control.steps;

import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.inbox.InboxEnvelopeResolver;
import software.spool.ingester.internal.model.EnvelopeStoredContext;

import javax.management.AttributeNotFoundException;
import java.util.Collection;

public class ResolveEnvelopesStep implements Step<PipelineContext, PipelineContext> {

    private final InboxEnvelopeResolver reader;

    public ResolveEnvelopesStep(InboxEnvelopeResolver reader) {
        this.reader = reader;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        Collection<EnvelopeStored> events = ctx.require(EnvelopePipelineKeys.STORED_EVENTS);
        Collection<EnvelopeStoredContext> resolved = reader
                .findByIds(events.stream().map(EnvelopeStored::idempotencyKey).toList())
                .stream()
                .map(envelope -> new EnvelopeStoredContext(findBy(envelope.idempotencyKey(), events), envelope))
                .toList();
        return ctx.with(EnvelopePipelineKeys.RESOLVED_CONTEXTS, resolved);
    }

    private EnvelopeStored findBy(IdempotencyKey key, Collection<EnvelopeStored> events) {
        return events.stream().filter(e -> e.idempotencyKey().equals(key)).findFirst().orElse(null);
    }
}