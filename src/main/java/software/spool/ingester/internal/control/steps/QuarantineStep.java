package software.spool.ingester.internal.control.steps;

import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;

import javax.management.AttributeNotFoundException;

public class QuarantineStep implements Step<PipelineContext, PipelineContext> {

    private final EventPublisher publisher;
    private final ErrorRouter errorRouter;

    public QuarantineStep(EventPublisher publisher, ErrorRouter errorRouter) {
        this.publisher = publisher;
        this.errorRouter = errorRouter;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        ctx.require(EnvelopePipelineKeys.QUARANTINE_CANDIDATES).forEach(this::process);
        return ctx;
    }

    private void process(QuarantineCandidate candidate) {
        try {
            publisher.publish(EnvelopeQuarantined.builder()
                    .from(candidate.context().event())
                    .idempotencyKey(candidate.context().idempotencyKey())
                    .reason(QuarantineReason.VALIDATION_FAILED)
                    .violations(candidate.violations())
                    .build());
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }
}
