package software.spool.ingester.internal.control.steps;

import software.spool.core.model.failure.ItemQuarantined;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.bus.BrokerMessage;
import software.spool.core.port.bus.Destination;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.api.port.QuarantineStore;
import software.spool.ingester.api.port.QuarantinedRecord;

import javax.management.AttributeNotFoundException;
import java.time.Instant;
import java.util.Map;

public class QuarantineStep implements Step<PipelineContext, PipelineContext> {

    private final QuarantineStore quarantineStore;
    private final EventPublisher publisher;
    private final ErrorRouter errorRouter;

    public QuarantineStep(QuarantineStore quarantineStore, EventPublisher publisher, ErrorRouter errorRouter) {
        this.quarantineStore = quarantineStore;
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
            quarantineStore.send(new QuarantinedRecord(candidate.context().payload(), candidate.violations(), Instant.now()));
            ItemQuarantined event = ItemQuarantined.builder()
                    .from(candidate.context().event())
                    .violations(candidate.violations())
                    .build();
            publisher.publish(
                    new Destination("spool." + event.getClass().getSimpleName()),
                    new BrokerMessage<>(event, event.getClass().getSimpleName(), Map.of())
            );
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }
}