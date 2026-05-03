package software.spool.ingester.internal.control;

import software.spool.core.exception.SpoolException;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.pipeline.ObservedStep;
import software.spool.core.pipeline.Pipeline;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.Handler;
import software.spool.core.port.inbox.InboxEnvelopeResolver;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.QuarantineStore;
import software.spool.ingester.internal.control.steps.*;

import java.util.Collection;

public class EnvelopeStoredHandler implements Handler<Collection<EnvelopeStored>> {
    private final Pipeline<PipelineContext, PipelineContext> pipeline;
    private final ErrorRouter errorRouter;

    public EnvelopeStoredHandler(DataLakeWriter dataLakeWriter,
                                 InboxEnvelopeResolver reader,
                                 EventPublisher publisher,
                                 ItemValidator validator,
                                 QuarantineStore quarantineStore,
                                 ErrorRouter errorRouter) {
        this.errorRouter = errorRouter;
        this.pipeline = Pipeline.<PipelineContext>start()
                .add(new ObservedStep<>("resolve-envelopes", new ResolveEnvelopesStep(reader)))
                .add(new ObservedStep<>("partition-envelopes", new PartitionEnvelopesStep(validator)))
                .add(new ObservedStep<>("quarantine", new QuarantineStep(quarantineStore, publisher, errorRouter)))
                .add(new ObservedStep<>("persist-and-emit", new PersistAndEmitStep(dataLakeWriter, publisher, errorRouter)));
    }

    @Override
    public void handle(Collection<EnvelopeStored> items) throws SpoolException {
        pipeline.execute(PipelineContext.empty().with(EnvelopePipelineKeys.STORED_EVENTS, items))
                .peekError(errorRouter::dispatch);
    }
}