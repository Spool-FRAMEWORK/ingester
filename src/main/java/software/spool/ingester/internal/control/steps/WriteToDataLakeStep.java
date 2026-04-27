package software.spool.ingester.internal.control.steps;

import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.internal.model.EnvelopeStoredContext;

import javax.management.AttributeNotFoundException;
import java.util.Set;
import java.util.stream.Collectors;

public class WriteToDataLakeStep implements Step<PipelineContext, PipelineContext> {

    private final DataLakeWriter dataLakeWriter;

    public WriteToDataLakeStep(DataLakeWriter dataLakeWriter) {
        this.dataLakeWriter = dataLakeWriter;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        Set<IdempotencyKey> written = dataLakeWriter
                .write(ctx.require(EnvelopePipelineKeys.VALID_CONTEXTS).stream().map(EnvelopeStoredContext::envelope).toList())
                .collect(Collectors.toSet());
        return ctx.with(EnvelopePipelineKeys.WRITTEN_KEYS, written);
    }
}