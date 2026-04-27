package software.spool.ingester.internal.control.steps;

import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.ingester.internal.control.ItemValidator;
import software.spool.ingester.internal.model.EnvelopeStoredContext;
import software.spool.validator.api.ValidationResult;

import javax.management.AttributeNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PartitionEnvelopesStep implements Step<PipelineContext, PipelineContext> {

    private final ItemValidator validator;

    public PartitionEnvelopesStep(ItemValidator validator) {
        this.validator = validator;
    }

    @Override
    public PipelineContext apply(PipelineContext ctx) throws AttributeNotFoundException {
        Collection<EnvelopeStoredContext> resolved = ctx.require(EnvelopePipelineKeys.RESOLVED_CONTEXTS);
        List<EnvelopeStoredContext> valid = new ArrayList<>();
        List<QuarantineCandidate> quarantine = new ArrayList<>();

        resolved.forEach(e -> {
            ValidationResult result = validator.validate(e.envelope());
            if (result.isQuarantine()) {
                quarantine.add(new QuarantineCandidate(e, result.getViolations().stream().map(Throwable::getMessage).toList()));
            } else {
                valid.add(e);
            }
        });

        return ctx.with(EnvelopePipelineKeys.VALID_CONTEXTS, valid).with(EnvelopePipelineKeys.QUARANTINE_CANDIDATES, quarantine);
    }
}