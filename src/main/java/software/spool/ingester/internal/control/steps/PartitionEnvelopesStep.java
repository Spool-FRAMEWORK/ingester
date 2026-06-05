package software.spool.ingester.internal.control.steps;

import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.ingester.internal.control.ItemValidator;
import software.spool.ingester.internal.model.EnvelopeStoredContext;
import software.spool.validator.api.ValidationResult;

import javax.management.AttributeNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PartitionEnvelopesStep implements Step<PipelineContext, PipelineContext> {

    private final ItemValidator validator;
    private final MetricsRegistry.CounterMetric recordsRejected;

    public PartitionEnvelopesStep(ItemValidator validator, MetricsRegistry.CounterMetric recordsRejected) {
        this.validator = validator;
        this.recordsRejected = recordsRejected;
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

        if (!quarantine.isEmpty()) {
            recordsRejected.add(quarantine.size(), Map.of(SpoolMetrics.Attributes.REASON, "invalid"));
        }
        return ctx.with(EnvelopePipelineKeys.VALID_CONTEXTS, valid).with(EnvelopePipelineKeys.QUARANTINE_CANDIDATES, quarantine);
    }
}