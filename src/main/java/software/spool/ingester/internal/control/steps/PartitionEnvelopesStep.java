package software.spool.ingester.internal.control.steps;

import software.spool.core.pipeline.PipelineContext;
import software.spool.core.pipeline.Step;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.ingester.internal.model.EnvelopeStoredContext;
import software.spool.validator.api.ValidationResult;

import javax.management.AttributeNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PartitionEnvelopesStep implements Step<PipelineContext, PipelineContext> {
    private static final String MODULE_NAME = "ingester";

    private final EnvelopeValidator validator;
    private final MetricsRegistry.CounterMetric recordsRejected;
    private final MetricsRegistry.CounterMetric validationWarnings;
    private final Logger logger;

    public PartitionEnvelopesStep(EnvelopeValidator validator, MetricsRegistry.CounterMetric recordsRejected,
                                  MetricsRegistry.CounterMetric validationWarnings, Logger logger) {
        this.validator = validator;
        this.recordsRejected = recordsRejected;
        this.validationWarnings = validationWarnings;
        this.logger = logger;
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
                recordWarnings(e, result);
                valid.add(e);
            }
        });

        if (!quarantine.isEmpty()) {
            recordsRejected.add(quarantine.size(), Map.of(SpoolMetrics.Attributes.REASON, "invalid", SpoolMetrics.Attributes.MODULE, MODULE_NAME));
        }
        return ctx.with(EnvelopePipelineKeys.VALID_CONTEXTS, valid).with(EnvelopePipelineKeys.QUARANTINE_CANDIDATES, quarantine);
    }

    private void recordWarnings(EnvelopeStoredContext context, ValidationResult result) {
        if (!result.hasWarnings()) return;
        result.getViolations().forEach(violation -> logger.warn("Envelope {} passed validation with a warning: {}",
                context.idempotencyKey().value(), violation.getMessage()));
        validationWarnings.add(result.getViolations().size(), Map.of(SpoolMetrics.Attributes.MODULE, MODULE_NAME));
    }
}