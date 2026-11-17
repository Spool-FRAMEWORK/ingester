package software.spool.ingester.internal.control.steps;

import org.junit.jupiter.api.Test;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.pipeline.PipelineContext;
import software.spool.core.port.logging.Logger;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.ingester.fixture.NonZeroAmountWarningValidator;
import software.spool.ingester.fixture.PositiveAmountValidator;
import software.spool.ingester.internal.model.EnvelopeStoredContext;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.engine.ValidatorRegistry;

import javax.management.AttributeNotFoundException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionEnvelopesStepTest {

    @Test
    void apply_envelopeThatFailsACriticalValidator_countsItAsRejectedByTheIngester() throws AttributeNotFoundException {
        List<Map<String, String>> rejectedAttributes = new ArrayList<>();
        PartitionEnvelopesStep step = new PartitionEnvelopesStep(
                new EnvelopeValidator(new ValidatorRegistry(List.of(new PositiveAmountValidator()))),
                (value, attributes) -> rejectedAttributes.add(attributes),
                (value, attributes) -> {},
                Logger.NOOP);

        PipelineContext result = step.apply(PipelineContext.empty()
                .with(EnvelopePipelineKeys.RESOLVED_CONTEXTS, List.of(context("{\"amount\":-1}"))));

        assertThat(result.require(EnvelopePipelineKeys.QUARANTINE_CANDIDATES)).hasSize(1);
        assertThat(rejectedAttributes).singleElement()
                .satisfies(attributes -> assertThat(attributes).containsEntry(SpoolMetrics.Attributes.MODULE, "ingester"));
    }

    @Test
    void apply_envelopeThatFailsAWarningValidator_keepsItValidAndRecordsTheViolation() throws AttributeNotFoundException {
        List<Long> warningCounts = new ArrayList<>();
        List<Map<String, String>> warningAttributes = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        PartitionEnvelopesStep step = new PartitionEnvelopesStep(
                new EnvelopeValidator(new ValidatorRegistry(List.of(new NonZeroAmountWarningValidator()))),
                (value, attributes) -> {},
                (value, attributes) -> {
                    warningCounts.add(value);
                    warningAttributes.add(attributes);
                },
                warningsInto(warnings));

        PipelineContext result = step.apply(PipelineContext.empty()
                .with(EnvelopePipelineKeys.RESOLVED_CONTEXTS, List.of(context("{\"amount\":0}"))));

        assertThat(result.require(EnvelopePipelineKeys.VALID_CONTEXTS)).hasSize(1);
        assertThat(result.require(EnvelopePipelineKeys.QUARANTINE_CANDIDATES)).isEmpty();
        assertThat(warnings).singleElement().satisfies(warning -> assertThat(warning)
                .contains("k1")
                .contains("amount should not be zero"));
        assertThat(warningCounts).containsExactly(1L);
        assertThat(warningAttributes).singleElement()
                .satisfies(attributes -> assertThat(attributes).containsEntry(SpoolMetrics.Attributes.MODULE, "ingester"));
    }

    private static Logger warningsInto(List<String> warnings) {
        return new Logger() {
            public void debug(String message) {}
            public void debug(String format, Object... args) {}
            public void info(String message) {}
            public void info(String format, Object... args) {}
            public void warn(String message) { warnings.add(message); }
            public void warn(String format, Object... args) { warnings.add(format + " " + Arrays.toString(args)); }
            public void warn(String message, Throwable cause) { warnings.add(message); }
            public void error(String message) {}
            public void error(String format, Object... args) {}
            public void error(String message, Throwable cause) {}
        };
    }

    private static EnvelopeStoredContext context(String payload) {
        IdempotencyKey key = IdempotencyKey.of("k1");
        Envelope envelope = new Envelope(key, new EventMetadata().set(EventMetadataKey.SOURCE, "payments"),
                MediaType.of("application/json"), payload.getBytes(StandardCharsets.UTF_8), EnvelopeStatus.CAPTURED, 0,
                Instant.now(), Instant.now());
        return new EnvelopeStoredContext(EnvelopeStored.builder().idempotencyKey(key).build(), envelope);
    }
}
