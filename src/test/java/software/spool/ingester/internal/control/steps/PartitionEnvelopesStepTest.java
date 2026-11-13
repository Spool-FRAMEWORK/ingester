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
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.ingester.fixture.PositiveAmountValidator;
import software.spool.ingester.internal.model.EnvelopeStoredContext;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.engine.ValidatorRegistry;

import javax.management.AttributeNotFoundException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionEnvelopesStepTest {

    @Test
    void apply_envelopeThatFailsACriticalValidator_countsItAsRejectedByTheIngester() throws AttributeNotFoundException {
        List<Map<String, String>> rejectedAttributes = new ArrayList<>();
        PartitionEnvelopesStep step = new PartitionEnvelopesStep(
                new EnvelopeValidator(new ValidatorRegistry(List.of(new PositiveAmountValidator()))),
                (value, attributes) -> rejectedAttributes.add(attributes));

        PipelineContext result = step.apply(PipelineContext.empty()
                .with(EnvelopePipelineKeys.RESOLVED_CONTEXTS, List.of(context("{\"amount\":-1}"))));

        assertThat(result.require(EnvelopePipelineKeys.QUARANTINE_CANDIDATES)).hasSize(1);
        assertThat(rejectedAttributes).singleElement()
                .satisfies(attributes -> assertThat(attributes).containsEntry(SpoolMetrics.Attributes.MODULE, "ingester"));
    }

    private static EnvelopeStoredContext context(String payload) {
        IdempotencyKey key = IdempotencyKey.of("k1");
        Envelope envelope = new Envelope(key, new EventMetadata().set(EventMetadataKey.SOURCE, "payments"),
                MediaType.of("application/json"), payload.getBytes(StandardCharsets.UTF_8), EnvelopeStatus.CAPTURED, 0,
                Instant.now(), Instant.now());
        return new EnvelopeStoredContext(EnvelopeStored.builder().idempotencyKey(key).build(), envelope);
    }
}
