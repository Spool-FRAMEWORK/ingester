package software.spool.ingester.acceptance;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.core.adapter.memory.InMemoryInbox;
import software.spool.core.adapter.memory.InMemoryProcessedKeyRepository;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.failure.EnvelopeQuarantined;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.ingester.api.builder.IngesterRounds;
import software.spool.ingester.fixture.MovableClock;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessedKeySteps {
    private static final String PARTITION_SCHEMA = "{\"sourceId\":\"payments\",\"attributes\":[]}";
    private final InMemoryInbox inbox = new InMemoryInbox();
    private final MovableClock clock = new MovableClock();
    private IngesterRounds ingester;

    @Given("an ingester that remembers the keys it wrote for {int} minutes")
    public void anIngesterThatRemembers(int minutes) {
        ingester = new IngesterRounds(inbox, new InMemoryProcessedKeyRepository(Duration.ofMinutes(minutes), clock), clock);
    }

    @Given("the envelope {string} is captured in the inbox")
    public void theEnvelopeIsCaptured(String key) {
        inbox.update(captured(key, "{\"amount\":10}"));
    }

    @Given("the envelope {string} is captured in the inbox with a payload that fails validation")
    public void theEnvelopeIsCapturedWithAnInvalidPayload(String key) {
        inbox.update(captured(key, "{\"amount\":-1}"));
    }

    @When("the ingester is told that {string} was stored")
    public void theIngesterIsToldItWasStored(String key) {
        ingester.announce(EnvelopeStored.builder().idempotencyKey(IdempotencyKey.of(key)).build());
    }

    @When("the ingester writes its batch")
    public void theIngesterWritesItsBatch() {
        ingester.writeBatch();
    }

    @When("{int} minutes pass")
    public void minutesPass(int minutes) {
        clock.advance(Duration.ofMinutes(minutes));
    }

    @Then("{string} was written to the data lake once")
    public void wasWrittenOnce(String key) {
        wasWrittenTimes(key, 1);
    }

    @Then("{string} was written to the data lake {int} times")
    public void wasWrittenTimes(String key, int times) {
        assertThat(ingester.written()).filteredOn(item -> item.envelope().idempotencyKey().equals(IdempotencyKey.of(key)))
                .hasSize(times);
    }

    @Then("{string} was not written to the data lake")
    public void wasNotWritten(String key) {
        wasWrittenTimes(key, 0);
    }

    @Then("EnvelopePersisted was published {int} time(s) for {string}")
    public void envelopePersistedWasPublished(int times, String key) {
        assertThat(ingester.published()).filteredOn(EnvelopePersisted.class::isInstance)
                .map(EnvelopePersisted.class::cast)
                .filteredOn(event -> event.idempotencyKey().equals(IdempotencyKey.of(key)))
                .hasSize(times);
    }

    @Then("{string} was sent to quarantine {int} times")
    public void wasSentToQuarantine(String key, int times) {
        assertThat(ingester.published()).filteredOn(EnvelopeQuarantined.class::isInstance)
                .map(EnvelopeQuarantined.class::cast)
                .filteredOn(event -> event.idempotencyKey().equals(IdempotencyKey.of(key)))
                .hasSize(times);
    }

    private Envelope captured(String key, String payload) {
        EventMetadata metadata = new EventMetadata()
                .set(EventMetadataKey.SOURCE, "payments")
                .set(EventMetadataKey.PARTITION_SCHEMA, PARTITION_SCHEMA);
        return new Envelope(IdempotencyKey.of(key), metadata, MediaType.of("application/json"),
                payload.getBytes(StandardCharsets.UTF_8), EnvelopeStatus.CAPTURED, 0, clock.instant(), clock.instant());
    }
}
