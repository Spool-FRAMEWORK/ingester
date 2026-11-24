package software.spool.ingester.api.builder;

import software.spool.core.adapter.memory.InMemoryProcessedKeyRepository;
import software.spool.core.adapter.otel.OpenTelemetryMetricsRegistry;
import software.spool.core.port.idempotency.ProcessedKeyRepository;
import software.spool.core.port.idempotency.ProcessedKeyWindow;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.bus.EventSubscriber;
import software.spool.core.port.decorator.SafeEventPublisher;
import software.spool.core.port.decorator.SafeEventSubscriber;
import software.spool.core.port.decorator.SafeInboxEnvelopeResolver;
import software.spool.core.port.inbox.InboxEnvelopeResolver;
import software.spool.core.port.metrics.MetricsRegistry;
import software.spool.core.port.metrics.SpoolMetrics;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.ingester.api.Ingester;
import software.spool.core.port.datalake.DataLakeWriter;
import software.spool.ingester.api.utils.IngesterErrorRouter;
import software.spool.ingester.internal.control.EnvelopeStoredHandler;
import software.spool.ingester.internal.decorator.RememberingDataLakeWriter;
import software.spool.ingester.internal.decorator.SafeDataLakeWriter;
import software.spool.ingester.internal.utils.Buffer;
import software.spool.ingester.internal.utils.FlushCoordinator;
import software.spool.ingester.internal.utils.FlushPolicy;
import software.spool.validator.api.EnvelopeValidator;
import software.spool.validator.engine.ValidatorRegistry;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public class IngesterBuilder {
    private final ModuleHeartBeat heartBeat;
    private EventSubscriber listener;
    private DataLakeWriter writer;
    private InboxEnvelopeResolver reader;
    private EventPublisher publisher;
    private FlushPolicy flushPolicy;
    private PollingConfiguration pollingConfiguration;
    private Clock clock = Clock.systemUTC();
    private Duration processedKeysWindow = ProcessedKeyWindow.DEFAULT_WINDOW;
    private Optional<ProcessedKeyRepository> processedKeys = Optional.empty();

    IngesterBuilder(ModuleHeartBeat heartBeat) {
        this.heartBeat = heartBeat;
        this.pollingConfiguration = PollingConfiguration.every(Duration.ofMillis(100));
    }

    public IngesterBuilder flushPolicy(FlushPolicy flushPolicy) {
        this.flushPolicy = flushPolicy;
        return this;
    }

    public IngesterBuilder from(EventSubscriber listener) {
        this.listener = SafeEventSubscriber.of(listener);
        return this;
    }

    public IngesterBuilder storesWith(DataLakeWriter writer) {
        this.writer = SafeDataLakeWriter.of(writer);
        return this;
    }

    public IngesterBuilder readWith(InboxEnvelopeResolver reader) {
        this.reader = SafeInboxEnvelopeResolver.of(reader);
        return this;
    }

    public IngesterBuilder on(EventPublisher emitter) {
        this.publisher = SafeEventPublisher.of(emitter);
        return this;
    }

    /**
     * How long the ingester remembers the keys it wrote, so it does not write them again (ADR-007).
     * {@link ProcessedKeyWindow#DEFAULT_WINDOW} by default.
     */
    public IngesterBuilder rememberProcessedKeysFor(Duration window) {
        this.processedKeysWindow = Objects.requireNonNull(window);
        return this;
    }

    /** Where the ingester remembers the keys it wrote; in the memory of the process by default. */
    public IngesterBuilder rememberProcessedKeysIn(ProcessedKeyRepository processedKeys) {
        this.processedKeys = Optional.of(processedKeys);
        return this;
    }

    IngesterBuilder clock(Clock clock) {
        this.clock = Objects.requireNonNull(clock);
        return this;
    }

    IngesterBuilder polling(PollingConfiguration pollingConfiguration) {
        this.pollingConfiguration = Objects.requireNonNull(pollingConfiguration);
        return this;
    }

    public Ingester create() {
        MetricsRegistry metrics = new OpenTelemetryMetricsRegistry();
        MetricsRegistry.CounterMetric recordsTotal = metrics.counter(SpoolMetrics.Ingester.RECORDS_TOTAL, SpoolMetrics.Ingester.RECORDS_TOTAL_DESC, "1");
        MetricsRegistry.CounterMetric recordsRejected = metrics.counter(SpoolMetrics.Validation.RECORDS_REJECTED_TOTAL, SpoolMetrics.Validation.RECORDS_REJECTED_TOTAL_DESC, "1");
        MetricsRegistry.CounterMetric validationWarnings = metrics.counter(SpoolMetrics.Validation.WARNINGS_TOTAL, SpoolMetrics.Validation.WARNINGS_TOTAL_DESC, "1");
        MetricsRegistry.TimerMetric processingTimer = metrics.timer(SpoolMetrics.Ingester.PROCESSING_DURATION, SpoolMetrics.Ingester.PROCESSING_DURATION_DESC, "s");
        EnvelopeValidator validator = new EnvelopeValidator(new ValidatorRegistry());
        DataLakeWriter rememberingWriter = new RememberingDataLakeWriter(writer,
                new ProcessedKeyWindow(processedKeys.orElseGet(InMemoryProcessedKeyRepository::new), processedKeysWindow, clock));
        EnvelopeStoredHandler handler = new EnvelopeStoredHandler(rememberingWriter, Objects.requireNonNull(reader, "InboxReader required"), publisher, validator, IngesterErrorRouter.defaults(publisher), recordsTotal, recordsRejected, validationWarnings, processingTimer);
        FlushCoordinator flushCoordinator = new FlushCoordinator(new Buffer(), flushPolicy, handler, clock);
        return new Ingester(listener, pollingConfiguration, flushCoordinator, heartBeat, IngesterErrorRouter.defaults(publisher));
    }
}
