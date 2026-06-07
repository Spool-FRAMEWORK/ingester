package software.spool.ingester.api.builder;

import software.spool.core.adapter.otel.OpenTelemetryMetricsRegistry;
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
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.QuarantineStore;
import software.spool.ingester.api.utils.IngesterErrorRouter;
import software.spool.ingester.internal.control.EnvelopeStoredHandler;
import software.spool.ingester.internal.control.ItemValidator;
import software.spool.ingester.internal.decorator.SafeDataLakeWriter;
import software.spool.ingester.internal.utils.Buffer;
import software.spool.ingester.internal.utils.FlushCoordinator;
import software.spool.ingester.internal.utils.FlushPolicy;
import software.spool.validator.engine.ValidatorRegistry;

import java.time.Duration;
import java.util.Objects;

public class IngesterBuilder {
    private final ModuleHeartBeat heartBeat;
    private EventSubscriber listener;
    private DataLakeWriter writer;
    private InboxEnvelopeResolver reader;
    private EventPublisher publisher;
    private FlushPolicy flushPolicy;
    private QuarantineStore quarantineStore;
    private final PollingConfiguration pollingConfiguration;

    IngesterBuilder(ModuleHeartBeat heartBeat) {
        this.heartBeat = heartBeat;
        this.pollingConfiguration = PollingConfiguration.every(Duration.ofMillis(100));
    }

    public IngesterBuilder flushPolicy(FlushPolicy flushPolicy) {
        this.flushPolicy = flushPolicy;
        return this;
    }

    public IngesterBuilder quarantineStore(QuarantineStore store) {
        this.quarantineStore = store;
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

    public Ingester create() {
        MetricsRegistry metrics = new OpenTelemetryMetricsRegistry();
        MetricsRegistry.CounterMetric recordsTotal = metrics.counter(SpoolMetrics.Ingester.RECORDS_TOTAL, SpoolMetrics.Ingester.RECORDS_TOTAL_DESC, "1");
        MetricsRegistry.CounterMetric recordsRejected = metrics.counter(SpoolMetrics.Ingester.RECORDS_REJECTED_TOTAL, SpoolMetrics.Ingester.RECORDS_REJECTED_TOTAL_DESC, "1");
        MetricsRegistry.TimerMetric processingTimer = metrics.timer(SpoolMetrics.Ingester.PROCESSING_DURATION, SpoolMetrics.Ingester.PROCESSING_DURATION_DESC, "s");
        ItemValidator validator = new ItemValidator(new ValidatorRegistry());
        EnvelopeStoredHandler handler = new EnvelopeStoredHandler(writer, Objects.requireNonNull(reader, "InboxReader required"), publisher, validator, quarantineStore, IngesterErrorRouter.defaults(publisher), recordsTotal, recordsRejected, processingTimer);
        FlushCoordinator flushCoordinator = new FlushCoordinator(new Buffer(), flushPolicy, handler);
        return new Ingester(listener, pollingConfiguration, flushCoordinator, heartBeat, IngesterErrorRouter.defaults(publisher));
    }
}
