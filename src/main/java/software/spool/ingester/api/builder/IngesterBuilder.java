package software.spool.ingester.api.builder;

import software.spool.core.port.*;
import software.spool.core.port.decorator.SafeEventBusEmitter;
import software.spool.core.port.decorator.SafeEventBusListener;
import software.spool.core.port.decorator.SafeInboxUpdater;
import software.spool.core.utils.PollingConfiguration;
import software.spool.ingester.api.Ingester;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.api.port.QuarantineStore;
import software.spool.ingester.internal.control.ItemPublishedHandler;
import software.spool.ingester.internal.control.ItemValidator;
import software.spool.ingester.internal.decorator.SafeDataLakeWriter;
import software.spool.ingester.internal.utils.Buffer;
import software.spool.ingester.internal.utils.FlushCoordinator;
import software.spool.ingester.internal.utils.FlushPolicy;
import software.spool.validator.engine.ValidatorRegistry;

import java.time.Duration;

/**
 * Fluent builder that configures and assembles an {@link Ingester} instance.
 *
 * <p>
 * Instances are normally obtained via
 * {@link IngesterBuilderFactory#reactive()} or
 * {@link IngesterBuilderFactory#buffered()} and then further customised
 * before calling {@link #create()}.
 * </p>
 *
 * <p>
 * All ports passed to setter methods are automatically wrapped in their
 * corresponding {@code Safe*} decorators to normalise unchecked exceptions
 * into typed {@link software.spool.core.exception.SpoolException} subclasses.
 * </p>
 *
 * <pre>{@code
 * Ingester ingester = IngesterBuilderFactory.buffered()
 *         .from(eventBusListener)
 *         .storesWith(dataLakeWriter)
 *         .updatedWith(inboxUpdater)
 *         .on(eventBusEmitter)
 *         .flushWith(FlushPolicy.every(Duration.ofSeconds(10)))
 *         .create();
 * }</pre>
 */
public class IngesterBuilder {
    private EventBusListener listener;
    private DataLakeWriter writer;
    private InboxUpdater updater;
    private EventBusEmitter emitter;
    private FlushPolicy flushPolicy;
    private QuarantineStore quarantineStore;
    private final PollingConfiguration pollingConfiguration;

    IngesterBuilder() {
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

    /**
     * Sets the {@link EventBusListener} that the ingester subscribes to.
     *
     * @param listener the event bus listener; must not be {@code null}
     * @return this builder for chaining
     */
    public IngesterBuilder from(EventBusListener listener) {
        this.listener = SafeEventBusListener.of(listener);
        return this;
    }

    /**
     * Sets the {@link DataLakeWriter} that persists flushed batches.
     *
     * @param writer the data lake writer; must not be {@code null}
     * @return this builder for chaining
     */
    public IngesterBuilder storesWith(DataLakeWriter writer) {
        this.writer = SafeDataLakeWriter.of(writer);
        return this;
    }

    /**
     * Sets the {@link InboxUpdater} used to mark inbox items as persisted.
     *
     * @param updater the inbox updater; must not be {@code null}
     * @return this builder for chaining
     */
    public IngesterBuilder updatedWith(InboxUpdater updater) {
        this.updater = SafeInboxUpdater.of(updater);
        return this;
    }

    /**
     * Sets the {@link EventBusEmitter} used to emit {@code ItemPersisted} events.
     *
     * @param emitter the event bus emitter; must not be {@code null}
     * @return this builder for chaining
     */
    public IngesterBuilder on(EventBusEmitter emitter) {
        this.emitter = SafeEventBusEmitter.of(emitter);
        return this;
    }

    /**
     * Builds and returns the configured {@link Ingester}.
     *
     * @return a new {@code Ingester} ready to start ingestion
     * @throws NullPointerException if any required port has not been set
     */
    public Ingester create() {
        ItemValidator validator = new ItemValidator(new ValidatorRegistry());
        ItemPublishedHandler handler = new ItemPublishedHandler(writer, updater, emitter, validator, quarantineStore);
        FlushCoordinator flushCoordinator = new FlushCoordinator(new Buffer(), flushPolicy, handler);
        return new Ingester(listener, pollingConfiguration, flushCoordinator);
    }
}
