package software.spool.ingester.api;

import software.spool.core.model.event.ItemPublished;
import software.spool.core.model.spool.SpoolModule;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.port.bus.EventBusListener;
import software.spool.core.port.health.HealthPayload;
import software.spool.core.port.watchdog.ModuleHeartBeat;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.core.utils.routing.ErrorRouter;
import software.spool.ingester.internal.utils.FlushCoordinator;

import java.util.Objects;

/**
 * Main API entry point for the ingestion lifecycle.
 *
 * <p>
 * An {@code Ingester} subscribes to {@link ItemPublished} events on the event
 * bus,
 * buffers them according to the configured {@link FlushCoordinator}, and
 * periodically
 * flushes the buffer to the data lake.
 * </p>
 *
 * <p>
 * Use the fluent builders in
 * {@link software.spool.ingester.api.builder.IngesterBuilderFactory}
 * to construct instances:
 * </p>
 *
 * <pre>{@code
 * Ingester ingester = IngesterBuilderFactory.buffered()
 *         .from(eventBus)
 *         .storesWith(dataLakeWriter)
 *         .updatedWith(inboxUpdater)
 *         .on(eventBusEmitter)
 *         .create();
 *
 * ingester.startIngestion();
 * }</pre>
 *
 * @see software.spool.ingester.api.builder.IngesterBuilderFactory
 * @see FlushCoordinator
 */
public class Ingester implements SpoolModule {
    private final ModuleHeartBeat heartBeat;
    private final EventBusListener listener;
    private final FlushCoordinator coordinator;
    private final PollingConfiguration pollingConfiguration;
    private final ErrorRouter errorRouter;
    private volatile CancellationToken token;


    /**
     * Creates a new {@code Ingester} with the given event bus listener and flush
     * coordinator.
     *
     * @param listener             the event bus listener to subscribe for
     *                             {@link ItemPublished} events;
     *                             must not be {@code null}
     * @param coordinator          the flush coordinator that manages buffering and flushing;
     *                             must not be {@code null}
     */
    public Ingester(EventBusListener listener, PollingConfiguration pollingConfiguration, FlushCoordinator coordinator, ModuleHeartBeat heartBeat, ErrorRouter errorRouter) {
        this.listener = listener;
        this.coordinator = coordinator;
        this.pollingConfiguration = pollingConfiguration;
        this.heartBeat = heartBeat;
        this.errorRouter = errorRouter;
        this.token = CancellationToken.NOOP;
    }

    /**
     * Starts the ingestion process.
     *
     * <p>
     * Subscribes to {@link ItemPublished} events on the event bus and starts a
     * background scheduler that checks the flush policy every 200 milliseconds.
     * Calling this method when ingestion is already active has no effect.
     * </p>
     */
    public void start(SpoolNode.StartPermit permit) {
        if (token.isActive()) return;
        Objects.requireNonNull(permit);
        token = CancellationToken.create();
        try {
            listener.on(ItemPublished.class, i -> {
                if (token.isCancelled()) return;
                coordinator.submit(i);
            });
            pollingConfiguration.scheduler().schedule(
                    coordinator::flushIfNeeded,
                    pollingConfiguration.policy(),
                    token
            );
            heartBeat.start();
        } catch (Exception e) {
            errorRouter.dispatch(e);
        }
    }

    /**
     * Stops the ingestion process.
     *
     * <p>
     * Cancels the event bus subscription and shuts down the background scheduler.
     * Calling this method when ingestion is already stopped has no effect.
     * </p>
     */
    public void stop(SpoolNode.StartPermit permit) {
        if (token.isCancelled()) return;
        Objects.requireNonNull(permit);
        token.cancel();
        token = CancellationToken.NOOP;
        heartBeat.stop();
    }

    @Override
    public HealthPayload checkHealth() {
        return token.isActive() ? HealthPayload.healthy(heartBeat.identity().moduleId()) : HealthPayload.degraded(heartBeat.identity().moduleId(), null);
    }
}
