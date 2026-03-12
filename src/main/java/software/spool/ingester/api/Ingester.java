package software.spool.ingester.api;

import software.spool.core.model.ItemPublished;
import software.spool.core.port.EventBusListener;
import software.spool.core.port.Subscription;
import software.spool.ingester.internal.utils.FlushCoordinator;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
public class Ingester {
    private final EventBusListener listener;
    private final FlushCoordinator coordinator;
    private Subscription subscription;
    private ScheduledExecutorService scheduler;

    /**
     * Creates a new {@code Ingester} with the given event bus listener and flush
     * coordinator.
     *
     * @param listener    the event bus listener to subscribe for
     *                    {@link ItemPublished} events;
     *                    must not be {@code null}
     * @param coordinator the flush coordinator that manages buffering and flushing;
     *                    must not be {@code null}
     */
    public Ingester(EventBusListener listener, FlushCoordinator coordinator) {
        this.listener = listener;
        this.coordinator = coordinator;
        this.subscription = Subscription.NULL;
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
    public void startIngestion() {
        if (subscription != Subscription.NULL)
            return;
        subscription = listener.on(ItemPublished.class, coordinator::submit);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(coordinator::flushIfNeeded,
                200, 200, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops the ingestion process.
     *
     * <p>
     * Cancels the event bus subscription and shuts down the background scheduler.
     * Calling this method when ingestion is already stopped has no effect.
     * </p>
     */
    public void stopIngestion() {
        if (subscription == Subscription.NULL)
            return;
        subscription.cancel();
        subscription = Subscription.NULL;
        scheduler.shutdown();
    }
}
