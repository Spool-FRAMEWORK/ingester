package software.spool.ingester.api.builder;

import software.spool.ingester.internal.utils.FlushPolicy;

import java.time.Duration;

/**
 * Factory entry point for creating pre-configured {@link IngesterBuilder}
 * instances.
 *
 * <p>
 * Two built-in presets are provided:
 * </p>
 * <ul>
 * <li>{@link #reactive()} — flushes immediately on every event (no
 * buffering).</li>
 * <li>{@link #buffered()} — flushes when the buffer reaches 100 items or every
 * 60 seconds, whichever comes first.</li>
 * </ul>
 *
 * <pre>{@code
 * Ingester ingester = IngesterBuilderFactory.buffered()
 *         .from(eventBus)
 *         .storesWith(myDataLakeWriter)
 *         .updatedWith(inboxUpdater)
 *         .on(eventBusEmitter)
 *         .create();
 * }</pre>
 *
 * @see IngesterBuilder
 */
public class IngesterBuilderFactory {
    /**
     * Creates a builder pre-configured for reactive (immediate) ingestion.
     *
     * <p>
     * Each {@code ItemPublished} event is flushed to the data lake immediately
     * without any buffering.
     * </p>
     *
     * @return a new {@link IngesterBuilder} with an immediate flush policy
     */
    public static IngesterBuilder reactive() {
        return new IngesterBuilder().flushWith(FlushPolicy.immediate());
    }

    /**
     * Creates a builder pre-configured for buffered ingestion.
     *
     * <p>
     * Events are accumulated and flushed when the buffer reaches 100 items
     * or every 60 seconds, whichever comes first. The flush policy can be
     * overridden via {@link IngesterBuilder#flushWith(FlushPolicy)}.
     * </p>
     *
     * @return a new {@link IngesterBuilder} with a buffered flush policy
     */
    public static IngesterBuilder buffered() {
        return new IngesterBuilder().flushWith(FlushPolicy.whenReaches(100).orEvery(Duration.ofSeconds(60)));
    }
}
