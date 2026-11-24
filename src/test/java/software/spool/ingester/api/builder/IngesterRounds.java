package software.spool.ingester.api.builder;

import software.spool.core.model.Event;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.schedule.Schedule;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.core.adapter.memory.InMemoryEventBus;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.inbox.InboxEnvelopeResolver;
import software.spool.core.utils.polling.CancellationToken;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.core.utils.polling.PollingScheduler;
import software.spool.ingester.api.Ingester;
import software.spool.ingester.fixture.MovableClock;
import software.spool.ingester.internal.utils.FlushPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

/**
 * An ingester built by its builder whose flush rounds run when a test asks, with a clock the test moves. It listens on
 * a bus of its own unless the test gives it one to share. It is here because the polling and the clock of the builder
 * are only reachable from its package.
 */
public final class IngesterRounds {
    private static final Duration EVERY = Duration.ofMinutes(1);
    private final MovableClock clock;
    private final EventBus bus;
    private final ManualScheduler scheduler = new ManualScheduler();
    private final List<PartitionedEnvelope> written = new ArrayList<>();
    private final List<Event> published = new ArrayList<>();
    private final Ingester ingester;

    public IngesterRounds(InboxEnvelopeResolver inbox, Duration processedKeysWindow, MovableClock clock) {
        this(new InMemoryEventBus(), inbox, processedKeysWindow, clock);
    }

    public IngesterRounds(EventBus bus, InboxEnvelopeResolver inbox, Duration processedKeysWindow,
                          MovableClock clock) {
        this.bus = bus;
        this.clock = clock;
        this.ingester = IngesterBuilderFactory.buffered()
                .flushPolicy(FlushPolicy.whenReaches(Integer.MAX_VALUE).orEvery(EVERY))
                .from(bus)
                .readWith(inbox)
                .storesWith(this::write)
                .on(new EventPublisher() {
                    @Override public <E extends Event> void publish(E event) {
                        published.add(event);
                    }
                })
                .rememberProcessedKeysFor(processedKeysWindow)
                .clock(clock)
                .polling(new PollingConfiguration(scheduler, Schedule.every(EVERY)))
                .create();
        start();
    }

    public void start() {
        ingester.start(mock(SpoolNode.StartPermit.class));
    }

    public void stop() {
        ingester.stop(mock(SpoolNode.StartPermit.class));
    }

    public void announce(EnvelopeStored event) {
        bus.publish(event);
    }

    public void writeBatch() {
        clock.advance(EVERY);
        scheduler.runRound();
    }

    public List<PartitionedEnvelope> written() {
        return List.copyOf(written);
    }

    public List<Event> published() {
        return List.copyOf(published);
    }

    private Stream<IdempotencyKey> write(Collection<PartitionedEnvelope> items) {
        written.addAll(items);
        return items.stream().map(item -> item.envelope().idempotencyKey());
    }

    private static final class ManualScheduler implements PollingScheduler {
        private Runnable task;

        @Override
        public void schedule(Runnable task, Schedule schedule, CancellationToken token) {
            this.task = task;
        }

        void runRound() {
            task.run();
        }
    }
}
